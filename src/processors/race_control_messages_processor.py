"""
Processor for RaceControlMessages streams from F1 races.
"""
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import json
import re
from datetime import datetime

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory


class RaceControlMessagesProcessor(BaseProcessor):
    """
    Process RaceControlMessages streams to extract and analyze official race control communications.
    """
    
    def __init__(self):
        """Initialize the RaceControlMessages processor."""
        super().__init__()
        self.topic_name = "RaceControlMessages"
        self.flag_colors = {
            "GREEN": "green",
            "YELLOW": "yellow",
            "DOUBLE YELLOW": "gold",
            "RED": "red",
            "BLUE": "blue",
            "WHITE": "white",
            "BLACK": "black",
            "CHEQUERED": "black",
            "CLEAR": "lightgray"
        }
    
    def extract_messages(self, timestamped_data):
        """
        Extract race control messages from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, json_data)
            
        Returns:
            list: List of dictionaries containing race control messages
        """
        messages = []
        
        for i, (timestamp, json_str) in enumerate(timestamped_data):
            try:
                # Parse the JSON data
                data = json.loads(json_str)
                session_timestamp = timestamp
                
                # Check if "Messages" is an array
                if "Messages" in data and isinstance(data["Messages"], list):
                    for msg in data["Messages"]:
                        message_entry = {
                            "timestamp": session_timestamp,
                            "utc_time": msg.get("Utc"),
                            "category": msg.get("Category"),
                            "message": msg.get("Message"),
                            "flag": msg.get("Flag"),
                            "scope": msg.get("Scope"),
                            "sector": msg.get("Sector")
                        }
                        messages.append(message_entry)
                
                # Check if "Messages" is a dictionary (with numbered keys)
                elif "Messages" in data and isinstance(data["Messages"], dict):
                    for msg_id, msg in data["Messages"].items():
                        message_entry = {
                            "timestamp": session_timestamp,
                            "message_id": msg_id,
                            "utc_time": msg.get("Utc"),
                            "category": msg.get("Category"),
                            "message": msg.get("Message"),
                            "flag": msg.get("Flag"),
                            "scope": msg.get("Scope"),
                            "sector": msg.get("Sector")
                        }
                        messages.append(message_entry)
                
                # Progress reporting
                if (i + 1) % 50 == 0:
                    print(f"Processed {i+1} message records...")
                    
            except json.JSONDecodeError:
                print(f"Error parsing JSON at timestamp {timestamp}")
                continue
        
        return messages
    
    def analyze_messages(self, messages_df):
        """
        Analyze race control messages to extract insights.
        
        Args:
            messages_df: DataFrame containing race control messages
            
        Returns:
            dict: Analysis results
        """
        analysis = {}
        
        # Count messages by category
        if 'category' in messages_df.columns:
            category_counts = messages_df['category'].value_counts().to_dict()
            analysis['category_counts'] = category_counts
        
        # Count flags
        if 'flag' in messages_df.columns:
            flag_counts = messages_df['flag'].dropna().value_counts().to_dict()
            analysis['flag_counts'] = flag_counts
        
        # Identify incidents and investigations
        investigation_messages = messages_df[messages_df['message'].str.contains('INVEST', case=False, na=False)]
        incident_messages = messages_df[messages_df['message'].str.contains('INCIDENT', case=False, na=False)]
        
        analysis['investigation_count'] = len(investigation_messages)
        analysis['incident_count'] = len(incident_messages)
        
        # Extract cars involved in incidents
        car_pattern = r'CAR\s+(\d+)'
        cars_involved = []
        
        for msg in incident_messages['message']:
            if isinstance(msg, str):
                car_matches = re.findall(car_pattern, msg)
                cars_involved.extend(car_matches)
        
        analysis['cars_involved'] = {}
        for car in set(cars_involved):
            car_count = cars_involved.count(car)
            analysis['cars_involved'][car] = car_count
        
        # Identify the most incident-prone sectors
        if 'sector' in messages_df.columns:
            sector_incidents = messages_df.dropna(subset=['sector'])
            if not sector_incidents.empty:
                sector_counts = sector_incidents['sector'].value_counts().to_dict()
                analysis['sector_counts'] = sector_counts
        
        return analysis
    
    def create_message_timeline(self, messages_df, race_name, session_name):
        """
        Create a timeline visualization of race control messages.
        
        Args:
            messages_df: DataFrame containing race control messages
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the saved visualization
        """
        if messages_df.empty:
            print("No messages available for visualization")
            return None
        
        # Create directory for visualization
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # Convert timestamp to session time in minutes
        if 'timestamp' in messages_df.columns:
            # Extract session time from the timestamp (format: HH:MM:SS.mmm)
            messages_df['session_time'] = messages_df['timestamp'].apply(
                lambda x: datetime.strptime(x.split('.')[0], '%H:%M:%S')
                if isinstance(x, str) and ':' in x else None
            )
            
            # Calculate minutes from session start
            messages_df = messages_df.dropna(subset=['session_time'])
            if messages_df.empty:
                print("No valid timestamp data for visualization")
                return None
                
            start_time = messages_df['session_time'].min()
            messages_df['minutes'] = messages_df['session_time'].apply(
                lambda x: (x - start_time).total_seconds() / 60
            )
        
        # Create figure
        plt.figure(figsize=(14, 8))
        
        # Plot messages as a timeline
        y_positions = []
        y_labels = []
        colors = []
        
        # Group messages by category
        for i, (category, group) in enumerate(messages_df.groupby('category')):
            if pd.isna(category):
                category = "Other"
                
            y_pos = i
            y_positions.append(y_pos)
            y_labels.append(category)
            
            for _, msg in group.iterrows():
                # Choose color based on flag or category
                if pd.notna(msg.get('flag')):
                    color = self.flag_colors.get(msg['flag'], 'gray')
                else:
                    # Use different colors for different categories
                    category_colors = {
                        'Flag': 'green',
                        'Other': 'blue',
                        'Drs': 'purple',
                        'SafetyCar': 'orange',
                        'VirtualSafetyCar': 'orange'
                    }
                    color = category_colors.get(category, 'gray')
                
                # Plot a marker for each message
                plt.scatter(
                    msg['minutes'],
                    y_pos,
                    marker='o',
                    s=100,
                    color=color,
                    alpha=0.7
                )
                
                # Add a short label for important messages
                if (pd.notna(msg.get('flag')) or 
                    (pd.notna(msg.get('message')) and
                     ('INCIDENT' in str(msg['message']) or 'INVEST' in str(msg['message'])))):
                    
                    short_msg = msg.get('flag', '')
                    if not short_msg and pd.notna(msg.get('message')):
                        # Truncate long messages
                        short_msg = str(msg['message'])[:30]
                        if len(str(msg['message'])) > 30:
                            short_msg += "..."
                    
                    plt.annotate(
                        short_msg,
                        (msg['minutes'], y_pos),
                        xytext=(5, 0),
                        textcoords='offset points',
                        fontsize=8,
                        rotation=45,
                        ha='left',
                        va='bottom'
                    )
        
        # Configure chart
        plt.yticks(y_positions, y_labels)
        plt.xlabel('Minutes from Session Start')
        plt.title(f'Race Control Messages - {race_name} - {session_name}')
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        
        # Add vertical lines for easier time reference
        max_minutes = messages_df['minutes'].max()
        for minute in range(0, int(max_minutes) + 1, 5):  # Every 5 minutes
            plt.axvline(x=minute, color='gray', linestyle=':', alpha=0.3)
        
        plt.tight_layout()
        
        # Save the figure
        timeline_path = viz_dir / "message_timeline.png"
        plt.savefig(timeline_path)
        plt.close()
        
        print(f"Message timeline visualization saved to {timeline_path}")
        return timeline_path
    
    def create_category_distribution_chart(self, messages_df, race_name, session_name):
        """
        Create a chart showing the distribution of message categories.
        
        Args:
            messages_df: DataFrame containing race control messages
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the saved visualization
        """
        if messages_df.empty or 'category' not in messages_df.columns:
            print("No category data available for visualization")
            return None
        
        # Create directory for visualization
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # Count messages by category
        category_counts = messages_df['category'].fillna('Unknown').value_counts()
        
        if category_counts.empty:
            print("No valid category data for chart")
            return None
        
        # Create figure
        plt.figure(figsize=(10, 6))
        
        # Create pie chart
        plt.pie(
            category_counts.values,
            labels=category_counts.index,
            autopct='%1.1f%%',
            startangle=90,
            shadow=False
        )
        
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
        plt.title(f'Message Categories - {race_name} - {session_name}')
        
        # Save the figure
        chart_path = viz_dir / "category_distribution.png"
        plt.savefig(chart_path)
        plt.close()
        
        print(f"Category distribution chart saved to {chart_path}")
        return chart_path
    
    def create_incident_summary(self, messages_df, race_name, session_name):
        """
        Create a visualization of incidents and investigations.
        
        Args:
            messages_df: DataFrame containing race control messages
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the saved text summary
        """
        if messages_df.empty:
            print("No messages available for summary")
            return None
        
        # Create directory for visualization
        summary_dir = self.processed_dir / race_name / session_name / self.topic_name / "summary"
        ensure_directory(summary_dir)
        
        # Filter for incident and investigation messages
        incident_msgs = messages_df[
            messages_df['message'].str.contains('INCIDENT|INVEST', case=False, na=False)
        ].sort_values('timestamp')
        
        if incident_msgs.empty:
            print("No incident messages found")
            return None
        
        # Create a text summary
        summary_path = summary_dir / "incident_summary.txt"
        
        with open(summary_path, 'w') as f:
            f.write(f"=== Incident Summary: {race_name} - {session_name} ===\n\n")
            
            for i, msg in incident_msgs.iterrows():
                timestamp = msg.get('timestamp', 'Unknown time')
                message = msg.get('message', 'No details')
                
                f.write(f"[{timestamp}] {message}\n\n")
            
            # Add some summary statistics
            f.write(f"\n=== Summary Statistics ===\n")
            f.write(f"Total incidents/investigations: {len(incident_msgs)}\n")
            
            # Count incidents by car number
            car_pattern = r'CAR\s+(\d+)'
            cars_involved = {}
            
            for msg in incident_msgs['message']:
                if isinstance(msg, str):
                    car_matches = re.findall(car_pattern, msg)
                    for car in car_matches:
                        cars_involved[car] = cars_involved.get(car, 0) + 1
            
            if cars_involved:
                f.write("\nCars involved in incidents:\n")
                for car, count in sorted(cars_involved.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"Car #{car}: {count} incidents\n")
        
        print(f"Incident summary saved to {summary_path}")
        return summary_path
    
    def process(self, race_name, session_name):
        """
        Process RaceControlMessages for a specific race and session.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Processing results with file paths
        """
        results = {}
        
        # Get the path to the raw data file
        raw_file_path = self.get_raw_file_path(race_name, session_name, self.topic_name)
        
        if not raw_file_path.exists():
            print(f"Raw data file not found: {raw_file_path}")
            return results
        
        # Extract timestamped data
        timestamped_data = self.extract_timestamped_data(raw_file_path)
        
        if not timestamped_data:
            print("No data found in the raw file")
            return results
        
        # Extract race control messages
        messages = self.extract_messages(timestamped_data)
        
        if not messages:
            print("No race control messages found")
            return results
        
        # Create DataFrame for messages
        messages_df = pd.DataFrame(messages)
        
        # Save the messages to CSV
        output_dir = self.processed_dir / race_name / session_name / self.topic_name
        ensure_directory(output_dir)
        
        csv_path = self.save_to_csv(
            messages_df,
            race_name,
            session_name,
            self.topic_name,
            "race_control_messages.csv"
        )
        results["messages_file"] = csv_path
        print(f"Race control messages saved to {csv_path}")
        
        # Analyze messages
        message_analysis = self.analyze_messages(messages_df)
        
        # Save the analysis
        analysis_path = output_dir / "message_analysis.json"
        with open(analysis_path, 'w') as f:
            json.dump(message_analysis, f, indent=2, default=str)
        results["analysis_file"] = analysis_path
        print(f"Message analysis saved to {analysis_path}")
        
        # Create visualizations
        viz_paths = {}
        
        # Message timeline
        timeline_path = self.create_message_timeline(
            messages_df,
            race_name,
            session_name
        )
        if timeline_path:
            viz_paths["timeline"] = timeline_path
        
        # Category distribution
        chart_path = self.create_category_distribution_chart(
            messages_df,
            race_name,
            session_name
        )
        if chart_path:
            viz_paths["category_chart"] = chart_path
        
        # Incident summary
        summary_path = self.create_incident_summary(
            messages_df,
            race_name,
            session_name
        )
        if summary_path:
            viz_paths["incident_summary"] = summary_path
        
        results["visualizations"] = viz_paths
        
        return results