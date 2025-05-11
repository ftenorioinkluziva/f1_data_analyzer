"""
Processor for CurrentTyres streams from F1 races.
"""
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import json
import re
from datetime import datetime

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory


class CurrentTyresProcessor(BaseProcessor):
    """
    Process CurrentTyres streams to extract and analyze tire compounds used during F1 sessions.
    """
    
    def __init__(self):
        """Initialize the CurrentTyres processor."""
        super().__init__()
        self.topic_name = "CurrentTyres"
        self.compound_colors = {
            "SOFT": "red",
            "MEDIUM": "yellow",
            "HARD": "white",
            "INTERMEDIATE": "green",
            "WET": "blue"
        }
    
    def extract_tyre_data(self, timestamped_data):
        """
        Extract tire data from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, json_data)
            
        Returns:
            list: List of dictionaries containing tire data entries
        """
        tyre_entries = []
        
        for i, (timestamp, json_str) in enumerate(timestamped_data):
            try:
                # Parse the JSON data
                data = json.loads(json_str)
                
                # Check if the data contains tire information
                if "Tyres" in data and isinstance(data["Tyres"], dict):
                    # Extract tire data for each driver
                    for driver_number, tyre_info in data["Tyres"].items():
                        entry = {
                            "timestamp": timestamp,
                            "driver_number": driver_number,
                            "compound": tyre_info.get("Compound"),
                            "new_tire": tyre_info.get("New", False)
                        }
                        tyre_entries.append(entry)
                
                # Progress reporting
                if (i + 1) % 100 == 0:
                    print(f"Processed {i+1} tire records...")
                    
            except json.JSONDecodeError:
                print(f"Error parsing JSON at timestamp {timestamp}")
                continue
        
        return tyre_entries
    
    def create_tyre_history(self, tyre_entries_df):
        """
        Create a history of tire changes for each driver.
        
        Args:
            tyre_entries_df: DataFrame containing individual tire entries
            
        Returns:
            pd.DataFrame: DataFrame with tire history per driver
        """
        # Ensure data is sorted by timestamp
        tyre_entries_df = tyre_entries_df.sort_values("timestamp")
        
        # Convert timestamp to datetime for better chronological sorting
        tyre_entries_df['datetime'] = pd.to_datetime(
            tyre_entries_df['timestamp'], 
            format='%H:%M:%S.%f', 
            errors='coerce'
        )
        
        # Create a history of tire changes
        tyre_history = []
        
        # Group by driver
        for driver, group in tyre_entries_df.groupby('driver_number'):
            prev_compound = None
            prev_new = None
            
            for _, row in group.iterrows():
                # Only record changes
                current_compound = row['compound']
                current_new = row['new_tire']
                
                if current_compound != prev_compound or current_new != prev_new:
                    stint_entry = {
                        "timestamp": row['timestamp'],
                        "datetime": row['datetime'],
                        "driver_number": driver,
                        "compound": current_compound,
                        "new_tire": current_new,
                        "stint_start": True if prev_compound is None or current_compound != prev_compound else False
                    }
                    tyre_history.append(stint_entry)
                    
                    prev_compound = current_compound
                    prev_new = current_new
        
        # Convert to DataFrame
        if tyre_history:
            history_df = pd.DataFrame(tyre_history)
            # Sort by datetime
            history_df = history_df.sort_values('datetime')
            # Drop the datetime column as it was only used for sorting
            history_df = history_df.drop('datetime', axis=1)
            return history_df
        
        return pd.DataFrame()
    
    def create_tyre_strategy_visualization(self, tyre_history_df, race_name, session_name):
        """
        Create a tire strategy visualization.
        
        Args:
            tyre_history_df: DataFrame containing tire history
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the saved visualization
        """
        if tyre_history_df.empty:
            print("No tire history data available for visualization")
            return None
        
        # Create directory for visualization
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # Convert timestamp to session time in minutes
        tyre_history_df['session_time'] = pd.to_datetime(
            tyre_history_df['timestamp'], 
            format='%H:%M:%S.%f', 
            errors='coerce'
        )
        
        # Calculate minutes from session start
        start_time = tyre_history_df['session_time'].min()
        tyre_history_df['minutes'] = tyre_history_df['session_time'].apply(
            lambda x: (x - start_time).total_seconds() / 60 if pd.notnull(x) else 0
        )
        
        # Get top drivers (limited to 15 to avoid overcrowding)
        top_drivers = sorted(tyre_history_df['driver_number'].unique())[:15]
        
        # Create figure
        plt.figure(figsize=(14, 10))
        
        # Setup y-positions and labels
        y_positions = []
        y_labels = []
        
        # Draw tire strategy for each driver
        for i, driver in enumerate(top_drivers):
            driver_stints = tyre_history_df[tyre_history_df['driver_number'] == driver]
            
            y_pos = i
            y_positions.append(y_pos)
            y_labels.append(f"Driver #{driver}")
            
            # Draw tire compounds as colored segments
            prev_minute = 0
            
            for _, stint in driver_stints.iterrows():
                if not pd.isna(stint['compound']):
                    compound = stint['compound']
                    minute = stint['minutes']
                    
                    if minute > prev_minute:
                        color = self.compound_colors.get(compound, "gray")
                        
                        # Draw a colored bar for the stint
                        plt.barh(
                            y_pos,
                            minute - prev_minute,
                            left=prev_minute,
                            height=0.6,
                            color=color,
                            edgecolor='black'
                        )
                        
                        # Add compound text
                        if minute - prev_minute > 5:  # Only add text if segment is wide enough
                            plt.text(
                                prev_minute + (minute - prev_minute) / 2,
                                y_pos,
                                f"{compound[0]}{'-N' if stint['new_tire'] else ''}",
                                ha='center',
                                va='center',
                                color='black' if color in ['yellow', 'white'] else 'white',
                                fontweight='bold'
                            )
                        
                        prev_minute = minute
        
        # Configure chart
        plt.yticks(y_positions, y_labels)
        plt.xlabel('Minutes from Session Start')
        plt.title(f'Tire Strategy - {race_name} - {session_name}')
        plt.grid(axis='x')
        
        # Add legend
        legend_elements = [
            plt.Rectangle((0, 0), 1, 1, color=color, edgecolor='black', label=compound)
            for compound, color in self.compound_colors.items()
        ]
        plt.legend(handles=legend_elements, bbox_to_anchor=(1.05, 1), loc='upper left')
        
        plt.tight_layout()
        
        # Save the figure
        viz_path = viz_dir / "tire_strategy.png"
        plt.savefig(viz_path)
        plt.close()
        
        print(f"Tire strategy visualization saved to {viz_path}")
        return viz_path
    
    def create_compound_distribution_chart(self, tyre_entries_df, race_name, session_name):
        """
        Create a chart showing the distribution of tire compounds used.
        
        Args:
            tyre_entries_df: DataFrame containing tire entries
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the saved visualization
        """
        if tyre_entries_df.empty or 'compound' not in tyre_entries_df.columns:
            print("No compound data available for visualization")
            return None
        
        # Create directory for visualization
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # Count compound usage
        compound_counts = tyre_entries_df['compound'].value_counts()
        
        # Filter out None/NaN values
        compound_counts = compound_counts.dropna()
        
        if compound_counts.empty:
            print("No valid compound data for chart")
            return None
        
        # Create figure
        plt.figure(figsize=(10, 6))
        
        # Create bar chart with compound colors
        bars = plt.bar(
            compound_counts.index,
            compound_counts.values,
            color=[self.compound_colors.get(compound, "gray") for compound in compound_counts.index]
        )
        
        # Add count labels on top of bars
        for bar in bars:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2,
                height,
                f'{height}',
                ha='center',
                va='bottom'
            )
        
        plt.title(f'Tire Compound Distribution - {race_name} - {session_name}')
        plt.xlabel('Compound')
        plt.ylabel('Count')
        plt.grid(axis='y')
        
        # Save the figure
        viz_path = viz_dir / "compound_distribution.png"
        plt.savefig(viz_path)
        plt.close()
        
        print(f"Compound distribution chart saved to {viz_path}")
        return viz_path
    
    def process(self, race_name, session_name):
        """
        Process CurrentTyres data for a specific race and session.
        
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
        
        # Extract tire data
        tyre_entries = self.extract_tyre_data(timestamped_data)
        
        if not tyre_entries:
            print("No tire data found")
            return results
        
        # Create DataFrame for tire entries
        tyre_entries_df = pd.DataFrame(tyre_entries)
        
        # Save the raw tire entries to CSV
        output_dir = self.processed_dir / race_name / session_name / self.topic_name
        ensure_directory(output_dir)
        
        entries_csv_path = self.save_to_csv(
            tyre_entries_df,
            race_name,
            session_name,
            self.topic_name,
            "tyre_entries.csv"
        )
        results["tyre_entries_file"] = entries_csv_path
        
        # Create and save tire history (stints)
        tyre_history_df = self.create_tyre_history(tyre_entries_df)
        
        if not tyre_history_df.empty:
            history_csv_path = self.save_to_csv(
                tyre_history_df,
                race_name,
                session_name,
                self.topic_name,
                "tyre_history.csv"
            )
            results["tyre_history_file"] = history_csv_path
            
            # Create visualizations
            viz_paths = {}
            
            # Tire strategy visualization
            strategy_viz_path = self.create_tyre_strategy_visualization(
                tyre_history_df,
                race_name,
                session_name
            )
            if strategy_viz_path:
                viz_paths["strategy_chart"] = strategy_viz_path
            
            # Compound distribution chart
            dist_viz_path = self.create_compound_distribution_chart(
                tyre_entries_df,
                race_name,
                session_name
            )
            if dist_viz_path:
                viz_paths["distribution_chart"] = dist_viz_path
            
            results["visualizations"] = viz_paths
        
        return results