"""
Processor for PitLaneTimeCollection streams from F1 races.
"""
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import json
import re
from datetime import datetime

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory


class PitLaneProcessor(BaseProcessor):
    """
    Process PitLaneTimeCollection streams to extract and analyze pit stop data during F1 sessions.
    """
    
    def __init__(self):
        """Initialize the PitLaneTimeCollection processor."""
        super().__init__()
        self.topic_name = "PitLaneTimeCollection"
    
    def extract_pit_data(self, timestamped_data):
        """
        Extract pit stop data from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, json_data)
            
        Returns:
            tuple: (pit_stops, deletions) with lists of pit stop entries and deletion records
        """
        pit_stops = []
        deletions = []
        
        for i, (timestamp, json_str) in enumerate(timestamped_data):
            try:
                # Parse the JSON data
                data = json.loads(json_str)
                
                # Check if the data contains pit time information
                if "PitTimes" in data and isinstance(data["PitTimes"], dict):
                    # Check for deletions
                    if "_deleted" in data["PitTimes"]:
                        deleted_drivers = data["PitTimes"]["_deleted"]
                        for driver in deleted_drivers:
                            deletion = {
                                "timestamp": timestamp,
                                "driver_number": driver
                            }
                            deletions.append(deletion)
                    else:
                        # Extract pit stop data for each driver
                        for driver_number, pit_info in data["PitTimes"].items():
                            if driver_number != "_deleted":  # Skip deletion entries
                                entry = {
                                    "timestamp": timestamp,
                                    "driver_number": driver_number,
                                    "racing_number": pit_info.get("RacingNumber", driver_number),
                                    "duration": pit_info.get("Duration", ""),
                                    "lap": pit_info.get("Lap", "")
                                }
                                
                                # Convert duration to float if possible
                                try:
                                    if entry["duration"]:
                                        entry["duration"] = float(entry["duration"])
                                except ValueError:
                                    pass
                                
                                # Convert lap to integer if possible
                                try:
                                    if entry["lap"]:
                                        entry["lap"] = int(entry["lap"])
                                except ValueError:
                                    pass
                                
                                pit_stops.append(entry)
                
                # Progress reporting
                if (i + 1) % 100 == 0:
                    print(f"Processed {i+1} pit records...")
                    
            except json.JSONDecodeError:
                print(f"Error parsing JSON at timestamp {timestamp}")
                continue
        
        return pit_stops, deletions
    
    def create_pit_stop_summary(self, pit_stops, deletions):
        """
        Create a summary of pit stops by processing the raw data and deletion records.
        
        Args:
            pit_stops: List of pit stop entries
            deletions: List of deletion records
            
        Returns:
            pd.DataFrame: DataFrame with clean pit stop data
        """
        if not pit_stops:
            return pd.DataFrame()
        
        # Convert to DataFrames
        pit_df = pd.DataFrame(pit_stops)
        
        if deletions:
            deletion_df = pd.DataFrame(deletions)
            # Convert timestamps to datetime for better comparison
            pit_df['datetime'] = pd.to_datetime(pit_df['timestamp'], format='%H:%M:%S.%f', errors='coerce')
            deletion_df['datetime'] = pd.to_datetime(deletion_df['timestamp'], format='%H:%M:%S.%f', errors='coerce')
            
            # For each driver, remove entries that were deleted
            # (entries before deletion timestamp that match the driver)
            clean_pit_stops = []
            
            for _, pit_entry in pit_df.iterrows():
                driver = pit_entry['driver_number']
                entry_time = pit_entry['datetime']
                
                # Check if this entry was later deleted
                driver_deletions = deletion_df[deletion_df['driver_number'] == driver]
                
                if driver_deletions.empty:
                    # No deletions for this driver, keep the entry
                    clean_pit_stops.append(pit_entry.drop('datetime').to_dict())
                else:
                    # Check if there's a deletion after this entry
                    later_deletions = driver_deletions[driver_deletions['datetime'] > entry_time]
                    
                    if later_deletions.empty:
                        # No later deletions, this entry is valid
                        clean_pit_stops.append(pit_entry.drop('datetime').to_dict())
            
            # Convert clean pit stops back to DataFrame
            if clean_pit_stops:
                clean_df = pd.DataFrame(clean_pit_stops)
            else:
                clean_df = pd.DataFrame()
        else:
            # No deletions, use the original DataFrame
            clean_df = pit_df
        
        # Sort by timestamp
        if not clean_df.empty and 'timestamp' in clean_df.columns:
            clean_df = clean_df.sort_values('timestamp')
        
        return clean_df
    
    def create_pit_stop_visualizations(self, pit_stops_df, race_name, session_name):
        """
        Create visualizations for pit stop data.
        
        Args:
            pit_stops_df: DataFrame containing pit stop data
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Paths to saved visualizations
        """
        visualizations = {}
        
        if pit_stops_df.empty or 'duration' not in pit_stops_df.columns:
            print("No valid pit stop data for visualization")
            return visualizations
        
        # Create directory for visualizations
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # 1. Pit Stop Duration Comparison
        # Filter out entries without duration
        duration_df = pit_stops_df[pit_stops_df['duration'] != ""].copy()
        
        if not duration_df.empty:
            # Ensure duration is numeric
            duration_df['duration'] = pd.to_numeric(duration_df['duration'], errors='coerce')
            duration_df = duration_df.dropna(subset=['duration'])
            
            if not duration_df.empty:
                plt.figure(figsize=(12, 8))
                
                # Sort by duration to see fastest pit stops
                duration_sorted = duration_df.sort_values('duration')
                
                # Plot horizontal bar chart
                bars = plt.barh(
                    [f"Driver #{d}" for d in duration_sorted['driver_number']],
                    duration_sorted['duration'],
                    color='orange'
                )
                
                # Add duration labels
                for bar in bars:
                    width = bar.get_width()
                    plt.text(
                        width + 0.3,
                        bar.get_y() + bar.get_height()/2,
                        f'{width:.1f}s',
                        va='center'
                    )
                
                plt.xlabel('Duration (seconds)')
                plt.title(f'Pit Stop Durations - {race_name} - {session_name}')
                plt.grid(axis='x')
                plt.tight_layout()
                
                # Save the figure
                duration_path = viz_dir / "pit_stop_durations.png"
                plt.savefig(duration_path)
                plt.close()
                
                visualizations['duration_chart'] = duration_path
        
        # 2. Pit Stop Timing (when in the race)
        if 'lap' in pit_stops_df.columns:
            lap_df = pit_stops_df[pit_stops_df['lap'] != ""].copy()
            
            if not lap_df.empty:
                # Ensure lap is numeric
                lap_df['lap'] = pd.to_numeric(lap_df['lap'], errors='coerce')
                lap_df = lap_df.dropna(subset=['lap'])
                
                if not lap_df.empty:
                    plt.figure(figsize=(12, 8))
                    
                    # Create scatter plot of pit stops by lap
                    plt.scatter(
                        lap_df['lap'],
                        [f"Driver #{d}" for d in lap_df['driver_number']],
                        s=100,
                        color='red',
                        marker='s'
                    )
                    
                    # Add duration labels if available
                    if 'duration' in lap_df.columns:
                        for _, row in lap_df.iterrows():
                            if pd.notna(row['duration']) and row['duration'] != "":
                                plt.text(
                                    row['lap'] + 0.2,
                                    f"Driver #{row['driver_number']}",
                                    f"{float(row['duration']):.1f}s",
                                    va='center'
                                )
                    
                    plt.xlabel('Lap Number')
                    plt.title(f'Pit Stop Timing - {race_name} - {session_name}')
                    plt.grid(True)
                    plt.tight_layout()
                    
                    # Save the figure
                    timing_path = viz_dir / "pit_stop_timing.png"
                    plt.savefig(timing_path)
                    plt.close()
                    
                    visualizations['timing_chart'] = timing_path
        
        # 3. If we have enough pit stops, create a histogram of durations
        if 'duration' in pit_stops_df.columns:
            duration_df = pit_stops_df[pit_stops_df['duration'] != ""].copy()
            
            if len(duration_df) >= 5:  # Only if we have at least 5 pit stops
                # Ensure duration is numeric
                duration_df['duration'] = pd.to_numeric(duration_df['duration'], errors='coerce')
                duration_df = duration_df.dropna(subset=['duration'])
                
                if not duration_df.empty:
                    plt.figure(figsize=(10, 6))
                    
                    # Create histogram of pit stop durations
                    plt.hist(
                        duration_df['duration'],
                        bins=10,
                        color='skyblue',
                        edgecolor='black'
                    )
                    
                    plt.xlabel('Duration (seconds)')
                    plt.ylabel('Frequency')
                    plt.title(f'Distribution of Pit Stop Durations - {race_name} - {session_name}')
                    plt.grid(axis='y')
                    plt.tight_layout()
                    
                    # Save the figure
                    histogram_path = viz_dir / "pit_stop_duration_histogram.png"
                    plt.savefig(histogram_path)
                    plt.close()
                    
                    visualizations['histogram'] = histogram_path
        
        return visualizations
    
    def process(self, race_name, session_name):
        """
        Process PitLaneTimeCollection data for a specific race and session.
        
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
        
        # Extract pit stop data
        pit_stops, deletions = self.extract_pit_data(timestamped_data)
        
        if not pit_stops:
            print("No pit stop data found")
            return results
        
        # Create DataFrames for raw data
        pit_stops_df = pd.DataFrame(pit_stops)
        deletions_df = pd.DataFrame(deletions) if deletions else pd.DataFrame()
        
        # Create directory for processed data
        output_dir = self.processed_dir / race_name / session_name / self.topic_name
        ensure_directory(output_dir)
        
        # Save raw pit stops to CSV
        raw_pit_path = self.save_to_csv(
            pit_stops_df,
            race_name,
            session_name,
            self.topic_name,
            "raw_pit_stops.csv"
        )
        results["raw_pit_stops_file"] = raw_pit_path
        
        # Save deletions to CSV if any
        if not deletions_df.empty:
            deletions_path = self.save_to_csv(
                deletions_df,
                race_name,
                session_name,
                self.topic_name,
                "pit_deletions.csv"
            )
            results["pit_deletions_file"] = deletions_path
        
        # Create and save clean pit stop summary
        pit_summary_df = self.create_pit_stop_summary(pit_stops, deletions)
        
        if not pit_summary_df.empty:
            summary_path = self.save_to_csv(
                pit_summary_df,
                race_name,
                session_name,
                self.topic_name,
                "pit_stops.csv"
            )
            results["pit_stops_file"] = summary_path
            
            # Create visualizations
            viz_paths = self.create_pit_stop_visualizations(
                pit_summary_df,
                race_name,
                session_name
            )
            
            if viz_paths:
                results["visualizations"] = viz_paths
        
        print(f"Processed {len(pit_stops)} pit stop records with {len(deletions)} deletions")
        return results