"""
Analyzer for correlating tire stints with laps and performance data.
"""
import pandas as pd
from pathlib import Path
import datetime

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory


class StintAnalyzer(BaseProcessor):
    """
    Analyze tire stints and correlate with lap times and performance.
    """
    
    def __init__(self):
        """Initialize the stint analyzer."""
        super().__init__()
    
    def analyze(self, race_name, session_name):
        """
        Analyze tire stints for a race session and correlate with lap data.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Analysis results with file paths
        """
        results = {}
        
        # Get paths to the required files
        timing_app_path = self.get_raw_file_path(race_name, session_name, "TimingAppData")
        timing_data_path = self.get_raw_file_path(race_name, session_name, "TimingData")
        
        if not timing_app_path.exists():
            print(f"TimingAppData file not found: {timing_app_path}")
            return results
        
        if not timing_data_path.exists():
            print(f"TimingData file not found: {timing_data_path}")
            return results
        
        # Process TimingAppData to get tire stint information
        print("Processing tire stint data...")
        app_timestamped_data = self.extract_timestamped_data(timing_app_path)
        app_parsed_data = self.parse_json_data(app_timestamped_data)
        
        tire_stints = []
        
        for entry in app_parsed_data:
            timestamp = entry["timestamp"]
            data = entry["data"]
            
            if "Lines" in data and isinstance(data["Lines"], dict):
                for driver_number, driver_info in data["Lines"].items():
                    if not isinstance(driver_info, dict):
                        continue
                    
                    # Extract stint/tire information
                    if "Stints" in driver_info:
                        stints = driver_info["Stints"]
                        
                        # Process stints (both as dictionary and as list)
                        if isinstance(stints, dict):
                            for stint_idx, stint_data in stints.items():
                                if isinstance(stint_data, dict) and "Compound" in stint_data:
                                    stint_info = {
                                        "timestamp": timestamp,
                                        "driver_number": driver_number,
                                        "stint_number": int(stint_idx) + 1,
                                        "compound": stint_data["Compound"],
                                        "new_tire": stint_data.get("New") == "true",
                                        "total_laps": int(stint_data.get("TotalLaps", 0)),
                                        "start_laps": int(stint_data.get("StartLaps", 0))
                                    }
                                    tire_stints.append(stint_info)
                        elif isinstance(stints, list):
                            for stint_idx, stint_data in enumerate(stints):
                                if isinstance(stint_data, dict) and "Compound" in stint_data:
                                    stint_info = {
                                        "timestamp": timestamp,
                                        "driver_number": driver_number,
                                        "stint_number": stint_idx + 1,
                                        "compound": stint_data["Compound"],
                                        "new_tire": stint_data.get("New") == "true",
                                        "total_laps": int(stint_data.get("TotalLaps", 0)),
                                        "start_laps": int(stint_data.get("StartLaps", 0))
                                    }
                                    tire_stints.append(stint_info)
        
        # Process TimingData to get lap information
        print("Processing lap data...")
        timing_timestamped_data = self.extract_timestamped_data(timing_data_path)
        timing_parsed_data = self.parse_json_data(timing_timestamped_data)
        
        driver_laps = {}
        pit_stops = []
        
        for entry in timing_parsed_data:
            timestamp = entry["timestamp"]
            data = entry["data"]
            
            if "Lines" in data and isinstance(data["Lines"], dict):
                for driver_number, driver_info in data["Lines"].items():
                    if not isinstance(driver_info, dict):
                        continue
                    
                    # Initialize driver laps if not already present
                    if driver_number not in driver_laps:
                        driver_laps[driver_number] = []
                    
                    # Extract lap number
                    if "NumberOfLaps" in driver_info:
                        lap_number = driver_info["NumberOfLaps"]
                        driver_laps[driver_number].append({
                            "timestamp": timestamp,
                            "lap_number": lap_number
                        })
                    
                    # Detect pit stops
                    if "PitOut" in driver_info and driver_info["PitOut"] == True:
                        pit_stops.append({
                            "timestamp": timestamp,
                            "driver_number": driver_number,
                            "action": "pit_out"
                        })
                    
                    if "InPit" in driver_info and driver_info["InPit"] == True:
                        # New pit entry
                        if not pit_stops or pit_stops[-1]["driver_number"] != driver_number or pit_stops[-1]["action"] != "in_pit":
                            pit_stops.append({
                                "timestamp": timestamp,
                                "driver_number": driver_number,
                                "action": "in_pit"
                            })
        
        # Create DataFrames
        stints_df = pd.DataFrame(tire_stints)
        stints_df['timestamp'] = pd.to_datetime(stints_df['timestamp'], format='%H:%M:%S.%f')
        
        # Get the latest stint information for each driver/stint
        latest_stints = stints_df.sort_values('timestamp').groupby(['driver_number', 'stint_number']).last().reset_index()
        
        # Create laps DataFrame
        laps_data = []
        for driver, laps in driver_laps.items():
            for lap in laps:
                laps_data.append({
                    "driver_number": driver,
                    "timestamp": lap["timestamp"],
                    "lap_number": lap["lap_number"]
                })
        
        laps_df = pd.DataFrame(laps_data)
        laps_df['timestamp'] = pd.to_datetime(laps_df['timestamp'], format='%H:%M:%S.%f')
        
        # Get the latest lap information for each driver/lap
        latest_laps = laps_df.sort_values('timestamp').groupby(['driver_number', 'lap_number']).last().reset_index()
        
        # Create pit stops DataFrame
        pit_df = pd.DataFrame(pit_stops)
        pit_df['timestamp'] = pd.to_datetime(pit_df['timestamp'], format='%H:%M:%S.%f')
        
        # Create stint-lap correlation
        stint_laps = []
        
        for _, stint in latest_stints.iterrows():
            driver = stint['driver_number']
            stint_num = stint['stint_number']
            
            # Filter laps for this driver
            driver_lap_data = latest_laps[latest_laps['driver_number'] == driver].sort_values('lap_number')
            
            if driver_lap_data.empty:
                continue
            
            # Determine start lap
            # For first stint, start at lap 1
            if stint_num == 1:
                lap_start = 1
            else:
                # For other stints, find the previous pit stop
                driver_pits = pit_df[(pit_df['driver_number'] == driver) & (pit_df['action'] == 'pit_out')].sort_values('timestamp')
                
                if not driver_pits.empty:
                    # Find the pit stop corresponding to this stint
                    pit_idx = stint_num - 2  # -1 for zero-based indexing, -1 for previous stint
                    if pit_idx >= 0 and pit_idx < len(driver_pits):
                        pit_out_time = driver_pits.iloc[pit_idx]['timestamp']
                        
                        # Find first lap after pit out
                        next_laps = driver_lap_data[driver_lap_data['timestamp'] >= pit_out_time]
                        if not next_laps.empty:
                            lap_start = next_laps.iloc[0]['lap_number']
                        else:
                            # If not found, use the prior lap + 1
                            lap_start = driver_lap_data['lap_number'].max() + 1
                    else:
                        # Estimate if not enough pit stops
                        lap_start = (stint_num - 1) * 20 + 1  # Estimate: 20 laps per stint
                else:
                    # Estimate if no pit stops
                    lap_start = (stint_num - 1) * 20 + 1  # Estimate: 20 laps per stint
            
            # Determine end lap
            # For the last known stint, end at the last lap
            is_last_stint = stint_num == latest_stints[latest_stints['driver_number'] == driver]['stint_number'].max()
            
            if is_last_stint:
                lap_end = driver_lap_data['lap_number'].max()
            else:
                # For other stints, end before the next stint starts
                next_stint_start = None
                for _, other_stint in latest_stints.iterrows():
                    if other_stint['driver_number'] == driver and other_stint['stint_number'] == stint_num + 1:
                        if 'lap_start' in locals():
                            next_stint_start = lap_start
                        break
                
                if next_stint_start:
                    lap_end = next_stint_start - 1
                else:
                    # Estimate if next stint not found
                    lap_end = lap_start + 19  # Estimate: 20 laps per stint
            
            stint_laps.append({
                "driver_number": driver,
                "stint_number": stint_num,
                "compound": stint['compound'],
                "new_tire": stint['new_tire'],
                "lap_start": lap_start,
                "lap_end": lap_end,
                "stint_length": lap_end - lap_start + 1
            })
        
        # Create stint-lap DataFrame
        stint_laps_df = pd.DataFrame(stint_laps)
        
        # Save the analysis results
        file_path = self.save_to_csv(
            stint_laps_df,
            race_name,
            session_name,
            "StintAnalysis",
            "stint_laps.csv"
        )
        results["stint_laps_file"] = file_path
        
        # Save the pit stop data
        file_path = self.save_to_csv(
            pit_df,
            race_name,
            session_name,
            "StintAnalysis",
            "pit_stops.csv"
        )
        results["pit_stops_file"] = file_path
        
        print(f"Completed stint analysis for {race_name}/{session_name}")
        return results