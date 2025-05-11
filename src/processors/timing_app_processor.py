"""
Processor for TimingAppData streams from F1 races.
"""
import pandas as pd
from pathlib import Path

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory


class TimingAppProcessor(BaseProcessor):
    """
    Process TimingAppData streams to extract tire strategy and stint information.
    """
    
    def __init__(self):
        """Initialize the TimingAppData processor."""
        super().__init__()
        self.topic_name = "TimingAppData"
    
    def extract_tire_data(self, parsed_data):
        """
        Extract tire strategy data from parsed TimingAppData entries.
        
        Args:
            parsed_data: List of dictionaries containing parsed data with timestamps
            
        Returns:
            tuple: (driver_positions, tire_stints)
        """
        driver_positions = []
        tire_stints = []
        
        # Process each parsed entry
        for entry in parsed_data:
            timestamp = entry["timestamp"]
            data = entry["data"]
            
            # Check if we have driver information
            if "Lines" in data and isinstance(data["Lines"], dict):
                for driver_number, driver_info in data["Lines"].items():
                    if not isinstance(driver_info, dict):
                        continue
                    
                    # Extract position
                    if "Line" in driver_info:
                        position = driver_info["Line"]
                        driver_positions.append({
                            "timestamp": timestamp,
                            "driver_number": driver_number,
                            "grid_position": position
                        })
                    
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
                                        "total_laps": stint_data.get("TotalLaps", 0),
                                        "start_laps": stint_data.get("StartLaps", 0)
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
                                        "total_laps": stint_data.get("TotalLaps", 0),
                                        "start_laps": stint_data.get("StartLaps", 0)
                                    }
                                    tire_stints.append(stint_info)
        
        return driver_positions, tire_stints
    
    def get_latest_tire_stints(self, tire_stints):
        """
        Get the latest tire stint information for each driver and stint number.
        
        Args:
            tire_stints: List of tire stint dictionaries
            
        Returns:
            pd.DataFrame: DataFrame with the latest tire stint information
        """
        if not tire_stints:
            return pd.DataFrame()
        
        # Create a DataFrame
        stints_df = pd.DataFrame(tire_stints)
        
        # Convert timestamp to datetime
        stints_df['timestamp'] = pd.to_datetime(stints_df['timestamp'], format='%H:%M:%S.%f')
        
        # Group by driver and stint number, taking the latest entry
        latest_stints = stints_df.sort_values('timestamp').groupby(
            ['driver_number', 'stint_number']
        ).last().reset_index()
        
        return latest_stints
    
    def process(self, race_name, session_name):
        """
        Process TimingAppData for a specific race and session.
        
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
        
        # Parse JSON data
        parsed_data = self.parse_json_data(timestamped_data)
        
        if not parsed_data:
            print("No valid JSON data found")
            return results
        
        # Extract tire data
        driver_positions, tire_stints = self.extract_tire_data(parsed_data)
        
        # Save the processed data
        if driver_positions:
            df_positions = pd.DataFrame(driver_positions)
            file_path = self.save_to_csv(
                df_positions, 
                race_name, 
                session_name, 
                self.topic_name, 
                "grid_positions.csv"
            )
            results["positions_file"] = file_path
        
        if tire_stints:
            # Get the latest tire stint information
            latest_stints = self.get_latest_tire_stints(tire_stints)
            
            file_path = self.save_to_csv(
                latest_stints, 
                race_name, 
                session_name, 
                self.topic_name, 
                "tire_stints.csv"
            )
            results["tire_stints_file"] = file_path
            
            # Also save the raw tire stint data
            raw_stints_df = pd.DataFrame(tire_stints)
            file_path = self.save_to_csv(
                raw_stints_df, 
                race_name, 
                session_name, 
                self.topic_name, 
                "tire_stints_raw.csv"
            )
            results["tire_stints_raw_file"] = file_path
        
        print(f"Processed TimingAppData for {race_name}/{session_name}")
        return results