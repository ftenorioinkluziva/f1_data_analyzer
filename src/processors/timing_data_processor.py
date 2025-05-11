"""
Processor for TimingData streams from F1 races.
"""
import pandas as pd
from pathlib import Path

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory


class TimingDataProcessor(BaseProcessor):
    """
    Process TimingData streams to extract lap times, sector times, speeds, and positions.
    """
    
    def __init__(self):
        """Initialize the TimingData processor."""
        super().__init__()
        self.topic_name = "TimingData"
    
    def extract_driver_data(self, parsed_data):
        """
        Extract detailed driver data from parsed TimingData entries.
        
        Args:
            parsed_data: List of dictionaries containing parsed data with timestamps
            
        Returns:
            tuple: (drivers_data, lap_times, sector_times, positions, speeds)
        """
        # Data structures to store the extracted information
        drivers_data = {}
        lap_times = []
        sector_times = []
        positions = []
        speeds = []
        
        # Process each parsed entry
        for entry in parsed_data:
            timestamp = entry["timestamp"]
            data = entry["data"]
            
            # Check if we have driver information
            if "Lines" in data and isinstance(data["Lines"], dict):
                for driver_number, driver_info in data["Lines"].items():
                    if not isinstance(driver_info, dict):
                        continue
                    
                    # Initialize driver data if it doesn't exist
                    if driver_number not in drivers_data:
                        drivers_data[driver_number] = {
                            "timestamps": [],
                            "positions": [],
                            "lap_times": [],
                            "sector1": [],
                            "sector2": [],
                            "sector3": [],
                            "speeds_i1": [],
                            "speeds_i2": [],
                            "speeds_st": [],
                            "in_pit": [],
                            "pit_out": [],
                            "laps": []
                        }
                    
                    # Add timestamp
                    drivers_data[driver_number]["timestamps"].append(timestamp)
                    
                    # Extract position
                    position = driver_info.get("Position")
                    if position:
                        drivers_data[driver_number]["positions"].append(position)
                        positions.append({
                            "timestamp": timestamp,
                            "driver_number": driver_number,
                            "position": position
                        })
                    else:
                        drivers_data[driver_number]["positions"].append(None)
                    
                    # Extract lap times
                    if "LastLapTime" in driver_info and "Value" in driver_info["LastLapTime"]:
                        lap_time = driver_info["LastLapTime"]["Value"]
                        if lap_time:
                            drivers_data[driver_number]["lap_times"].append(lap_time)
                            lap_times.append({
                                "timestamp": timestamp,
                                "driver_number": driver_number,
                                "lap_time": lap_time,
                                "fastest": driver_info["LastLapTime"].get("OverallFastest", False),
                                "personal_fastest": driver_info["LastLapTime"].get("PersonalFastest", False)
                            })
                    
                    # Extract sector times
                    if "Sectors" in driver_info and isinstance(driver_info["Sectors"], list):
                        for sector_idx, sector in enumerate(driver_info["Sectors"]):
                            if isinstance(sector, dict) and "Value" in sector and sector["Value"]:
                                sector_key = f"sector{sector_idx+1}"
                                sector_times.append({
                                    "timestamp": timestamp,
                                    "driver_number": driver_number,
                                    "sector": sector_idx+1,
                                    "time": sector["Value"],
                                    "fastest": sector.get("OverallFastest", False),
                                    "personal_fastest": sector.get("PersonalFastest", False)
                                })
                                drivers_data[driver_number][sector_key].append(sector["Value"])
                    
                    # Extract speeds
                    if "Speeds" in driver_info and isinstance(driver_info["Speeds"], dict):
                        speeds_info = driver_info["Speeds"]
                        
                        # Intermediate 1
                        if "I1" in speeds_info and "Value" in speeds_info["I1"] and speeds_info["I1"]["Value"]:
                            speed = speeds_info["I1"]["Value"]
                            drivers_data[driver_number]["speeds_i1"].append(speed)
                            speeds.append({
                                "timestamp": timestamp,
                                "driver_number": driver_number,
                                "type": "I1",
                                "speed": speed,
                                "fastest": speeds_info["I1"].get("OverallFastest", False),
                                "personal_fastest": speeds_info["I1"].get("PersonalFastest", False)
                            })
                        
                        # Intermediate 2
                        if "I2" in speeds_info and "Value" in speeds_info["I2"] and speeds_info["I2"]["Value"]:
                            speed = speeds_info["I2"]["Value"]
                            drivers_data[driver_number]["speeds_i2"].append(speed)
                            speeds.append({
                                "timestamp": timestamp,
                                "driver_number": driver_number,
                                "type": "I2",
                                "speed": speed,
                                "fastest": speeds_info["I2"].get("OverallFastest", False),
                                "personal_fastest": speeds_info["I2"].get("PersonalFastest", False)
                            })
                        
                        # Speed Trap
                        if "ST" in speeds_info and "Value" in speeds_info["ST"] and speeds_info["ST"]["Value"]:
                            speed = speeds_info["ST"]["Value"]
                            drivers_data[driver_number]["speeds_st"].append(speed)
                            speeds.append({
                                "timestamp": timestamp,
                                "driver_number": driver_number,
                                "type": "ST",
                                "speed": speed,
                                "fastest": speeds_info["ST"].get("OverallFastest", False),
                                "personal_fastest": speeds_info["ST"].get("PersonalFastest", False)
                            })
                    
                    # Pit status
                    if "InPit" in driver_info:
                        drivers_data[driver_number]["in_pit"].append(driver_info["InPit"])
                    else:
                        drivers_data[driver_number]["in_pit"].append(None)
                    
                    if "PitOut" in driver_info:
                        drivers_data[driver_number]["pit_out"].append(driver_info["PitOut"])
                    else:
                        drivers_data[driver_number]["pit_out"].append(None)
                    
                    # Lap number
                    if "NumberOfLaps" in driver_info:
                        drivers_data[driver_number]["laps"].append(driver_info["NumberOfLaps"])
                    else:
                        drivers_data[driver_number]["laps"].append(None)
        
        return drivers_data, lap_times, sector_times, positions, speeds
    
    def process(self, race_name, session_name):
        """
        Process TimingData for a specific race and session.
        
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
        
        # Extract driver data
        drivers_data, lap_times, sector_times, positions, speeds = self.extract_driver_data(parsed_data)
        
        # Save the processed data to CSV files
        if positions:
            df_positions = pd.DataFrame(positions)
            file_path = self.save_to_csv(
                df_positions, 
                race_name, 
                session_name, 
                self.topic_name, 
                "positions.csv"
            )
            results["positions_file"] = file_path
        
        if lap_times:
            df_lap_times = pd.DataFrame(lap_times)
            file_path = self.save_to_csv(
                df_lap_times, 
                race_name, 
                session_name, 
                self.topic_name, 
                "lap_times.csv"
            )
            results["lap_times_file"] = file_path
        
        if sector_times:
            df_sector_times = pd.DataFrame(sector_times)
            file_path = self.save_to_csv(
                df_sector_times, 
                race_name, 
                session_name, 
                self.topic_name, 
                "sector_times.csv"
            )
            results["sector_times_file"] = file_path
        
        if speeds:
            df_speeds = pd.DataFrame(speeds)
            file_path = self.save_to_csv(
                df_speeds, 
                race_name, 
                session_name, 
                self.topic_name, 
                "speeds.csv"
            )
            results["speeds_file"] = file_path
        
        # Save individual driver data
        driver_dir = self.processed_dir / race_name / session_name / self.topic_name / "drivers"
        ensure_directory(driver_dir)
        
        for driver_number, data in drivers_data.items():
            # Create a DataFrame for each driver
            driver_df = pd.DataFrame({
                "timestamp": data["timestamps"],
                "position": data["positions"],
                "in_pit": data["in_pit"],
                "pit_out": data["pit_out"],
                "lap_number": data["laps"]
            })
            
            file_path = driver_dir / f"driver_{driver_number}.csv"
            driver_df.to_csv(file_path, index=False)
            
            if "driver_files" not in results:
                results["driver_files"] = {}
            
            results["driver_files"][driver_number] = file_path
        
        print(f"Processed TimingData for {race_name}/{session_name}")
        return results