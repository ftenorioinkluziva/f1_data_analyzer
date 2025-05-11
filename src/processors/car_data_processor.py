"""
Processor for CarData.z compressed streams from F1 races.
"""
import pandas as pd
from pathlib import Path

from src.processors.base_processor import BaseProcessor
from src.utils.data_decoders import decode_compressed_data


class CarDataProcessor(BaseProcessor):
    """
    Process CarData.z compressed streams to extract telemetry data.
    """
    
    def __init__(self):
        """Initialize the CarData processor."""
        super().__init__()
        self.topic_name = "CarData.z"
    
    def extract_car_telemetry(self, timestamped_data):
        """
        Extract car telemetry data from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, compressed_data)
            
        Returns:
            list: List of dictionaries containing car telemetry data
        """
        car_data = []
        
        for i, (timestamp, encoded_data) in enumerate(timestamped_data):
            # Decode the compressed data
            data = decode_compressed_data(encoded_data)
            
            if data and "Entries" in data:
                for entry in data["Entries"]:
                    if "Utc" in entry and "Cars" in entry:
                        entry_time = entry["Utc"]
                        
                        for driver_number, car_info in entry["Cars"].items():
                            if "Channels" in car_info:
                                channels = car_info["Channels"]
                                
                                car_entry = {
                                    "timestamp": timestamp,
                                    "utc": entry_time,
                                    "driver_number": driver_number,
                                    "rpm": channels.get("0"),
                                    "speed": channels.get("2"),
                                    "gear": channels.get("3"),
                                    "throttle": channels.get("4"),
                                    "brake": channels.get("5"),
                                    "drs": channels.get("45")
                                }
                                
                                car_data.append(car_entry)
            
            # Progress reporting
            if (i + 1) % 500 == 0:
                print(f"Processed {i+1} telemetry records...")
        
        return car_data
    
    def process(self, race_name, session_name):
        """
        Process CarData.z for a specific race and session.
        
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
        
        # Extract car telemetry
        car_data = self.extract_car_telemetry(timestamped_data)
        
        if not car_data:
            print("No car telemetry data found")
            return results
        
        # Create a DataFrame with the car data
        df_car_data = pd.DataFrame(car_data)
        
        # Convert numeric columns
        numeric_columns = ['rpm', 'speed', 'gear', 'throttle', 'brake', 'drs']
        for col in numeric_columns:
            df_car_data[col] = pd.to_numeric(df_car_data[col], errors='coerce')
        
        # Save the processed data
        file_path = self.save_to_csv(
            df_car_data, 
            race_name, 
            session_name, 
            self.topic_name, 
            "car_data.csv"
        )
        results["car_data_file"] = file_path
        
        # Split data by driver for easier analysis
        driver_dir = self.processed_dir / race_name / session_name / self.topic_name / "drivers"
        driver_dir.mkdir(exist_ok=True, parents=True)
        
        drivers = df_car_data['driver_number'].unique()
        
        for driver in drivers:
            driver_data = df_car_data[df_car_data['driver_number'] == driver]
            driver_file = driver_dir / f"telemetry_driver_{driver}.csv"
            driver_data.to_csv(driver_file, index=False)
            
            if "driver_files" not in results:
                results["driver_files"] = {}
            
            results["driver_files"][driver] = driver_file
        
        print(f"Processed car telemetry data for {race_name}/{session_name}")
        return results