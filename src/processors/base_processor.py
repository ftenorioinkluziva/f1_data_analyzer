"""
Base Processor class with common functionality for all data processors.
"""
import re
import json
import pandas as pd
from pathlib import Path

import config
from src.utils.file_utils import ensure_directory
from src.utils.data_decoders import decode_compressed_data


class BaseProcessor:
    """
    Base class for all data processors with shared functionality.
    """
    
    def __init__(self):
        """Initialize the base processor."""
        self.raw_dir = config.RAW_DATA_DIR
        self.processed_dir = config.PROCESSED_DATA_DIR
    
    def extract_timestamped_data(self, file_path):
        """
        Extract timestamped data from a JSON stream file.
        
        Args:
            file_path: Path to the raw data file
            
        Returns:
            list: List of tuples containing (timestamp, data)
        """
        print(f"Processing: {file_path}")
        
        with open(file_path, 'rb') as f:
            content = f.read()
        
        # Decode the content as UTF-8
        text = content.decode('utf-8', errors='replace')
        
        # Extract timestamp and data using regex
        pattern = r'(\d{2}:\d{2}:\d{2}\.\d{3})(.*?)(?=\d{2}:\d{2}:\d{2}\.\d{3}|$)'
        matches = re.findall(pattern, text, re.DOTALL)
        
        print(f"Found {len(matches)} records")
        
        return matches
    
    def ensure_timestamp_first(self, df):
        """
        Ensure that the timestamp column is the first column in the DataFrame.
        
        Args:
            df: DataFrame to reorganize
            
        Returns:
            pd.DataFrame: DataFrame with timestamp as the first column
        """
        # Check if timestamp column exists
        timestamp_cols = [col for col in df.columns if 'timestamp' in col.lower()]
        
        if timestamp_cols:
            # Use the first timestamp column found
            timestamp_col = timestamp_cols[0]
            # Reorder columns
            columns = [timestamp_col] + [col for col in df.columns if col != timestamp_col]
            return df[columns]
        
        return df


    def parse_json_data(self, timestamped_data):
        """
        Parse JSON data from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, raw_json_string)
            
        Returns:
            list: List of dictionaries containing parsed data with timestamps
        """
        parsed_data = []
        
        for i, (timestamp, json_str) in enumerate(timestamped_data):
            try:
                data = json.loads(json_str)
                parsed_data.append({
                    "timestamp": timestamp,
                    "data": data
                })
                
                # Progress reporting
                if (i + 1) % 1000 == 0:
                    print(f"Parsed {i+1} records...")
                    
            except json.JSONDecodeError:
                # Skip invalid JSON
                continue
        
        return parsed_data
    
    def save_processed_data(self, data, race_name, session_name, topic_name, file_name):
        """
        Save processed data to a file.
        
        Args:
            data: The data to save
            race_name: Name of the race
            session_name: Name of the session
            topic_name: Name of the data topic
            file_name: Name of the output file
            
        Returns:
            Path: Path to the saved file
        """
        output_dir = self.processed_dir / race_name / session_name / topic_name
        ensure_directory(output_dir)
        
        file_path = output_dir / file_name
        
        if isinstance(data, pd.DataFrame):
            # Save DataFrame to CSV
            data.to_csv(file_path, index=False)
        else:
            # Save other data (like lists, dicts) to JSON
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        
        print(f"Processed data saved to {file_path}")
        
        return file_path
    
    def save_to_csv(self, df, race_name, session_name, topic_name, file_name):
        """
        Save a DataFrame to a CSV file.
        
        Args:
            df: The DataFrame to save
            race_name: Name of the race
            session_name: Name of the session
            topic_name: Name of the data topic
            file_name: Name of the output file
            
        Returns:
            Path: Path to the saved CSV file
        """
        output_dir = self.processed_dir / race_name / session_name / topic_name
        ensure_directory(output_dir)
        
        # Ensure timestamp is the first column
        df = self.ensure_timestamp_first(df)
        
        file_path = output_dir / file_name
        df.to_csv(file_path, index=False)
        
        print(f"CSV data saved to {file_path}")
        
        return file_path
    
    def get_raw_file_path(self, race_name, session_name, topic_name):
        """
        Get the path to a raw data file.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            topic_name: Name of the data topic
            
        Returns:
            Path: Path to the raw data file
        """
        return self.raw_dir / race_name / session_name / f"{topic_name}.jsonStream"
    
    def process(self, race_name, session_name):
        """
        Process raw data for a specific race and session.
        This method should be implemented by subclasses.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Processing results
        """
        raise NotImplementedError("Subclasses must implement this method")