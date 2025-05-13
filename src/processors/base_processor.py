"""
Base Processor class with common functionality for all data processors.
Updated to work with key-based folder structure.
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
    Updated to work with meeting_key/session_key folder structure.
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
    
    def save_processed_data(self, data, meeting_key, session_key, topic_name, file_name, race_name=None, session_name=None):
        """
        Save processed data to a file using key-based folder structure.
        
        Args:
            data: The data to save
            meeting_key: Meeting key (race key)
            session_key: Session key
            topic_name: Name of the data topic
            file_name: Name of the output file
            race_name: Optional race name for logging (default: None)
            session_name: Optional session name for logging (default: None)
            
        Returns:
            Path: Path to the saved file
        """
        # Convert keys to strings for path construction
        meeting_key_str = str(meeting_key)
        session_key_str = str(session_key)
        
        # Create output directory path with key-based structure
        output_dir = self.processed_dir / meeting_key_str / session_key_str / topic_name
        ensure_directory(output_dir)
        
        file_path = output_dir / file_name
        
        if isinstance(data, pd.DataFrame):
            # Save DataFrame to CSV
            data.to_csv(file_path, index=False)
        else:
            # Save other data (like lists, dicts) to JSON
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        
        # Display info with race/session names if provided, otherwise use keys
        display_info = f"{race_name}/{session_name}" if race_name and session_name else f"Meeting {meeting_key}/Session {session_key}"
        print(f"Processed data saved to {file_path} ({display_info})")
        
        return file_path
    
    def save_to_csv(self, df, meeting_key, session_key, topic_name, file_name, race_name=None, session_name=None):
        """
        Save a DataFrame to a CSV file using key-based folder structure.
        
        Args:
            df: The DataFrame to save
            meeting_key: Meeting key (race key)
            session_key: Session key
            topic_name: Name of the data topic
            file_name: Name of the output file
            race_name: Optional race name for logging (default: None)
            session_name: Optional session name for logging (default: None)
            
        Returns:
            Path: Path to the saved CSV file
        """
        # Convert keys to strings for path construction
        meeting_key_str = str(meeting_key)
        session_key_str = str(session_key)
        
        # Create output directory path with key-based structure
        output_dir = self.processed_dir / meeting_key_str / session_key_str / topic_name
        ensure_directory(output_dir)
        
        # Ensure timestamp is the first column
        df = self.ensure_timestamp_first(df)
        
        file_path = output_dir / file_name
        df.to_csv(file_path, index=False)
        
        # Display info with race/session names if provided, otherwise use keys
        display_info = f"{race_name}/{session_name}" if race_name and session_name else f"Meeting {meeting_key}/Session {session_key}"
        print(f"CSV data saved to {file_path} ({display_info})")
        
        return file_path
    
    def get_raw_file_path(self, meeting_key, session_key, topic_name):
        """
        Get the path to a raw data file using key-based folder structure.
        
        Args:
            meeting_key: Meeting key (race key)
            session_key: Session key
            topic_name: Name of the data topic
            
        Returns:
            Path: Path to the raw data file
        """
        # Convert keys to strings for path construction
        meeting_key_str = str(meeting_key)
        session_key_str = str(session_key)
        
        return self.raw_dir / meeting_key_str / session_key_str / f"{topic_name}.jsonStream"
    
    def get_processed_dir(self, meeting_key, session_key, topic_name=None):
        """
        Get the processed data directory path using key-based folder structure.
        
        Args:
            meeting_key: Meeting key (race key)
            session_key: Session key
            topic_name: Optional name of the data topic (default: None)
            
        Returns:
            Path: Path to the processed data directory
        """
        # Convert keys to strings for path construction
        meeting_key_str = str(meeting_key)
        session_key_str = str(session_key)
        
        if topic_name:
            return self.processed_dir / meeting_key_str / session_key_str / topic_name
        else:
            return self.processed_dir / meeting_key_str / session_key_str
    
    def load_metadata(self, meeting_key, session_key, topic_name):
        """
        Load metadata for a specific topic from the key-based folder structure.
        
        Args:
            meeting_key: Meeting key (race key)
            session_key: Session key
            topic_name: Name of the data topic
            
        Returns:
            dict: Metadata dictionary or None if not found
        """
        # Convert keys to strings for path construction
        meeting_key_str = str(meeting_key)
        session_key_str = str(session_key)
        
        metadata_path = self.raw_dir / meeting_key_str / session_key_str / f"{topic_name}_metadata.json"
        
        if metadata_path.exists():
            try:
                with open(metadata_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading metadata: {str(e)}")
                return None
        else:
            return None
    
    # Legacy method for backward compatibility
    def get_raw_file_path_by_name(self, race_name, session_name, topic_name):
        """
        Legacy method to get raw file path using race and session names.
        For backward compatibility.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            topic_name: Name of the data topic
            
        Returns:
            Path: Path to the raw data file
        """
        return self.raw_dir / race_name / session_name / f"{topic_name}.jsonStream"
    
    # Legacy method for backward compatibility
    def save_to_csv_by_name(self, df, race_name, session_name, topic_name, file_name):
        """
        Legacy method to save CSV using race and session names.
        For backward compatibility.
        
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
        
        print(f"CSV data saved to {file_path} (using legacy path structure)")
        
        return file_path
    
    def process(self, meeting_key, session_key, race_name=None, session_name=None):
        """
        Process raw data for a specific race and session using key-based structure.
        This method should be implemented by subclasses.
        
        Args:
            meeting_key: Meeting key (race key)
            session_key: Session key
            race_name: Optional race name for logging (default: None)
            session_name: Optional session name for logging (default: None)
            
        Returns:
            dict: Processing results
        """
        raise NotImplementedError("Subclasses must implement this method")
    
    # Legacy process method for backward compatibility
    def process_by_name(self, race_name, session_name):
        """
        Legacy method to process data using race and session names.
        Subclasses should override this if needed for backward compatibility.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Processing results
        """
        # This method should be implemented by subclasses that need backward compatibility
        raise NotImplementedError("Legacy processing not implemented")