"""
Processor for TeamRadio streams from F1 races with correct handling of different JSON formats.
"""
import pandas as pd
import json
import re
from pathlib import Path
import os

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory


class TeamRadioProcessor(BaseProcessor):
    """
    Process TeamRadio streams to extract and analyze team radio communications during F1 sessions.
    """
    
    def __init__(self):
        """Initialize the TeamRadio processor."""
        super().__init__()
        self.topic_name = "TeamRadio"
    
    def custom_extract_timestamped_data(self, file_path):
        """
        Custom extraction method that reads the file line by line.
        
        Args:
            file_path: Path to the raw data file
            
        Returns:
            list: List of tuples containing (timestamp, json_data)
        """
        print(f"Using custom extraction for: {file_path}")
        
        try:
            # Ler o arquivo linha a linha
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                lines = [line.strip() for line in f if line.strip()]
            
            print(f"Found {len(lines)} lines in file")
            
            # Extrair timestamp e JSON de cada linha
            results = []
            for i, line in enumerate(lines):
                # Extrair o timestamp (padrão: 00:00:00.000) e o resto da linha como JSON
                match = re.match(r'^(\d{2}:\d{2}:\d{2}\.\d{3})(.*)$', line)
                if match:
                    timestamp = match.group(1)
                    json_data = match.group(2)
                    results.append((timestamp, json_data))
                else:
                    print(f"Line {i+1} does not match expected pattern: {line[:50]}...")
            
            print(f"Successfully extracted {len(results)} records")
            return results
            
        except Exception as e:
            print(f"Error during custom extraction: {str(e)}")
            return []
    
    def extract_team_radio_data(self, timestamped_data):
        """
        Extract team radio data from timestamped entries, handling different JSON formats.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, json_data)
            
        Returns:
            list: List of dictionaries containing team radio data
        """
        radio_messages = []
        
        for i, (timestamp, json_str) in enumerate(timestamped_data):
            try:
                # Parse the JSON data
                data = json.loads(json_str)
                
                # Check if "Captures" exists in the data
                if "Captures" in data:
                    captures = data["Captures"]
                    
                    # Format 1: Captures is a list/array
                    if isinstance(captures, list):
                        print(f"Processing array format for timestamp {timestamp}")
                        for capture in captures:
                            if "RacingNumber" in capture and "Path" in capture:
                                radio_message = {
                                    "timestamp": timestamp,
                                    "utc_time": capture.get("Utc", ""),
                                    "driver_number": capture["RacingNumber"],
                                    "audio_path": capture["Path"]
                                }
                                radio_messages.append(radio_message)
                                print(f"Added message from array format: Driver {capture['RacingNumber']}")
                    
                    # Format 2: Captures is a dictionary/object
                    elif isinstance(captures, dict):
                        for capture_key, capture_value in captures.items():
                            if isinstance(capture_value, dict) and "RacingNumber" in capture_value and "Path" in capture_value:
                                radio_message = {
                                    "timestamp": timestamp,
                                    "utc_time": capture_value.get("Utc", ""),
                                    "driver_number": capture_value["RacingNumber"],
                                    "audio_path": capture_value["Path"],
                                    "message_id": capture_key
                                }
                                radio_messages.append(radio_message)
                else:
                    print(f"No 'Captures' found in data for timestamp {timestamp}")
            
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON at timestamp {timestamp}: {str(e)}")
                print(f"Raw JSON: {json_str[:100]}...")
                continue
            except Exception as e:
                print(f"Unexpected error processing team radio at timestamp {timestamp}: {str(e)}")
                continue
        
        return radio_messages
    
    def process(self, race_name, session_name):
        """
        Process TeamRadio data for a specific race and session.
        
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
        
        # Mostrar algumas informações sobre o arquivo
        file_size = os.path.getsize(raw_file_path)
        print(f"Processing TeamRadio file: {raw_file_path} (Size: {file_size} bytes)")
        
        # Mostrar primeiras linhas do arquivo para depuração
        try:
            with open(raw_file_path, 'r', encoding='utf-8-sig') as f:
                first_line = f.readline().strip()
                print(f"First line: {first_line}")
        except Exception as e:
            print(f"Error reading first line: {str(e)}")
        
        # Use custom extraction method instead of base class method
        timestamped_data = self.custom_extract_timestamped_data(raw_file_path)
        
        if not timestamped_data:
            print("No data found in the raw file")
            return results
        
        print(f"Found {len(timestamped_data)} raw records in {raw_file_path}")
        
        # Mostrar informações detalhadas sobre o primeiro registro
        if timestamped_data:
            first_timestamp, first_json = timestamped_data[0]
            print(f"First timestamp: {first_timestamp}")
            print(f"First JSON data: {first_json}")
        
        # Extract team radio data
        radio_messages = self.extract_team_radio_data(timestamped_data)
        
        if not radio_messages:
            print("No team radio messages found")
            return results
        
        print(f"Extracted {len(radio_messages)} team radio messages")
        
        # Count unique driver numbers
        driver_numbers = set(msg.get('driver_number') for msg in radio_messages)
        print(f"Found messages from {len(driver_numbers)} unique drivers: {', '.join(sorted(driver_numbers))}")
        
        # Mostrar as mensagens extraídas para debug
        for i, msg in enumerate(radio_messages[:5]):
            print(f"Mensagem {i+1}: Driver #{msg.get('driver_number')}, Path: {msg.get('audio_path')}")
        
        # Create DataFrame for radio messages
        radio_messages_df = pd.DataFrame(radio_messages)
        
        # Save the radio messages to CSV
        output_dir = self.processed_dir / race_name / session_name / self.topic_name
        ensure_directory(output_dir)
        
        csv_path = self.save_to_csv(
            radio_messages_df,
            race_name,
            session_name,
            self.topic_name,
            "team_radio_messages.csv"
        )
        results["team_radio_file"] = csv_path
        print(f"Team radio data saved to {csv_path}")
        
        # Organize messages by driver
        driver_files = {}
        for driver, group in radio_messages_df.groupby('driver_number'):
            driver_csv_path = self.save_to_csv(
                group,
                race_name,
                session_name,
                self.topic_name,
                f"driver_{driver}_radio.csv"
            )
            driver_files[driver] = driver_csv_path
        
        if driver_files:
            results["driver_files"] = driver_files
        
        return results