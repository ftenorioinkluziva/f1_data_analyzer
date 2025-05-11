"""
Processor for TeamRadio streams from F1 races.
"""
import pandas as pd
import json
import re
from pathlib import Path
from datetime import datetime

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
    
    def extract_team_radio_data(self, timestamped_data):
        """
        Extract team radio data from timestamped entries.
        
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
                
                # There are two formats in the data, handle both
                if "Captures" in data:
                    captures = data["Captures"]
                    
                    # Format 1: Captures is a list
                    if isinstance(captures, list):
                        for capture in captures:
                            if "RacingNumber" in capture and "Path" in capture:
                                radio_message = {
                                    "timestamp": timestamp,
                                    "utc_time": capture.get("Utc", ""),
                                    "driver_number": capture["RacingNumber"],
                                    "audio_path": capture["Path"]
                                }
                                radio_messages.append(radio_message)
                    
                    # Format 2: Captures is a dictionary with numbered keys
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
                
                # Progress reporting
                if (i + 1) % 50 == 0:
                    print(f"Processed {i+1} team radio records...")
                    
            except json.JSONDecodeError:
                print(f"Error parsing JSON at timestamp {timestamp}")
                continue
        
        return radio_messages
    
    def organize_by_driver(self, radio_messages_df):
        """
        Organize radio messages by driver for easier analysis.
        
        Args:
            radio_messages_df: DataFrame containing all radio messages
            
        Returns:
            dict: Dictionary mapping driver numbers to their messages
        """
        if radio_messages_df.empty:
            return {}
        
        driver_messages = {}
        
        # Convert to datetime for sorting
        radio_messages_df['datetime'] = pd.to_datetime(radio_messages_df['timestamp'], format='%H:%M:%S.%f', errors='coerce')
        radio_messages_df = radio_messages_df.sort_values('datetime')
        
        # Group by driver
        for driver, group in radio_messages_df.groupby('driver_number'):
            driver_messages[driver] = group.drop('datetime', axis=1).to_dict('records')
        
        return driver_messages
    
    def generate_html_report(self, radio_messages_df, race_name, session_name):
        """
        Generate an HTML report with embedded audio players for the team radio messages.
        
        Args:
            radio_messages_df: DataFrame containing all radio messages
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the generated HTML report
        """
        if radio_messages_df.empty:
            print("No radio messages to include in the report")
            return None
        
        # Create directory for the report
        report_dir = self.processed_dir / race_name / session_name / self.topic_name
        ensure_directory(report_dir)
        
        # Convert timestamp to session time
        radio_messages_df['session_time'] = pd.to_datetime(radio_messages_df['timestamp'], format='%H:%M:%S.%f', errors='coerce')
        
        # Sort by session time
        radio_messages_df = radio_messages_df.sort_values('session_time')
        
        # Format session time for display
        radio_messages_df['formatted_time'] = radio_messages_df['session_time'].dt.strftime('%H:%M:%S')
        
        # Create HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Team Radio Report - {race_name} - {session_name}</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 20px;
                    background-color: #f8f8f8;
                }}
                h1, h2, h3 {{
                    color: #e10600;
                }}
                .header {{
                    background-color: #15151e;
                    color: white;
                    padding: 20px;
                    text-align: center;
                    margin-bottom: 30px;
                }}
                .radio-message {{
                    background-color: white;
                    border-radius: 8px;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                    margin-bottom: 20px;
                    padding: 15px;
                    display: flex;
                    align-items: center;
                }}
                .message-time {{
                    flex: 0 0 120px;
                    font-weight: bold;
                }}
                .driver-info {{
                    flex: 0 0 100px;
                    text-align: center;
                }}
                .driver-number {{
                    display: inline-block;
                    width: 40px;
                    height: 40px;
                    line-height: 40px;
                    text-align: center;
                    background-color: #e10600;
                    color: white;
                    border-radius: 50%;
                    font-weight: bold;
                }}
                .audio-player {{
                    flex: 1;
                    margin-left: 20px;
                }}
                audio {{
                    width: 100%;
                }}
                .driver-filter {{
                    background-color: white;
                    padding: 15px;
                    margin-bottom: 20px;
                    border-radius: 8px;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                }}
                .driver-filter button {{
                    margin: 5px;
                    padding: 8px 15px;
                    border: none;
                    border-radius: 4px;
                    background-color: #15151e;
                    color: white;
                    cursor: pointer;
                }}
                .driver-filter button:hover {{
                    background-color: #e10600;
                }}
                .driver-filter button.active {{
                    background-color: #e10600;
                }}
                .footer {{
                    text-align: center;
                    margin-top: 40px;
                    color: #777;
                    font-size: 0.9em;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>F1 Team Radio Report</h1>
                <h2>{race_name.replace('_', ' ')} - {session_name.replace('_', ' ')}</h2>
                <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <div class="driver-filter">
                <h3>Filter by Driver</h3>
                <button onclick="filterMessages('all')" class="active">All Drivers</button>
        """
        
        # Add filter buttons for each driver
        unique_drivers = radio_messages_df['driver_number'].unique()
        for driver in sorted(unique_drivers):
            html_content += f'<button onclick="filterMessages(\'{driver}\')">{driver}</button>\n'
        
        html_content += """
            </div>
            
            <h2>Radio Messages</h2>
            <div class="radio-messages-container">
        """
        
        # Add each radio message
        for _, message in radio_messages_df.iterrows():
            html_content += f"""
                <div class="radio-message" data-driver="{message['driver_number']}">
                    <div class="message-time">{message['formatted_time']}</div>
                    <div class="driver-info">
                        <div class="driver-number">{message['driver_number']}</div>
                    </div>
                    <div class="audio-player">
                        <audio controls>
                            <source src="https://livetiming.formula1.com/static/{message['audio_path']}" type="audio/mp3">
                            Your browser does not support the audio element.
                        </audio>
                    </div>
                </div>
            """
        
        # Complete the HTML
        html_content += """
            </div>
            
            <div class="footer">
                <p>Generated by F1 Data Analyzer</p>
                <p>Audio files are property of Formula 1</p>
            </div>
            
            <script>
                function filterMessages(driverFilter) {
                    // Update active button
                    const buttons = document.querySelectorAll('.driver-filter button');
                    buttons.forEach(button => {
                        button.classList.remove('active');
                        if ((driverFilter === 'all' && button.innerText === 'All Drivers') || 
                            button.innerText === driverFilter) {
                            button.classList.add('active');
                        }
                    });
                    
                    // Filter messages
                    const messages = document.querySelectorAll('.radio-message');
                    messages.forEach(message => {
                        if (driverFilter === 'all' || message.getAttribute('data-driver') === driverFilter) {
                            message.style.display = 'flex';
                        } else {
                            message.style.display = 'none';
                        }
                    });
                }
            </script>
        </body>
        </html>
        """
        
        # Save the HTML report
        report_path = report_dir / "team_radio_report.html"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML team radio report generated at {report_path}")
        return report_path
    
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
        
        # Extract timestamped data
        timestamped_data = self.extract_timestamped_data(raw_file_path)
        
        if not timestamped_data:
            print("No data found in the raw file")
            return results
        
        # Extract team radio data
        radio_messages = self.extract_team_radio_data(timestamped_data)
        
        if not radio_messages:
            print("No team radio messages found")
            return results
        
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
        driver_messages = self.organize_by_driver(radio_messages_df)
        
        # Save driver-specific messages
        for driver, messages in driver_messages.items():
            driver_df = pd.DataFrame(messages)
            driver_csv_path = self.save_to_csv(
                driver_df,
                race_name,
                session_name,
                self.topic_name,
                f"driver_{driver}_radio.csv"
            )
            if "driver_files" not in results:
                results["driver_files"] = {}
            results["driver_files"][driver] = driver_csv_path
        
        # Generate HTML report with embedded audio players
        html_report_path = self.generate_html_report(radio_messages_df, race_name, session_name)
        if html_report_path:
            results["html_report"] = html_report_path
        
        return results