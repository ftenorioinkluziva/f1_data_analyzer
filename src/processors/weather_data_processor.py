"""
Processor for WeatherData streams from F1 races.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import json
import re
from datetime import datetime, timedelta

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory


class WeatherDataProcessor(BaseProcessor):
    """
    Process WeatherData streams to extract and analyze weather conditions during F1 sessions.
    """
    
    def __init__(self):
        """Initialize the WeatherData processor."""
        super().__init__()
        self.topic_name = "WeatherData"
    
    def extract_weather_data(self, timestamped_data):
        """
        Extract weather data from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, json_data)
            
        Returns:
            list: List of dictionaries containing weather data
        """
        weather_data = []
        
        for i, (timestamp, json_str) in enumerate(timestamped_data):
            try:
                # Parse the JSON data
                data = json.loads(json_str)
                
                # Create a weather entry with the timestamp
                weather_entry = data.copy()
                weather_entry["timestamp"] = timestamp
                
                # Convert numeric values
                for key in ["AirTemp", "Humidity", "Pressure", "Rainfall", "TrackTemp", "WindDirection", "WindSpeed"]:
                    if key in weather_entry:
                        try:
                            weather_entry[key] = float(weather_entry[key])
                        except (ValueError, TypeError):
                            # Keep as string if conversion fails
                            pass
                
                weather_data.append(weather_entry)
                
                # Progress reporting
                if (i + 1) % 100 == 0:
                    print(f"Processed {i+1} weather records...")
                    
            except json.JSONDecodeError:
                print(f"Error parsing JSON at timestamp {timestamp}")
                continue
        
        return weather_data
    
    def analyze_weather_trends(self, weather_data_df):
        """
        Analyze weather trends to detect significant changes or patterns.
        
        Args:
            weather_data_df: DataFrame containing weather data
            
        Returns:
            dict: Analysis results
        """
        analysis = {}
        
        # Calculate basic statistics
        if 'AirTemp' in weather_data_df.columns:
            analysis['air_temp_stats'] = {
                'min': weather_data_df['AirTemp'].min(),
                'max': weather_data_df['AirTemp'].max(),
                'avg': weather_data_df['AirTemp'].mean(),
                'std': weather_data_df['AirTemp'].std()
            }
        
        if 'TrackTemp' in weather_data_df.columns:
            analysis['track_temp_stats'] = {
                'min': weather_data_df['TrackTemp'].min(),
                'max': weather_data_df['TrackTemp'].max(),
                'avg': weather_data_df['TrackTemp'].mean(),
                'std': weather_data_df['TrackTemp'].std()
            }
        
        if 'Humidity' in weather_data_df.columns:
            analysis['humidity_stats'] = {
                'min': weather_data_df['Humidity'].min(),
                'max': weather_data_df['Humidity'].max(),
                'avg': weather_data_df['Humidity'].mean(),
                'std': weather_data_df['Humidity'].std()
            }
        
        # Detect significant changes
        analysis['significant_changes'] = []
        
        # Check for significant temperature changes (more than 3 degrees in short time)
        if 'AirTemp' in weather_data_df.columns:
            temp_changes = weather_data_df['AirTemp'].diff().abs()
            significant_temps = weather_data_df[temp_changes > 3.0]
            
            for _, row in significant_temps.iterrows():
                analysis['significant_changes'].append({
                    'type': 'temperature_change',
                    'timestamp': row['timestamp'],
                    'value': row['AirTemp'],
                    'change': temp_changes.loc[_]
                })
        
        # Check for rain
        if 'Rainfall' in weather_data_df.columns:
            rain_periods = weather_data_df[weather_data_df['Rainfall'] > 0]
            
            if not rain_periods.empty:
                analysis['rain_detected'] = True
                analysis['rain_periods'] = []
                
                for _, row in rain_periods.iterrows():
                    analysis['rain_periods'].append({
                        'timestamp': row['timestamp'],
                        'intensity': row['Rainfall']
                    })
            else:
                analysis['rain_detected'] = False
        
        # Analyze wind patterns
        if 'WindSpeed' in weather_data_df.columns and 'WindDirection' in weather_data_df.columns:
            analysis['wind_stats'] = {
                'max_speed': weather_data_df['WindSpeed'].max(),
                'avg_speed': weather_data_df['WindSpeed'].mean(),
                'dominant_direction': weather_data_df['WindDirection'].mode().iloc[0]
            }
        
        # Check track temperature delta (difference between track and air temp)
        if 'AirTemp' in weather_data_df.columns and 'TrackTemp' in weather_data_df.columns:
            weather_data_df['TempDelta'] = weather_data_df['TrackTemp'] - weather_data_df['AirTemp']
            
            analysis['temp_delta_stats'] = {
                'min': weather_data_df['TempDelta'].min(),
                'max': weather_data_df['TempDelta'].max(),
                'avg': weather_data_df['TempDelta'].mean()
            }
        
        return analysis
    
    def create_weather_visualizations(self, weather_data_df, race_name, session_name):
        """
        Create visualizations of weather data.
        
        Args:
            weather_data_df: DataFrame containing weather data
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Paths to saved visualizations
        """
        visualizations = {}
        
        # Create directory for visualizations
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # Convert timestamp to datetime for better x-axis formatting
        if 'timestamp' in weather_data_df.columns:
            # Extract session time from the timestamp (format: HH:MM:SS.mmm)
            weather_data_df['session_time'] = weather_data_df['timestamp'].apply(
                lambda x: datetime.strptime(x.split('.')[0], '%H:%M:%S')
            )
            
            # Calculate minutes from session start for each record
            start_time = weather_data_df['session_time'].min()
            weather_data_df['minutes_elapsed'] = weather_data_df['session_time'].apply(
                lambda x: (x - start_time).total_seconds() / 60
            )
        
        # 1. Temperature plot (air and track)
        if 'AirTemp' in weather_data_df.columns and 'TrackTemp' in weather_data_df.columns:
            plt.figure(figsize=(12, 6))
            plt.plot(weather_data_df['minutes_elapsed'], weather_data_df['AirTemp'], 'b-', label='Air Temperature')
            plt.plot(weather_data_df['minutes_elapsed'], weather_data_df['TrackTemp'], 'r-', label='Track Temperature')
            plt.xlabel('Minutes from Session Start')
            plt.ylabel('Temperature (°C)')
            plt.title(f'Temperature Evolution - {race_name} - {session_name}')
            plt.grid(True)
            plt.legend()
            
            temp_plot_path = viz_dir / "temperature_evolution.png"
            plt.savefig(temp_plot_path)
            plt.close()
            
            visualizations['temperature_plot'] = temp_plot_path
        
        # 2. Humidity plot
        if 'Humidity' in weather_data_df.columns:
            plt.figure(figsize=(12, 6))
            plt.plot(weather_data_df['minutes_elapsed'], weather_data_df['Humidity'], 'g-')
            plt.xlabel('Minutes from Session Start')
            plt.ylabel('Humidity (%)')
            plt.title(f'Humidity Evolution - {race_name} - {session_name}')
            plt.grid(True)
            
            humidity_plot_path = viz_dir / "humidity_evolution.png"
            plt.savefig(humidity_plot_path)
            plt.close()
            
            visualizations['humidity_plot'] = humidity_plot_path
        
        # 3. Wind speed and direction
        if 'WindSpeed' in weather_data_df.columns and 'WindDirection' in weather_data_df.columns:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
            
            # Wind speed
            ax1.plot(weather_data_df['minutes_elapsed'], weather_data_df['WindSpeed'], 'm-')
            ax1.set_ylabel('Wind Speed (km/h)')
            ax1.set_title(f'Wind Conditions - {race_name} - {session_name}')
            ax1.grid(True)
            
            # Wind direction
            ax2.scatter(weather_data_df['minutes_elapsed'], weather_data_df['WindDirection'], c='darkorange', alpha=0.7)
            ax2.set_xlabel('Minutes from Session Start')
            ax2.set_ylabel('Wind Direction (degrees)')
            ax2.set_yticks([0, 90, 180, 270, 360])
            ax2.set_yticklabels(['N', 'E', 'S', 'W', 'N'])
            ax2.grid(True)
            
            plt.tight_layout()
            
            wind_plot_path = viz_dir / "wind_conditions.png"
            plt.savefig(wind_plot_path)
            plt.close()
            
            visualizations['wind_plot'] = wind_plot_path
        
        # 4. Combined weather dashboard
        plt.figure(figsize=(16, 12))
        
        # Temperature subplot
        if 'AirTemp' in weather_data_df.columns and 'TrackTemp' in weather_data_df.columns:
            plt.subplot(3, 1, 1)
            plt.plot(weather_data_df['minutes_elapsed'], weather_data_df['AirTemp'], 'b-', label='Air Temperature')
            plt.plot(weather_data_df['minutes_elapsed'], weather_data_df['TrackTemp'], 'r-', label='Track Temperature')
            plt.ylabel('Temperature (°C)')
            plt.title(f'Weather Conditions - {race_name} - {session_name}')
            plt.grid(True)
            plt.legend()
        
        # Humidity subplot
        if 'Humidity' in weather_data_df.columns:
            plt.subplot(3, 1, 2)
            plt.plot(weather_data_df['minutes_elapsed'], weather_data_df['Humidity'], 'g-')
            plt.ylabel('Humidity (%)')
            plt.grid(True)
        
        # Wind speed subplot
        if 'WindSpeed' in weather_data_df.columns:
            plt.subplot(3, 1, 3)
            plt.plot(weather_data_df['minutes_elapsed'], weather_data_df['WindSpeed'], 'm-')
            plt.xlabel('Minutes from Session Start')
            plt.ylabel('Wind Speed (km/h)')
            plt.grid(True)
        
        plt.tight_layout()
        
        dashboard_path = viz_dir / "weather_dashboard.png"
        plt.savefig(dashboard_path)
        plt.close()
        
        visualizations['weather_dashboard'] = dashboard_path
        
        return visualizations
    
    def process(self, race_name, session_name):
        """
        Process WeatherData for a specific race and session.
        
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
        
        # Extract weather data
        weather_data = self.extract_weather_data(timestamped_data)
        
        if not weather_data:
            print("No weather data found")
            return results
        
        # Create a DataFrame with the weather data
        df_weather = pd.DataFrame(weather_data)
        
        # Reorder columns to make timestamp the first column
        if 'timestamp' in df_weather.columns:
            columns = ['timestamp'] + [col for col in df_weather.columns if col != 'timestamp']
            df_weather = df_weather[columns]
        
        # Save the processed data to CSV
        output_dir = self.processed_dir / race_name / session_name / self.topic_name
        ensure_directory(output_dir)
        
        csv_path = output_dir / "weather_data.csv"
        df_weather.to_csv(csv_path, index=False)
        results["weather_data_file"] = csv_path
        print(f"Weather data saved to {csv_path}")
        
        # Analyze weather trends
        weather_analysis = self.analyze_weather_trends(df_weather)
        
        # Save the analysis
        analysis_path = output_dir / "weather_analysis.json"
        with open(analysis_path, 'w') as f:
            json.dump(weather_analysis, f, indent=2, default=str)
        results["weather_analysis_file"] = analysis_path
        print(f"Weather analysis saved to {analysis_path}")
        
        # Create visualizations
        visualization_paths = self.create_weather_visualizations(df_weather, race_name, session_name)
        results["visualizations"] = visualization_paths
        print(f"Weather visualizations created: {len(visualization_paths)} charts")
        
        return results