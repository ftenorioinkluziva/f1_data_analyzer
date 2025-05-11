"""
Processor for Position.z compressed streams from F1 races.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import json
import re
import zlib
import base64
from mpl_toolkits.mplot3d import Axes3D

from src.processors.base_processor import BaseProcessor
from src.utils.file_utils import ensure_directory
from src.utils.data_decoders import decode_compressed_data


class PositionProcessor(BaseProcessor):
    """
    Process Position.z compressed streams to extract and visualize track positions.
    """
    
    def __init__(self):
        """Initialize the Position processor."""
        super().__init__()
        self.topic_name = "Position.z"
    
    def extract_position_data(self, timestamped_data):
        """
        Extract position data from timestamped entries.
        
        Args:
            timestamped_data: List of tuples containing (timestamp, compressed_data)
            
        Returns:
            list: List of dictionaries containing position data
        """
        position_data = []
        
        for i, (timestamp, encoded_data) in enumerate(timestamped_data):
            try:
                # Remove surrounding quotes if present
                if encoded_data.startswith('"') and encoded_data.endswith('"'):
                    encoded_data = encoded_data[1:-1]
                
                # Decode base64 and decompress zlib
                decoded_data = zlib.decompress(base64.b64decode(encoded_data), -zlib.MAX_WBITS)
                
                # Convert to JSON
                data = json.loads(decoded_data)
                
                # Extract position entries
                if "Position" in data:
                    for entry in data["Position"]:
                        if "Timestamp" in entry and "Entries" in entry:
                            entry_time = entry["Timestamp"]
                            
                            for driver_number, coords in entry["Entries"].items():
                                position_entry = {
                                    "timestamp": timestamp,
                                    "utc": entry_time,
                                    "driver_number": driver_number,
                                    "x": coords.get("X"),
                                    "y": coords.get("Y"),
                                    "z": coords.get("Z")
                                }
                                
                                position_data.append(position_entry)
                
                # Progress reporting
                if (i + 1) % 100 == 0:
                    print(f"Processed {i+1} position records...")
                    
            except Exception as e:
                print(f"Error processing position data at timestamp {timestamp}: {str(e)}")
                continue
        
        return position_data
    
    def create_track_layout_2d(self, position_data_df, race_name, session_name):
        """
        Create a 2D visualization of the track layout.
        
        Args:
            position_data_df: DataFrame with position data
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the saved visualization
        """
        if position_data_df.empty or not all(col in position_data_df.columns for col in ['x', 'y']):
            print("No valid position data for 2D track layout")
            return None
        
        # Create directory for visualization
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # Get a reference driver with the most data points
        driver_counts = position_data_df['driver_number'].value_counts()
        if driver_counts.empty:
            print("No valid driver data found")
            return None
        
        reference_driver = driver_counts.index[0]
        driver_data = position_data_df[position_data_df['driver_number'] == reference_driver].copy()
        
        # Ensure numerical coordinates
        for col in ['x', 'y']:
            driver_data[col] = pd.to_numeric(driver_data[col], errors='coerce')
        
        # Drop rows with NaN coordinates
        driver_data = driver_data.dropna(subset=['x', 'y'])
        
        if len(driver_data) < 10:
            print("Not enough valid position data points")
            return None
        
        # Create 2D track layout
        plt.figure(figsize=(12, 12))
        
        scatter = plt.scatter(
            driver_data['x'], 
            driver_data['y'], 
            s=1, 
            c=np.arange(len(driver_data)), 
            cmap='viridis'
        )
        
        plt.title(f'Track Layout - {race_name} - {session_name}')
        plt.xlabel('X Coordinate')
        plt.ylabel('Y Coordinate')
        plt.axis('equal')  # Keep correct proportions
        plt.colorbar(scatter, label='Time Sequence')
        plt.grid(True)
        
        # Save the figure
        viz_path = viz_dir / "track_layout_2d.png"
        plt.savefig(viz_path)
        plt.close()
        
        print(f"2D track layout saved to {viz_path}")
        return viz_path
    
    def create_track_layout_3d(self, position_data_df, race_name, session_name):
        """
        Create a 3D visualization of the track layout.
        
        Args:
            position_data_df: DataFrame with position data
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the saved visualization
        """
        if position_data_df.empty or not all(col in position_data_df.columns for col in ['x', 'y', 'z']):
            print("No valid position data for 3D track layout")
            return None
        
        # Create directory for visualization
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # Get a reference driver with the most data points
        driver_counts = position_data_df['driver_number'].value_counts()
        if driver_counts.empty:
            print("No valid driver data found")
            return None
        
        reference_driver = driver_counts.index[0]
        driver_data = position_data_df[position_data_df['driver_number'] == reference_driver].copy()
        
        # Ensure numerical coordinates
        for col in ['x', 'y', 'z']:
            driver_data[col] = pd.to_numeric(driver_data[col], errors='coerce')
        
        # Drop rows with NaN coordinates
        driver_data = driver_data.dropna(subset=['x', 'y', 'z'])
        
        if len(driver_data) < 10:
            print("Not enough valid position data points")
            return None
        
        # Create 3D track layout
        fig = plt.figure(figsize=(14, 10))
        ax = fig.add_subplot(111, projection='3d')
        
        scatter = ax.scatter(
            driver_data['x'], 
            driver_data['y'], 
            driver_data['z'],
            s=1, 
            c=np.arange(len(driver_data)), 
            cmap='viridis'
        )
        
        ax.set_title(f'3D Track Layout - {race_name} - {session_name}')
        ax.set_xlabel('X Coordinate')
        ax.set_ylabel('Y Coordinate')
        ax.set_zlabel('Altitude (Z)')
        fig.colorbar(scatter, label='Time Sequence')
        
        # Save the figure
        viz_path = viz_dir / "track_layout_3d.png"
        plt.savefig(viz_path)
        plt.close()
        
        print(f"3D track layout saved to {viz_path}")
        return viz_path
    
    def create_driver_comparison(self, position_data_df, race_name, session_name):
        """
        Create a comparison of driver trajectories.
        
        Args:
            position_data_df: DataFrame with position data
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            Path: Path to the saved visualization
        """
        if position_data_df.empty or not all(col in position_data_df.columns for col in ['x', 'y']):
            print("No valid position data for driver comparison")
            return None
        
        # Create directory for visualization
        viz_dir = self.processed_dir / race_name / session_name / self.topic_name / "visualizations"
        ensure_directory(viz_dir)
        
        # Get the top 2 drivers with the most data points
        driver_counts = position_data_df['driver_number'].value_counts().head(2)
        if len(driver_counts) < 2:
            print("Not enough drivers for comparison")
            return None
        
        drivers = driver_counts.index.tolist()
        
        # Prepare data for both drivers
        driver_data = {}
        for driver in drivers:
            data = position_data_df[position_data_df['driver_number'] == driver].copy()
            
            # Ensure numerical coordinates
            for col in ['x', 'y']:
                data[col] = pd.to_numeric(data[col], errors='coerce')
            
            # Drop rows with NaN coordinates
            data = data.dropna(subset=['x', 'y'])
            
            if len(data) >= 10:
                # Sample the data to avoid overcrowding (every 5th point)
                driver_data[driver] = data.iloc[::5].reset_index(drop=True)
        
        if len(driver_data) < 2:
            print("Not enough valid data for driver comparison")
            return None
        
        # Create comparison visualization
        plt.figure(figsize=(12, 12))
        
        colors = ['red', 'blue']
        for i, (driver, data) in enumerate(driver_data.items()):
            plt.scatter(
                data['x'], 
                data['y'], 
                s=3, 
                c=colors[i], 
                label=f'Driver #{driver}'
            )
        
        plt.title(f'Driver Trajectory Comparison - {race_name} - {session_name}')
        plt.xlabel('X Coordinate')
        plt.ylabel('Y Coordinate')
        plt.axis('equal')
        plt.legend()
        plt.grid(True)
        
        # Save the figure
        viz_path = viz_dir / "driver_comparison.png"
        plt.savefig(viz_path)
        plt.close()
        
        print(f"Driver comparison visualization saved to {viz_path}")
        return viz_path
    
    def process(self, race_name, session_name):
        """
        Process Position.z data for a specific race and session.
        
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
        
        # Extract position data
        position_data = self.extract_position_data(timestamped_data)
        
        if not position_data:
            print("No position data found")
            return results
        
        # Create a DataFrame with the position data
        position_data_df = pd.DataFrame(position_data)
        
        # Save the processed data
        output_dir = self.processed_dir / race_name / session_name / self.topic_name
        ensure_directory(output_dir)
        
        # Save position data
        position_csv_path = self.save_to_csv(
            position_data_df,
            race_name,
            session_name,
            self.topic_name,
            "position_data.csv"
        )
        results["position_data_file"] = position_csv_path
        
        # Create visualizations
        viz_paths = {}
        
        # 2D track layout
        track_2d_path = self.create_track_layout_2d(
            position_data_df,
            race_name,
            session_name
        )
        if track_2d_path:
            viz_paths["track_2d"] = track_2d_path
        
        # 3D track layout
        track_3d_path = self.create_track_layout_3d(
            position_data_df,
            race_name,
            session_name
        )
        if track_3d_path:
            viz_paths["track_3d"] = track_3d_path
        
        # Driver comparison
        comparison_path = self.create_driver_comparison(
            position_data_df,
            race_name,
            session_name
        )
        if comparison_path:
            viz_paths["driver_comparison"] = comparison_path
        
        results["visualizations"] = viz_paths
        
        return results