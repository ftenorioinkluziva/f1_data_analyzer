"""
Visualizer for position and track data from F1 races.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from pathlib import Path

from src.visualizers.base_visualizer import BaseVisualizer


class PositionVisualizer(BaseVisualizer):
    """
    Create visualizations of position and track data.
    """
    
    def __init__(self):
        """Initialize the position visualizer."""
        super().__init__()
        self.viz_type = "positions"
    
    def create_position_chart(self, positions_df, race_name, session_name, driver_numbers=None):
        """
        Create a chart of driver positions over time.
        
        Args:
            positions_df: DataFrame containing position data
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of specific driver numbers to include
            
        Returns:
            dict: Paths to saved figures
        """
        figure_paths = {}
        
        # Filter for specific drivers if requested
        if driver_numbers:
            selected_drivers = [str(d) for d in driver_numbers]
            filtered_df = positions_df[positions_df['driver_number'].isin(selected_drivers)]
            if filtered_df.empty:
                print("No data found for specified drivers")
                return figure_paths
            positions_df = filtered_df
        
        # Get unique drivers (up to 10 drivers to avoid overcrowding)
        all_drivers = positions_df['driver_number'].unique()
        drivers = all_drivers[:min(10, len(all_drivers))]
        
        # Create position chart
        fig, ax = plt.subplots(figsize=self.figure_sizes.get("position_chart", (14, 8)))
        
        # Create a color map for consistent driver colors
        cmap = plt.cm.get_cmap('tab10', len(drivers))
        
        for i, driver in enumerate(drivers):
            driver_data = positions_df[positions_df['driver_number'] == driver]
            
            # Convert position to numeric
            driver_data['position'] = pd.to_numeric(driver_data['position'], errors='coerce')
            
            # Sort data by timestamp
            driver_data = driver_data.sort_values('timestamp')
            
            # Create an index for the X-axis
            driver_data['index'] = range(len(driver_data))
            
            # Plot positions
            ax.plot(
                driver_data['index'],
                driver_data['position'],
                'o-',
                label=f'Driver #{driver}',
                markersize=3,
                color=cmap(i)
            )
        
        # Invert Y-axis so position 1 is at the top
        ax.invert_yaxis()
        
        ax.set_xlabel('Time (index)')
        ax.set_ylabel('Position')
        ax.set_title(f'Driver Positions - {race_name} - {session_name}')
        ax.grid(True)
        ax.legend()
        
        # Save the figure
        figure_path = self.save_figure(
            fig, 
            race_name, 
            session_name, 
            self.viz_type, 
            "position_chart"
        )
        figure_paths["position_chart"] = figure_path
        
        return figure_paths
    
    def create_track_layout(self, position_data_df, race_name, session_name):
        """
        Create a visualization of the track layout.
        
        Args:
            position_data_df: DataFrame containing position data (x, y, z coordinates)
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Paths to saved figures
        """
        figure_paths = {}
        
        # Ensure required columns exist
        if not all(col in position_data_df.columns for col in ['x', 'y', 'z']):
            print("Position data must contain x, y, z columns")
            return figure_paths
        
        # Convert coordinates to numeric
        for col in ['x', 'y', 'z']:
            position_data_df[col] = pd.to_numeric(position_data_df[col], errors='coerce')
        
        # Get a reference driver with the most data points
        driver_counts = position_data_df['driver_number'].value_counts()
        if driver_counts.empty:
            print("No valid driver data found")
            return figure_paths
        
        reference_driver = driver_counts.index[0]
        driver_data = position_data_df[position_data_df['driver_number'] == reference_driver].copy()
        
        # 2D Track Layout (top view)
        fig, ax = plt.subplots(figsize=self.figure_sizes.get("track_layout", (12, 12)))
        
        scatter = ax.scatter(
            driver_data['x'], 
            driver_data['y'], 
            s=1, 
            c=np.arange(len(driver_data)), 
            cmap='viridis'
        )
        
        ax.set_title(f'Track Layout - {race_name} - {session_name}')
        ax.set_xlabel('X Coordinate')
        ax.set_ylabel('Y Coordinate')
        ax.axis('equal')  # Keep correct proportions
        plt.colorbar(scatter, label='Time Sequence')
        ax.grid(True)
        
        # Save the figure
        figure_path = self.save_figure(
            fig, 
            race_name, 
            session_name, 
            self.viz_type, 
            "track_layout_2d"
        )
        figure_paths["track_layout_2d"] = figure_path
        
        # 3D Track Layout
        fig = plt.figure(figsize=self.figure_sizes.get("track_layout", (12, 12)))
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
        figure_path = self.save_figure(
            fig, 
            race_name, 
            session_name, 
            self.viz_type, 
            "track_layout_3d"
        )
        figure_paths["track_layout_3d"] = figure_path
        
        return figure_paths
    
    def create_driver_trajectory_comparison(self, position_data_df, race_name, session_name, driver_numbers=None):
        """
        Create a comparison of driver trajectories on the track.
        
        Args:
            position_data_df: DataFrame containing position data (x, y, z coordinates)
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of specific driver numbers to include
            
        Returns:
            dict: Paths to saved figures
        """
        figure_paths = {}
        
        # Ensure required columns exist
        if not all(col in position_data_df.columns for col in ['x', 'y']):
            print("Position data must contain x, y columns")
            return figure_paths
        
        # Convert coordinates to numeric
        for col in ['x', 'y']:
            position_data_df[col] = pd.to_numeric(position_data_df[col], errors='coerce')
        
        # Filter for specific drivers if requested
        if driver_numbers:
            selected_drivers = [str(d) for d in driver_numbers]
            filtered_df = position_data_df[position_data_df['driver_number'].isin(selected_drivers)]
            if len(filtered_df) < 2:
                print("Need at least two drivers with data to create comparison")
                return figure_paths
            position_data_df = filtered_df
        
        # Get unique drivers (limit to comparing 2 drivers)
        drivers = position_data_df['driver_number'].unique()[:2]
        
        if len(drivers) < 2:
            print("Need at least two drivers to create comparison")
            return figure_paths
        
        # Get data for both drivers
        driver1_data = position_data_df[position_data_df['driver_number'] == drivers[0]].iloc[:300]  # Limit points for clarity
        driver2_data = position_data_df[position_data_df['driver_number'] == drivers[1]].iloc[:300]
        
        # Create comparison visualization
        fig, ax = plt.subplots(figsize=self.figure_sizes.get("track_layout", (12, 12)))
        
        ax.scatter(
            driver1_data['x'], 
            driver1_data['y'], 
            s=3, 
            c='red', 
            label=f'Driver #{drivers[0]}'
        )
        
        ax.scatter(
            driver2_data['x'], 
            driver2_data['y'], 
            s=3, 
            c='blue', 
            label=f'Driver #{drivers[1]}'
        )
        
        ax.set_title(f'Trajectory Comparison - {race_name} - {session_name}')
        ax.set_xlabel('X Coordinate')
        ax.set_ylabel('Y Coordinate')
        ax.axis('equal')
        ax.legend()
        ax.grid(True)
        
        # Save the figure
        figure_path = self.save_figure(
            fig, 
            race_name, 
            session_name, 
            self.viz_type, 
            f"trajectory_comparison_{drivers[0]}_{drivers[1]}"
        )
        figure_paths["trajectory_comparison"] = figure_path
        
        return figure_paths
    
    def create_visualizations(self, race_name, session_name, driver_numbers=None):
        """
        Create position visualizations for a specific race and session.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of driver numbers to visualize
            
        Returns:
            dict: Visualization results
        """
        results = {}
        
        # Check if position data exists
        positions_path = self.get_processed_file_path(
            race_name, 
            session_name, 
            "TimingData", 
            "positions.csv"
        )
        
        if positions_path.exists():
            print(f"Creating position charts for {race_name}/{session_name}")
            positions_df = pd.read_csv(positions_path)
            
            # Create position chart
            position_figures = self.create_position_chart(
                positions_df,
                race_name,
                session_name,
                driver_numbers
            )
            results["position_figures"] = position_figures
        else:
            print(f"Position data not found: {positions_path}")
        
        # Check if track position data exists (x, y, z coordinates)
        position_xyz_path = self.get_processed_file_path(
            race_name, 
            session_name, 
            "Position.z", 
            "position_data.csv"
        )
        
        if position_xyz_path.exists():
            print(f"Creating track layout visualizations for {race_name}/{session_name}")
            position_xyz_df = pd.read_csv(position_xyz_path)
            
            # Create track layout
            track_figures = self.create_track_layout(
                position_xyz_df,
                race_name,
                session_name
            )
            results["track_figures"] = track_figures
            
            # Create driver trajectory comparison
            if driver_numbers and len(driver_numbers) >= 2:
                trajectory_figures = self.create_driver_trajectory_comparison(
                    position_xyz_df,
                    race_name,
                    session_name,
                    driver_numbers
                )
                results["trajectory_figures"] = trajectory_figures
        else:
            print(f"Track position data not found: {position_xyz_path}")
        
        return results