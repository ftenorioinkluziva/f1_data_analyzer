"""
Visualizer for car telemetry data from F1 races.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from src.visualizers.base_visualizer import BaseVisualizer


class TelemetryVisualizer(BaseVisualizer):
    """
    Create visualizations of car telemetry data.
    """
    
    def __init__(self):
        """Initialize the telemetry visualizer."""
        super().__init__()
        self.viz_type = "telemetry"
    
    def create_speed_chart(self, car_data_df, race_name, session_name, driver_numbers=None):
        """
        Create a chart of speed data.
        
        Args:
            car_data_df: DataFrame containing car telemetry data
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
            filtered_df = car_data_df[car_data_df['driver_number'].isin(selected_drivers)]
            if filtered_df.empty:
                print("No data found for specified drivers")
                return figure_paths
            car_data_df = filtered_df
        
        # Get unique drivers
        drivers = car_data_df['driver_number'].unique()
        
        # Create individual driver speed charts
        for driver in drivers[:5]:  # Limit to 5 drivers to avoid too many charts
            driver_data = car_data_df[car_data_df['driver_number'] == driver].copy()
            
            # Create index for plotting
            driver_data = driver_data.reset_index(drop=True)
            driver_data['index'] = driver_data.index
            
            fig, ax = plt.subplots(figsize=self.figure_sizes.get("telemetry", (16, 8)))
            
            # Plot speed
            ax.plot(
                driver_data['index'],
                driver_data['speed'],
                'b-',
                label='Speed'
            )
            
            ax.set_xlabel('Index')
            ax.set_ylabel('Speed (km/h)')
            ax.set_title(f'Speed - Driver #{driver} - {race_name} - {session_name}')
            ax.grid(True)
            ax.legend()
            
            # Save the figure
            figure_path = self.save_figure(
                fig, 
                race_name, 
                session_name, 
                self.viz_type, 
                f"speed_driver_{driver}"
            )
            figure_paths[f"speed_driver_{driver}"] = figure_path
        
        return figure_paths
    
    def create_throttle_brake_chart(self, car_data_df, race_name, session_name, driver_numbers=None):
        """
        Create a chart of throttle and brake data.
        
        Args:
            car_data_df: DataFrame containing car telemetry data
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
            filtered_df = car_data_df[car_data_df['driver_number'].isin(selected_drivers)]
            if filtered_df.empty:
                print("No data found for specified drivers")
                return figure_paths
            car_data_df = filtered_df
        
        # Get unique drivers
        drivers = car_data_df['driver_number'].unique()
        
        # Create individual driver throttle/brake charts
        for driver in drivers[:5]:  # Limit to 5 drivers
            driver_data = car_data_df[car_data_df['driver_number'] == driver].copy()
            
            # Create index for plotting
            driver_data = driver_data.reset_index(drop=True)
            driver_data['index'] = driver_data.index
            
            fig, ax = plt.subplots(figsize=self.figure_sizes.get("telemetry", (16, 8)))
            
            # Plot throttle and brake
            ax.plot(
                driver_data['index'],
                driver_data['throttle'],
                'g-',
                label='Throttle'
            )
            
            ax.plot(
                driver_data['index'],
                driver_data['brake'],
                'r-',
                label='Brake'
            )
            
            ax.set_xlabel('Index')
            ax.set_ylabel('Percentage')
            ax.set_title(f'Throttle/Brake - Driver #{driver} - {race_name} - {session_name}')
            ax.grid(True)
            ax.legend()
            
            # Save the figure
            figure_path = self.save_figure(
                fig, 
                race_name, 
                session_name, 
                self.viz_type, 
                f"throttle_brake_driver_{driver}"
            )
            figure_paths[f"throttle_brake_driver_{driver}"] = figure_path
        
        return figure_paths
    
    def create_composite_telemetry_chart(self, car_data_df, race_name, session_name, driver_numbers=None):
        """
        Create a composite chart with multiple telemetry channels.
        
        Args:
            car_data_df: DataFrame containing car telemetry data
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
            filtered_df = car_data_df[car_data_df['driver_number'].isin(selected_drivers)]
            if filtered_df.empty:
                print("No data found for specified drivers")
                return figure_paths
            car_data_df = filtered_df
        
        # Get unique drivers
        drivers = car_data_df['driver_number'].unique()
        
        # Create detailed telemetry view for a sample of data
        for driver in drivers[:3]:  # Limit to 3 drivers
            driver_data = car_data_df[car_data_df['driver_number'] == driver].copy()
            
            # Create index for plotting
            driver_data = driver_data.reset_index(drop=True)
            driver_data['index'] = driver_data.index
            
            # Extract a sample of consecutive points
            if len(driver_data) > 200:
                sample = driver_data.iloc[100:300]  # 200 points in the middle
            else:
                sample = driver_data  # Use all points if less than 200
            
            # Create a detailed chart with speed, RPM, and gear
            fig, ax1 = plt.subplots(figsize=self.figure_sizes.get("telemetry", (16, 8)))
            
            # Speed on primary Y-axis
            ax1.set_xlabel('Index')
            ax1.set_ylabel('Speed (km/h)', color='tab:blue')
            ax1.plot(sample['index'], sample['speed'], 'b-', label='Speed')
            ax1.tick_params(axis='y', labelcolor='tab:blue')
            
            # RPM on secondary Y-axis
            ax2 = ax1.twinx()
            ax2.set_ylabel('RPM', color='tab:red')
            ax2.plot(sample['index'], sample['rpm'], 'r-', label='RPM')
            ax2.tick_params(axis='y', labelcolor='tab:red')
            
            # Gear as a third dataset
            ax3 = ax1.twinx()
            ax3.spines["right"].set_position(("axes", 1.1))  # Offset to not overlap with second Y-axis
            ax3.set_ylabel('Gear', color='tab:green')
            ax3.plot(sample['index'], sample['gear'], 'g-', label='Gear')
            ax3.tick_params(axis='y', labelcolor='tab:green')
            
            # Title and grid
            plt.title(f'Telemetry Detail - Driver #{driver} - {race_name} - {session_name}')
            ax1.grid(True)
            
            # Legend
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            lines3, labels3 = ax3.get_legend_handles_labels()
            ax1.legend(lines1 + lines2 + lines3, labels1 + labels2 + labels3, loc='upper right')
            
            plt.tight_layout()
            
            # Save the figure
            figure_path = self.save_figure(
                fig, 
                race_name, 
                session_name, 
                self.viz_type, 
                f"telemetry_detail_driver_{driver}"
            )
            figure_paths[f"telemetry_detail_driver_{driver}"] = figure_path
        
        return figure_paths
    
    def create_drs_usage_chart(self, car_data_df, race_name, session_name, driver_numbers=None):
        """
        Create a chart showing DRS usage.
        
        Args:
            car_data_df: DataFrame containing car telemetry data
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of specific driver numbers to include
            
        Returns:
            dict: Paths to saved figures
        """
        figure_paths = {}
        
        # Check if DRS data exists
        if 'drs' not in car_data_df.columns or car_data_df['drs'].isna().all():
            print("No DRS data available")
            return figure_paths
        
        # Filter for specific drivers if requested
        if driver_numbers:
            selected_drivers = [str(d) for d in driver_numbers]
            filtered_df = car_data_df[car_data_df['driver_number'].isin(selected_drivers)]
            if filtered_df.empty:
                print("No data found for specified drivers")
                return figure_paths
            car_data_df = filtered_df
        
        # Get unique drivers
        drivers = car_data_df['driver_number'].unique()
        
        # Create DRS usage chart
        fig, ax = plt.subplots(figsize=self.figure_sizes.get("telemetry", (16, 8)))
        
        # Create a color map for consistent driver colors
        cmap = plt.cm.get_cmap('tab10', len(drivers))
        
        for i, driver in enumerate(drivers):
            driver_data = car_data_df[car_data_df['driver_number'] == driver].copy()
            
            # Create index for plotting
            driver_data = driver_data.reset_index(drop=True)
            driver_data['index'] = driver_data.index
            
            # Plot DRS usage
            ax.plot(
                driver_data['index'],
                driver_data['drs'],
                '-',
                label=f'Driver #{driver}',
                color=cmap(i)
            )
        
        ax.set_xlabel('Index')
        ax.set_ylabel('DRS State')
        ax.set_title(f'DRS Usage - {race_name} - {session_name}')
        ax.grid(True)
        ax.legend()
        
        # Save the figure
        figure_path = self.save_figure(
            fig, 
            race_name, 
            session_name, 
            self.viz_type, 
            "drs_usage"
        )
        figure_paths["drs_usage"] = figure_path
        
        return figure_paths
    
    def create_visualizations(self, race_name, session_name, driver_numbers=None):
        """
        Create telemetry visualizations for a specific race and session.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of driver numbers to visualize
            
        Returns:
            dict: Visualization results
        """
        results = {}
        
        # Check if car data exists
        car_data_path = self.get_processed_file_path(
            race_name, 
            session_name, 
            "CarData.z", 
            "car_data.csv"
        )
        
        if car_data_path.exists():
            print(f"Creating telemetry visualizations for {race_name}/{session_name}")
            car_data_df = pd.read_csv(car_data_path)
            
            # Create speed charts
            speed_figures = self.create_speed_chart(
                car_data_df,
                race_name,
                session_name,
                driver_numbers
            )
            results["speed_figures"] = speed_figures
            
            # Create throttle/brake charts
            throttle_brake_figures = self.create_throttle_brake_chart(
                car_data_df,
                race_name,
                session_name,
                driver_numbers
            )
            results["throttle_brake_figures"] = throttle_brake_figures
            
            # Create composite telemetry charts
            composite_figures = self.create_composite_telemetry_chart(
                car_data_df,
                race_name,
                session_name,
                driver_numbers
            )
            results["composite_figures"] = composite_figures
            
            # Create DRS usage charts
            drs_figures = self.create_drs_usage_chart(
                car_data_df,
                race_name,
                session_name,
                driver_numbers
            )
            results["drs_figures"] = drs_figures
        else:
            print(f"Car telemetry data not found: {car_data_path}")
        
        return results