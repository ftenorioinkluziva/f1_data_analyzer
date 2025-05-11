"""
Visualizer for lap time data from F1 races.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from src.visualizers.base_visualizer import BaseVisualizer
from src.utils.time_utils import convert_lap_time_to_seconds


class LapTimeVisualizer(BaseVisualizer):
    """
    Create visualizations of lap time data.
    """
    
    def __init__(self):
        """Initialize the lap time visualizer."""
        super().__init__()
        self.viz_type = "lap_times"
    
    def create_lap_time_chart(self, lap_times_df, race_name, session_name, driver_numbers=None):
        """
        Create a line chart of lap times.
        
        Args:
            lap_times_df: DataFrame containing lap time data
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of specific driver numbers to include
            
        Returns:
            dict: Paths to saved figures
        """
        figure_paths = {}
        
        # Convert lap times to seconds
        lap_times_df['lap_time_seconds'] = lap_times_df['lap_time'].apply(convert_lap_time_to_seconds)
        
        # Filter for specific drivers if requested
        if driver_numbers:
            selected_drivers = [str(d) for d in driver_numbers]
            filtered_df = lap_times_df[lap_times_df['driver_number'].isin(selected_drivers)]
            if filtered_df.empty:
                print("No data found for specified drivers")
                return figure_paths
            lap_times_df = filtered_df
        
        # Get unique drivers
        drivers = lap_times_df['driver_number'].unique()
        
        # Create overall lap time comparison chart
        fig, ax = plt.subplots(figsize=self.figure_sizes.get("lap_times", (14, 8)))
        
        # Create a color map for consistent driver colors
        cmap = plt.cm.get_cmap('tab10', len(drivers))
        
        for i, driver in enumerate(drivers):
            driver_data = lap_times_df[lap_times_df['driver_number'] == driver]
            
            # Group by lap number and get the fastest time if there are duplicates
            driver_laps = driver_data.groupby('lap_number')['lap_time_seconds'].min().reset_index()
            
            # Sort by lap number
            driver_laps = driver_laps.sort_values('lap_number')
            
            # Plot lap times
            ax.plot(
                driver_laps['lap_number'], 
                driver_laps['lap_time_seconds'], 
                'o-', 
                label=f"Driver #{driver}",
                color=cmap(i)
            )
        
        ax.set_xlabel('Lap Number')
        ax.set_ylabel('Lap Time (seconds)')
        ax.set_title(f'Lap Times Comparison - {race_name} - {session_name}')
        ax.grid(True)
        ax.legend()
        
        # Save the figure
        figure_path = self.save_figure(
            fig, 
            race_name, 
            session_name, 
            self.viz_type, 
            "lap_times_comparison"
        )
        figure_paths["comparison"] = figure_path
        
        # Create individual driver lap time charts
        for driver in drivers[:5]:  # Limit to 5 drivers to avoid too many charts
            driver_data = lap_times_df[lap_times_df['driver_number'] == driver]
            
            # Group by lap number and get the fastest time if there are duplicates
            driver_laps = driver_data.groupby('lap_number')['lap_time_seconds'].min().reset_index()
            
            # Sort by lap number
            driver_laps = driver_laps.sort_values('lap_number')
            
            fig, ax = plt.subplots(figsize=self.figure_sizes.get("lap_times", (14, 8)))
            
            # Plot lap times
            ax.plot(
                driver_laps['lap_number'], 
                driver_laps['lap_time_seconds'], 
                'o-', 
                label=f"Lap Times",
                color='blue'
            )
            
            # Add trend line
            if len(driver_laps) > 1:
                z = np.polyfit(driver_laps['lap_number'], driver_laps['lap_time_seconds'], 1)
                p = np.poly1d(z)
                ax.plot(
                    driver_laps['lap_number'], 
                    p(driver_laps['lap_number']), 
                    '--', 
                    label=f"Trend",
                    color='red'
                )
            
            ax.set_xlabel('Lap Number')
            ax.set_ylabel('Lap Time (seconds)')
            ax.set_title(f'Lap Times - Driver #{driver} - {race_name} - {session_name}')
            ax.grid(True)
            ax.legend()
            
            # Save the figure
            figure_path = self.save_figure(
                fig, 
                race_name, 
                session_name, 
                self.viz_type, 
                f"lap_times_driver_{driver}"
            )
            figure_paths[f"driver_{driver}"] = figure_path
        
        return figure_paths
    
    def create_sector_time_chart(self, sector_times_df, race_name, session_name, driver_numbers=None):
        """
        Create charts of sector times.
        
        Args:
            sector_times_df: DataFrame containing sector time data
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of specific driver numbers to include
            
        Returns:
            dict: Paths to saved figures
        """
        figure_paths = {}
        
        # Convert sector times to seconds
        sector_times_df['time_seconds'] = sector_times_df['time'].apply(convert_lap_time_to_seconds)
        
        # Filter for specific drivers if requested
        if driver_numbers:
            selected_drivers = [str(d) for d in driver_numbers]
            filtered_df = sector_times_df[sector_times_df['driver_number'].isin(selected_drivers)]
            if filtered_df.empty:
                print("No data found for specified drivers")
                return figure_paths
            sector_times_df = filtered_df
        
        # Create sector time distribution chart
        fig, ax = plt.subplots(figsize=self.figure_sizes.get("lap_times", (14, 8)))
        
        # Create box plots for each sector
        sector_groups = sector_times_df.groupby('sector')
        
        sectors = []
        times = []
        
        for sector, group in sector_groups:
            sectors.extend([f"Sector {sector}"] * len(group))
            times.extend(group['time_seconds'])
        
        ax.boxplot([sector_times_df[sector_times_df['sector'] == s]['time_seconds'] 
                   for s in sector_times_df['sector'].unique()])
        
        ax.set_xlabel('Sector')
        ax.set_ylabel('Time (seconds)')
        ax.set_title(f'Sector Times Distribution - {race_name} - {session_name}')
        ax.set_xticklabels([f"Sector {s}" for s in sector_times_df['sector'].unique()])
        ax.grid(True)
        
        # Save the figure
        figure_path = self.save_figure(
            fig, 
            race_name, 
            session_name, 
            self.viz_type, 
            "sector_times_distribution"
        )
        figure_paths["sector_distribution"] = figure_path
        
        # Create individual driver sector time charts for top drivers
        drivers = sector_times_df['driver_number'].unique()
        
        for driver in drivers[:3]:  # Limit to 3 drivers
            driver_data = sector_times_df[sector_times_df['driver_number'] == driver]
            
            fig, ax = plt.subplots(figsize=self.figure_sizes.get("lap_times", (14, 8)))
            
            for sector in sorted(driver_data['sector'].unique()):
                sector_data = driver_data[driver_data['sector'] == sector]
                sector_data = sector_data.sort_values('timestamp')
                
                # Plot sector times evolution
                ax.plot(
                    range(len(sector_data)), 
                    sector_data['time_seconds'], 
                    'o-', 
                    label=f"Sector {sector}"
                )
            
            ax.set_xlabel('Index')
            ax.set_ylabel('Time (seconds)')
            ax.set_title(f'Sector Times Evolution - Driver #{driver} - {race_name} - {session_name}')
            ax.grid(True)
            ax.legend()
            
            # Save the figure
            figure_path = self.save_figure(
                fig, 
                race_name, 
                session_name, 
                self.viz_type, 
                f"sector_times_driver_{driver}"
            )
            figure_paths[f"sector_driver_{driver}"] = figure_path
        
        return figure_paths
    
    def create_visualizations(self, race_name, session_name, driver_numbers=None):
        """
        Create lap time visualizations for a specific race and session.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of driver numbers to visualize
            
        Returns:
            dict: Visualization results
        """
        results = {}
        
        # Check if lap times data exists
        lap_times_path = self.get_processed_file_path(
            race_name, 
            session_name, 
            "TimingData", 
            "lap_times.csv"
        )
        
        if lap_times_path.exists():
            print(f"Creating lap time visualizations for {race_name}/{session_name}")
            lap_times_df = pd.read_csv(lap_times_path)
            
            # Create lap time chart
            lap_time_figures = self.create_lap_time_chart(
                lap_times_df,
                race_name,
                session_name,
                driver_numbers
            )
            results["lap_time_figures"] = lap_time_figures
        else:
            print(f"Lap times data not found: {lap_times_path}")
        
        # Check if sector times data exists
        sector_times_path = self.get_processed_file_path(
            race_name, 
            session_name, 
            "TimingData", 
            "sector_times.csv"
        )
        
        if sector_times_path.exists():
            print(f"Creating sector time visualizations for {race_name}/{session_name}")
            sector_times_df = pd.read_csv(sector_times_path)
            
            # Create sector time chart
            sector_time_figures = self.create_sector_time_chart(
                sector_times_df,
                race_name,
                session_name,
                driver_numbers
            )
            results["sector_time_figures"] = sector_time_figures
        else:
            print(f"Sector times data not found: {sector_times_path}")
        
        return results