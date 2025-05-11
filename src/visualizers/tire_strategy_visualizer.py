"""
Visualizer for tire strategy data from F1 races.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

import config
from src.visualizers.base_visualizer import BaseVisualizer


class TireStrategyVisualizer(BaseVisualizer):
    """
    Create visualizations of tire strategy data.
    """
    
    def __init__(self):
        """Initialize the tire strategy visualizer."""
        super().__init__()
        self.viz_type = "tire_strategy"
        self.compound_colors = config.TIRE_COMPOUND_COLORS
    
    def create_tire_strategy_chart(self, tire_stints_df, race_name, session_name, driver_numbers=None):
        """
        Create a chart visualizing tire strategy.
        
        Args:
            tire_stints_df: DataFrame containing tire stint data
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of specific driver numbers to include
            
        Returns:
            dict: Paths to saved figures
        """
        figure_paths = {}
        
        # Convert driver_number to string if not already
        tire_stints_df['driver_number'] = tire_stints_df['driver_number'].astype(str)
        
        # Filter for specific drivers if requested
        if driver_numbers:
            selected_drivers = [str(d) for d in driver_numbers]
            filtered_df = tire_stints_df[tire_stints_df['driver_number'].isin(selected_drivers)]
            if filtered_df.empty:
                print("No data found for specified drivers")
                return figure_paths
            tire_stints_df = filtered_df
        
        # Get top drivers (or all if less than 10)
        top_drivers = sorted(tire_stints_df['driver_number'].unique())[:10]
        
        # Create tire strategy chart
        plt.figure(figsize=self.figure_sizes.get("tire_strategy", (16, 8)))
        
        # Set up y-positions and labels
        y_positions = []
        y_labels = []
        
        for i, driver in enumerate(top_drivers):
            driver_stints = tire_stints_df[tire_stints_df['driver_number'] == driver].sort_values('stint_number')
            
            y_pos = i
            y_positions.append(y_pos)
            y_labels.append(f"Driver #{driver}")
            
            for _, stint in driver_stints.iterrows():
                compound = stint['compound']
                color = self.compound_colors.get(compound, "gray")
                
                # Add a bar for the stint
                # Using the stint number as X position
                plt.barh(
                    y_pos, 
                    1,  # Width of 1 unit
                    left=stint['stint_number'] - 1,  # Starting position (0, 1, 2, ...)
                    color=color, 
                    edgecolor='black'
                )
                
                # Add compound text
                plt.text(
                    stint['stint_number'] - 0.5,  # Center of the bar
                    y_pos, 
                    compound[0],  # First letter of the compound
                    ha='center', 
                    va='center',
                    color='black' if color in ['yellow', 'white'] else 'white'
                )
        
        # Configure the chart
        plt.yticks(y_positions, y_labels)
        plt.xlabel('Stint Number')
        plt.title(f'Tire Strategy - {race_name} - {session_name}')
        plt.grid(axis='x')
        
        # Add legend
        legend_elements = [
            plt.Rectangle((0, 0), 1, 1, color=color, edgecolor='black', label=compound)
            for compound, color in self.compound_colors.items()
        ]
        plt.legend(handles=legend_elements, bbox_to_anchor=(1.05, 1), loc='upper left')
        
        plt.tight_layout()
        
        # Save the figure
        figure_path = self.save_figure(
            plt.gcf(), 
            race_name, 
            session_name, 
            self.viz_type, 
            "tire_strategy"
        )
        figure_paths["tire_strategy"] = figure_path
        
        return figure_paths
    
    def create_tire_strategy_by_lap_chart(self, stint_laps_df, race_name, session_name, driver_numbers=None):
        """
        Create a chart visualizing tire strategy with lap information.
        
        Args:
            stint_laps_df: DataFrame containing stint-lap correlation data
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of specific driver numbers to include
            
        Returns:
            dict: Paths to saved figures
        """
        figure_paths = {}
        
        # Convert driver_number to string if not already
        stint_laps_df['driver_number'] = stint_laps_df['driver_number'].astype(str)
        
        # Filter for specific drivers if requested
        if driver_numbers:
            selected_drivers = [str(d) for d in driver_numbers]
            filtered_df = stint_laps_df[stint_laps_df['driver_number'].isin(selected_drivers)]
            if filtered_df.empty:
                print("No data found for specified drivers")
                return figure_paths
            stint_laps_df = filtered_df
        
        # Get top drivers (or all if less than 10)
        top_drivers = sorted(stint_laps_df['driver_number'].unique())[:10]
        
        # Create tire strategy chart with lap information
        plt.figure(figsize=self.figure_sizes.get("tire_strategy", (16, 8)))
        
        # Set up y-positions and labels
        y_positions = []
        y_labels = []
        
        for i, driver in enumerate(top_drivers):
            driver_stints = stint_laps_df[stint_laps_df['driver_number'] == driver].sort_values('stint_number')
            
            y_pos = i
            y_positions.append(y_pos)
            y_labels.append(f"Driver #{driver}")
            
            for _, stint in driver_stints.iterrows():
                compound = stint['compound']
                color = self.compound_colors.get(compound, "gray")
                
                # Add a bar for the stint using actual lap ranges
                lap_start = stint['lap_start']
                lap_end = stint['lap_end']
                stint_length = lap_end - lap_start + 1
                
                plt.barh(
                    y_pos, 
                    stint_length,
                    left=lap_start,
                    color=color, 
                    edgecolor='black'
                )
                
                # Add compound text
                plt.text(
                    lap_start + stint_length/2,  # Center of the bar
                    y_pos, 
                    f"{compound[0]}{'-N' if stint['new_tire'] else ''}",  # First letter + 'N' for new tires
                    ha='center', 
                    va='center',
                    color='black' if color in ['yellow', 'white'] else 'white'
                )
        
        # Configure the chart
        plt.yticks(y_positions, y_labels)
        plt.xlabel('Lap Number')
        plt.title(f'Tire Strategy by Lap - {race_name} - {session_name}')
        plt.grid(axis='x')
        
        # Add legend
        legend_elements = [
            plt.Rectangle((0, 0), 1, 1, color=color, edgecolor='black', label=f"{compound} (New)" if new else compound)
            for compound, color in self.compound_colors.items()
            for new in [True, False]
        ]
        plt.legend(handles=legend_elements, bbox_to_anchor=(1.05, 1), loc='upper left')
        
        plt.tight_layout()
        
        # Save the figure
        figure_path = self.save_figure(
            plt.gcf(), 
            race_name, 
            session_name, 
            self.viz_type, 
            "tire_strategy_by_lap"
        )
        figure_paths["tire_strategy_by_lap"] = figure_path
        
        return figure_paths
    
    def create_tire_compound_distribution(self, tire_stints_df, race_name, session_name):
        """
        Create a chart showing the distribution of tire compounds used.
        
        Args:
            tire_stints_df: DataFrame containing tire stint data
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Paths to saved figures
        """
        figure_paths = {}
        
        # Create compound distribution chart
        plt.figure(figsize=(10, 6))
        
        # Get compound counts
        compound_counts = tire_stints_df['compound'].value_counts()
        
        # Plot bar chart with appropriate colors
        bars = compound_counts.plot(
            kind='bar', 
            color=[self.compound_colors.get(c, "gray") for c in compound_counts.index]
        )
        
        plt.title(f'Tire Compound Distribution - {race_name} - {session_name}')
        plt.xlabel('Compound')
        plt.ylabel('Number of Stints')
        plt.tight_layout()
        
        # Save the figure
        figure_path = self.save_figure(
            plt.gcf(), 
            race_name, 
            session_name, 
            self.viz_type, 
            "tire_compound_distribution"
        )
        figure_paths["tire_compound_distribution"] = figure_path
        
        return figure_paths
    
    def create_stint_length_distribution(self, stint_laps_df, race_name, session_name):
        """
        Create a chart showing the distribution of stint lengths by compound.
        
        Args:
            stint_laps_df: DataFrame containing stint-lap correlation data
            race_name: Name of the race
            session_name: Name of the session
            
        Returns:
            dict: Paths to saved figures
        """
        figure_paths = {}
        
        # Create stint length distribution chart
        plt.figure(figsize=(12, 6))
        
        # Create histogram of stint lengths grouped by compound
        stint_laps_df['stint_length'].hist(by=stint_laps_df['compound'], bins=range(0, 50, 5))
        
        plt.suptitle(f'Stint Length Distribution by Compound - {race_name} - {session_name}')
        plt.tight_layout()
        
        # Save the figure
        figure_path = self.save_figure(
            plt.gcf(), 
            race_name, 
            session_name, 
            self.viz_type, 
            "stint_length_distribution"
        )
        figure_paths["stint_length_distribution"] = figure_path
        
        return figure_paths
    
    def create_visualizations(self, race_name, session_name, driver_numbers=None):
        """
        Create tire strategy visualizations for a specific race and session.
        
        Args:
            race_name: Name of the race
            session_name: Name of the session
            driver_numbers: Optional list of driver numbers to visualize
            
        Returns:
            dict: Visualization results
        """
        results = {}
        
        # Check if tire stint data exists
        tire_stints_path = self.get_processed_file_path(
            race_name, 
            session_name, 
            "TimingAppData", 
            "tire_stints.csv"
        )
        
        if tire_stints_path.exists():
            print(f"Creating tire strategy visualizations for {race_name}/{session_name}")
            tire_stints_df = pd.read_csv(tire_stints_path)
            
            # Create tire strategy chart
            strategy_figures = self.create_tire_strategy_chart(
                tire_stints_df,
                race_name,
                session_name,
                driver_numbers
            )
            results["strategy_figures"] = strategy_figures
            
            # Create tire compound distribution chart
            distribution_figures = self.create_tire_compound_distribution(
                tire_stints_df,
                race_name,
                session_name
            )
            results["distribution_figures"] = distribution_figures
        else:
            print(f"Tire stint data not found: {tire_stints_path}")
        
        # Check if stint-lap correlation data exists
        stint_laps_path = self.get_processed_file_path(
            race_name, 
            session_name, 
            "StintAnalysis", 
            "stint_laps.csv"
        )
        
        if stint_laps_path.exists():
            print(f"Creating stint-lap correlation visualizations for {race_name}/{session_name}")
            stint_laps_df = pd.read_csv(stint_laps_path)
            
            # Create tire strategy by lap chart
            strategy_by_lap_figures = self.create_tire_strategy_by_lap_chart(
                stint_laps_df,
                race_name,
                session_name,
                driver_numbers
            )
            results["strategy_by_lap_figures"] = strategy_by_lap_figures
            
            # Create stint length distribution chart
            stint_length_figures = self.create_stint_length_distribution(
                stint_laps_df,
                race_name,
                session_name
            )
            results["stint_length_figures"] = stint_length_figures
        else:
            print(f"Stint-lap correlation data not found: {stint_laps_path}")
        
        return results