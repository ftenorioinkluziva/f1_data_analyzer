#!/usr/bin/env python3
"""
lap_analyzer.py - Script for visualizing and analyzing F1 lap times

This script reads processed data from TimingData from the F1 Data Analyzer and generates
detailed visualizations of lap times during a session.

Usage:
    python lap_analyzer.py --meeting 1264 --session 1297 --driver 1 --output-dir visualizations
"""

import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
from pathlib import Path
from datetime import datetime, timedelta
from scipy.stats import zscore
import re

# Constants and settings
FIG_SIZE = (16, 10)  # Default figure size
DPI = 300  # Resolution for saved images

# Team colors for visualization
TEAM_COLORS = {
    'Mercedes': '#00D2BE',
    'Red Bull': '#0600EF',
    'Ferrari': '#DC0000',
    'Alpine': '#0090FF',
    'McLaren': '#FF8700',
    'Alfa Romeo': '#900000',
    'Aston Martin': '#006F62',
    'Haas': '#FFFFFF',
    'AlphaTauri': '#2B4562',
    'Williams': '#005AFF'
}

# Tire compound colors
COMPOUND_COLORS = {
    'SOFT': 'red',
    'MEDIUM': 'yellow',
    'HARD': 'white',
    'INTERMEDIATE': 'green',
    'WET': 'blue',
    'UNKNOWN': 'gray'
}

def parse_args():
    """Process command line arguments."""
    parser = argparse.ArgumentParser(description='Visualize and analyze F1 lap times')
    
    parser.add_argument('--meeting', type=str, required=True,
                        help='Meeting key (e.g., 1264 for Miami GP)')
    
    parser.add_argument('--session', type=str, required=True,
                        help='Session key (e.g., 1297 for the main race)')
    
    parser.add_argument('--driver', type=str, default=None,
                        help='Specific driver number to analyze in detail')
    
    parser.add_argument('--compare', type=str, nargs='+', default=None,
                        help='Driver numbers to compare (e.g., 1 16 44)')
    
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Directory to save visualizations')
    
    parser.add_argument('--race-name', type=str, default=None,
                        help='Custom name for the event')
    
    parser.add_argument('--session-name', type=str, default=None,
                        help='Custom name for the session')
    
    parser.add_argument('--dark-mode', action='store_true', default=False,
                        help='Use dark theme for visualizations')
    
    parser.add_argument('--fastest-only', action='store_true', default=False,
                        help='Analyze only the fastest laps')
    
    parser.add_argument('--include-tires', action='store_true', default=False,
                        help='Include tire compound information if available')
    
    return parser.parse_args()

def load_lap_data(meeting_key, session_key):
    """
    Load lap time data from the session.
    
    Args:
        meeting_key: Meeting key
        session_key: Session key
        
    Returns:
        pd.DataFrame: DataFrame containing lap time data or None if not available
    """
    lap_file = f"f1_data/processed/{meeting_key}/{session_key}/TimingData/lap_times.csv"
    
    if not os.path.exists(lap_file):
        print(f"Warning: Lap time data not found: {lap_file}")
        return None
    
    try:
        print(f"Loading lap time data from: {lap_file}")
        df = pd.read_csv(lap_file)
        
        # Convert driver numbers to strings for consistency
        if 'driver_number' in df.columns:
            df['driver_number'] = df['driver_number'].astype(str)
        
        # Sort by timestamp if available
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp')
        
        print(f"Lap time data loaded: {len(df)} records")
        
        # Process lap times to seconds if in string format
        if 'lap_time' in df.columns and isinstance(df['lap_time'].iloc[0], str):
            df['lap_seconds'] = df['lap_time'].apply(convert_lap_time_to_seconds)
        
        return df
    except Exception as e:
        print(f"Error loading lap time data: {str(e)}")
        return None

def convert_lap_time_to_seconds(lap_time):
    """
    Convert lap time from string format (MM:SS.sss) to seconds.
    
    Args:
        lap_time: Lap time string (e.g., "1:30.456")
        
    Returns:
        float: Lap time in seconds
    """
    try:
        if pd.isna(lap_time) or lap_time == "":
            return None
        
        # If already a number, return as is
        if isinstance(lap_time, (int, float)):
            return float(lap_time)
        
        # Format: MM:SS.sss
        if ":" in lap_time:
            parts = lap_time.split(":")
            minutes = float(parts[0])
            seconds = float(parts[1])
            return minutes * 60 + seconds
        
        # Format: SS.sss
        return float(lap_time)
    except:
        print(f"Warning: Could not convert lap time '{lap_time}' to seconds")
        return None

def load_tire_data(meeting_key, session_key):
    """
    Load tire data to correlate with lap times.
    
    Args:
        meeting_key: Meeting key
        session_key: Session key
        
    Returns:
        pd.DataFrame: DataFrame containing tire data or None if not available
    """
    # Check several possible sources of tire data
    possible_files = [
        f"f1_data/processed/{meeting_key}/{session_key}/CurrentTyres/tyre_history.csv",
        f"f1_data/processed/{meeting_key}/{session_key}/TimingAppData/tire_stints.csv",
        f"f1_data/processed/{meeting_key}/{session_key}/StintAnalysis/stint_laps.csv"
    ]
    
    for file_path in possible_files:
        if os.path.exists(file_path):
            try:
                print(f"Loading tire data from: {file_path}")
                df = pd.read_csv(file_path)
                
                # Convert driver numbers to strings for consistency
                if 'driver_number' in df.columns:
                    df['driver_number'] = df['driver_number'].astype(str)
                
                print(f"Tire data loaded: {len(df)} records")
                return df
            except Exception as e:
                print(f"Error loading tire data from {file_path}: {str(e)}")
    
    print("Warning: No tire data file found")
    return None

def load_driver_info(meeting_key, session_key):
    """
    Load driver information to get names and teams.
    
    Args:
        meeting_key: Meeting key
        session_key: Session key
        
    Returns:
        pd.DataFrame: DataFrame with driver information, or None if not available
    """
    driver_file = f"f1_data/processed/{meeting_key}/{session_key}/DriverList/driver_info.csv"
    
    if not os.path.exists(driver_file):
        print(f"Warning: Driver information not found: {driver_file}")
        return None
    
    try:
        print(f"Loading driver information from: {driver_file}")
        df = pd.read_csv(driver_file)
        
        # Convert driver numbers to strings for consistency
        if 'driver_number' in df.columns:
            df['driver_number'] = df['driver_number'].astype(str)
        
        print(f"Driver information loaded: {len(df)} drivers")
        return df
    except Exception as e:
        print(f"Error loading driver information: {str(e)}")
        return None

def process_lap_data(lap_df, tire_df=None, driver_info=None, selected_driver=None, 
                   compare_drivers=None, fastest_only=False):
    """
    Process lap time data for visualization and analysis.
    
    Args:
        lap_df: DataFrame with lap time data
        tire_df: DataFrame with tire data (optional)
        driver_info: DataFrame with driver information (optional)
        selected_driver: Driver number for detailed analysis (optional)
        compare_drivers: List of driver numbers to compare (optional)
        fastest_only: If True, only include fastest laps
        
    Returns:
        dict: Dictionary with processed data for visualization
    """
    if lap_df is None or lap_df.empty:
        print("Error: No lap data available for processing")
        return None
    
    # Check if we have the necessary columns
    required_columns = ['driver_number', 'lap_time']
    missing_columns = [col for col in required_columns if col not in lap_df.columns]
    
    if missing_columns:
        print(f"Error: Lap data missing essential columns: {missing_columns}")
        return None
    
    # Make sure we have lap time in seconds
    if 'lap_seconds' not in lap_df.columns:
        lap_df['lap_seconds'] = lap_df['lap_time'].apply(convert_lap_time_to_seconds)
    
    # Add lap number if not present
    if 'lap_number' not in lap_df.columns:
        # Group by driver and create sequential lap numbers
        lap_df['lap_number'] = lap_df.groupby('driver_number').cumcount() + 1
    
    # Get unique driver numbers
    all_drivers = sorted(lap_df['driver_number'].unique())
    print(f"Drivers in lap data: {', '.join(all_drivers)}")
    
    # Filter to selected driver if specified
    drivers_to_analyze = []
    if selected_driver:
        selected_driver = str(selected_driver)
        if selected_driver in all_drivers:
            drivers_to_analyze.append(selected_driver)
            print(f"Selected driver for detailed analysis: {selected_driver}")
        else:
            print(f"Warning: Selected driver {selected_driver} not found in lap data")
    
    # Add compare drivers if specified
    if compare_drivers:
        compare_drivers = [str(d) for d in compare_drivers]
        # Add any new drivers not already in the list
        for driver in compare_drivers:
            if driver in all_drivers and driver not in drivers_to_analyze:
                drivers_to_analyze.append(driver)
        
        print(f"Drivers for comparison: {', '.join(drivers_to_analyze)}")
    
    # If no specific drivers selected, use all
    if not drivers_to_analyze:
        drivers_to_analyze = all_drivers
    
    # Create a dictionary to store processed data
    processed_data = {
        'drivers': {},
        'fastest_lap': {
            'time': float('inf'),
            'driver': None,
            'lap': None
        },
        'slowest_lap': {
            'time': 0,
            'driver': None,
            'lap': None
        }
    }
    
    # Process each driver's lap data
    for driver in drivers_to_analyze:
        driver_name = f"Driver #{driver}"
        team_name = None
        team_color = 'gray'
        
        # Get driver name and team if available
        if driver_info is not None:
            driver_info_row = driver_info[driver_info['driver_number'] == driver]
            if not driver_info_row.empty:
                if 'last_name' in driver_info_row.columns and not pd.isna(driver_info_row['last_name'].iloc[0]):
                    driver_name = driver_info_row['last_name'].iloc[0]
                elif 'full_name' in driver_info_row.columns and not pd.isna(driver_info_row['full_name'].iloc[0]):
                    driver_name = driver_info_row['full_name'].iloc[0]
                elif 'tla' in driver_info_row.columns and not pd.isna(driver_info_row['tla'].iloc[0]):
                    driver_name = driver_info_row['tla'].iloc[0]
                
                if 'team_name' in driver_info_row.columns and not pd.isna(driver_info_row['team_name'].iloc[0]):
                    team_name = driver_info_row['team_name'].iloc[0]
                    team_color = TEAM_COLORS.get(team_name, team_color)
        
        # Get driver's lap data
        driver_laps = lap_df[lap_df['driver_number'] == driver].copy()
        
        # Skip if no lap data for this driver
        if driver_laps.empty:
            continue
        
        # Sort by lap number
        driver_laps = driver_laps.sort_values('lap_number')
        
        # Filter to fastest laps only if requested
        if fastest_only:
            # Find the fastest lap
            fastest_idx = driver_laps['lap_seconds'].idxmin()
            driver_laps = driver_laps.loc[[fastest_idx]]
        
        # Get lap numbers and times
        lap_numbers = driver_laps['lap_number'].tolist()
        lap_times = driver_laps['lap_seconds'].tolist()
        
        # Find the driver's fastest lap
        fastest_idx = driver_laps['lap_seconds'].idxmin() if not driver_laps.empty else None
        fastest_lap = None
        fastest_time = None
        
        if fastest_idx is not None:
            fastest_lap = driver_laps.loc[fastest_idx, 'lap_number']
            fastest_time = driver_laps.loc[fastest_idx, 'lap_seconds']
            
            # Update overall fastest lap if this is faster
            if fastest_time < processed_data['fastest_lap']['time']:
                processed_data['fastest_lap'] = {
                    'time': fastest_time,
                    'driver': driver,
                    'driver_name': driver_name,
                    'lap': fastest_lap
                }
        
        # Find the driver's slowest lap (excluding outliers)
        valid_times = driver_laps[driver_laps['lap_seconds'] > 0]['lap_seconds']
        if not valid_times.empty:
            # Use zscore to identify outliers
            z_scores = zscore(valid_times) if len(valid_times) > 2 else np.zeros(len(valid_times))
            valid_times_no_outliers = valid_times[np.abs(z_scores) < 3]
            
            if not valid_times_no_outliers.empty:
                slowest_idx = valid_times_no_outliers.idxmax()
                slowest_lap = driver_laps.loc[slowest_idx, 'lap_number']
                slowest_time = driver_laps.loc[slowest_idx, 'lap_seconds']
                
                # Update overall slowest lap if this is slower
                if slowest_time > processed_data['slowest_lap']['time']:
                    processed_data['slowest_lap'] = {
                        'time': slowest_time,
                        'driver': driver,
                        'driver_name': driver_name,
                        'lap': slowest_lap
                    }
        
        # Calculate average lap time (excluding outliers)
        valid_times = driver_laps[driver_laps['lap_seconds'] > 0]['lap_seconds']
        if not valid_times.empty:
            # Use zscore to identify outliers
            if len(valid_times) > 2:
                z_scores = zscore(valid_times)
                valid_times_no_outliers = valid_times[np.abs(z_scores) < 3]
                avg_time = valid_times_no_outliers.mean() if not valid_times_no_outliers.empty else valid_times.mean()
            else:
                avg_time = valid_times.mean()
        else:
            avg_time = None
        
        # Get tire data for this driver if available
        tire_stints = []
        if tire_df is not None:
            if 'driver_number' in tire_df.columns and 'compound' in tire_df.columns:
                driver_tires = tire_df[tire_df['driver_number'] == driver]
                
                if not driver_tires.empty:
                    # Process tire data based on available columns
                    if 'stint_number' in driver_tires.columns:
                        # Format with stint number and compound
                        for _, stint in driver_tires.iterrows():
                            if 'lap_start' in stint and 'lap_end' in stint:
                                tire_stints.append({
                                    'stint': int(stint['stint_number']),
                                    'compound': stint['compound'],
                                    'start_lap': int(stint['lap_start']),
                                    'end_lap': int(stint['lap_end'])
                                })
                            elif 'start_laps' in stint and 'total_laps' in stint:
                                start_lap = int(stint['start_laps'])
                                total_laps = int(stint['total_laps'])
                                tire_stints.append({
                                    'stint': int(stint['stint_number']),
                                    'compound': stint['compound'],
                                    'start_lap': start_lap,
                                    'end_lap': start_lap + total_laps - 1
                                })
                    elif 'timestamp' in driver_tires.columns:
                        # Format with timestamps
                        # Sort by timestamp
                        driver_tires = driver_tires.sort_values('timestamp')
                        
                        # Each row is a compound change
                        current_stint = 1
                        for i, (_, stint) in enumerate(driver_tires.iterrows()):
                            # Get corresponding lap number
                            timestamp = stint['timestamp']
                            lap_row = driver_laps[driver_laps['timestamp'] >= timestamp].iloc[0] if not driver_laps.empty else None
                            
                            if lap_row is not None:
                                start_lap = int(lap_row['lap_number'])
                                
                                # End lap is either next stint start-1 or last lap
                                if i < len(driver_tires) - 1:
                                    next_timestamp = driver_tires.iloc[i+1]['timestamp']
                                    next_lap_row = driver_laps[driver_laps['timestamp'] >= next_timestamp].iloc[0] if not driver_laps.empty else None
                                    end_lap = int(next_lap_row['lap_number']) - 1 if next_lap_row is not None else max(lap_numbers)
                                else:
                                    end_lap = max(lap_numbers)
                                
                                tire_stints.append({
                                    'stint': current_stint,
                                    'compound': stint['compound'],
                                    'start_lap': start_lap,
                                    'end_lap': end_lap
                                })
                                
                                current_stint += 1
        
        # Store driver data
        processed_data['drivers'][driver] = {
            'name': driver_name,
            'team': team_name,
            'color': team_color,
            'lap_numbers': lap_numbers,
            'lap_times': lap_times,
            'fastest_lap': fastest_lap,
            'fastest_time': fastest_time,
            'average_time': avg_time,
            'tire_stints': tire_stints
        }
    
    return processed_data

def format_time(seconds):
    """
    Format time in seconds to MM:SS.sss format.
    
    Args:
        seconds: Time in seconds
        
    Returns:
        str: Formatted time string
    """
    if seconds is None:
        return "N/A"
    
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    return f"{minutes}:{remaining_seconds:06.3f}"

def create_lap_time_evolution(lap_data, race_name, session_name, output_path, dark_mode=False):
    """
    Create visualization of lap time evolution throughout the session.
    
    Args:
        lap_data: Dictionary with processed lap data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        dark_mode: If True, use dark theme for visualization
    """
    if not lap_data or not lap_data['drivers']:
        print("Warning: Insufficient data for lap time evolution visualization")
        return
    
    # Configure dark mode if requested
    if dark_mode:
        plt.style.use('dark_background')
        text_color = 'white'
        grid_color = 'gray'
        bg_color = '#333333'
    else:
        plt.style.use('default')
        text_color = 'black'
        grid_color = 'lightgray'
        bg_color = 'white'
    
    # Create figure
    fig, ax = plt.subplots(figsize=FIG_SIZE, dpi=100)
    
    # Plot each driver's lap times
    for driver, data in lap_data['drivers'].items():
        lap_numbers = data['lap_numbers']
        lap_times = data['lap_times']
        
        # Skip if no valid data
        if not lap_numbers or not lap_times:
            continue
        
        # Create scatter plot of lap times
        plt.plot(lap_numbers, lap_times, 'o-', color=data['color'], linewidth=1.5, 
                label=data['name'], alpha=0.8, markersize=4)
        
        # Mark fastest lap
        if data['fastest_lap'] is not None:
            fastest_idx = lap_numbers.index(data['fastest_lap'])
            plt.scatter([lap_numbers[fastest_idx]], [lap_times[fastest_idx]], 
                       color=data['color'], s=100, edgecolor='black', zorder=10)
        
        # Add tire stints if available
        if data['tire_stints']:
            for stint in data['tire_stints']:
                # Find the x range for this stint
                x_start = stint['start_lap']
                x_end = stint['end_lap']
                
                # Get the y range (lap times) for this stint
                y_values = [t for n, t in zip(lap_numbers, lap_times) if x_start <= n <= x_end]
                
                if y_values:
                    y_min = min(y_values)
                    y_max = max(y_values)
                    
                    # Plot a colored rectangle for this stint
                    compound = stint['compound']
                    color = COMPOUND_COLORS.get(compound, COMPOUND_COLORS['UNKNOWN'])
                    
                    # Plot vertical lines at stint boundaries
                    plt.axvline(x=x_start, color=color, linestyle='--', alpha=0.5)
                    
                    # Add compound label
                    plt.text(x_start + (x_end - x_start)/2, min(y_values) - 0.5, 
                            compound[0] if len(compound) > 0 else compound, 
                            ha='center', va='top', color=color, fontweight='bold',
                            bbox=dict(facecolor='white' if dark_mode else 'black', alpha=0.2, boxstyle='round,pad=0.2'))
    
    # Add horizontal line for fastest lap
    if lap_data['fastest_lap']['time'] < float('inf'):
        plt.axhline(y=lap_data['fastest_lap']['time'], color='purple', linestyle='--', alpha=0.5)
        plt.text(ax.get_xlim()[1], lap_data['fastest_lap']['time'], 
                f"Fastest: {format_time(lap_data['fastest_lap']['time'])} ({lap_data['fastest_lap']['driver_name']})", 
                ha='right', va='bottom', color='purple', fontsize=9)
    
    # Configure title and labels
    plt.title(f"Lap Time Evolution - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel('Lap Number', fontsize=12, color=text_color)
    plt.ylabel('Lap Time (seconds)', fontsize=12, color=text_color)
    
    # Add legend
    plt.legend(loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Add grid for easier reading
    plt.grid(True, alpha=0.3, color=grid_color)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Lap time evolution visualization saved to: {output_path}")
    plt.close()

def create_lap_time_distribution(lap_data, race_name, session_name, output_path, dark_mode=False):
    """
    Create visualization of lap time distribution (box plot).
    
    Args:
        lap_data: Dictionary with processed lap data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        dark_mode: If True, use dark theme for visualization
    """
    if not lap_data or not lap_data['drivers']:
        print("Warning: Insufficient data for lap time distribution visualization")
        return
    
    # Configure dark mode if requested
    if dark_mode:
        plt.style.use('dark_background')
        text_color = 'white'
        grid_color = 'gray'
        bg_color = '#333333'
    else:
        plt.style.use('default')
        text_color = 'black'
        grid_color = 'lightgray'
        bg_color = 'white'
    
    # Create figure
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Prepare data for box plot
    driver_names = []
    lap_times_list = []
    colors = []
    
    for driver, data in lap_data['drivers'].items():
        # Skip if no valid data
        if not data['lap_times']:
            continue
        
        driver_names.append(data['name'])
        lap_times_list.append(data['lap_times'])
        colors.append(data['color'])
    
    # Create box plot
    box = plt.boxplot(lap_times_list, labels=driver_names, patch_artist=True, 
                     vert=True, widths=0.5)
    
    # Color the boxes with driver colors
    for patch, color in zip(box['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    # Add scatter points for individual lap times
    for i, (driver, data) in enumerate(lap_data['drivers'].items()):
        # Skip if no valid data
        if not data['lap_times']:
            continue
        
        # Add jittered points for each lap time
        x = np.random.normal(i+1, 0.04, size=len(data['lap_times']))
        plt.scatter(x, data['lap_times'], color=data['color'], alpha=0.5, s=20)
        
        # Add marker for fastest lap
        if data['fastest_time'] is not None:
            plt.scatter(i+1, data['fastest_time'], color=data['color'], s=100, 
                       edgecolor='black', zorder=10, marker='*')
    
    # Configure title and labels
    plt.title(f"Lap Time Distribution - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.ylabel('Lap Time (seconds)', fontsize=12, color=text_color)
    
    # Rotate driver names for better readability
    plt.xticks(rotation=45, ha='right')
    
    # Add grid for easier reading
    plt.grid(axis='y', alpha=0.3, color=grid_color)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Lap time distribution visualization saved to: {output_path}")
    plt.close()

def create_stint_analysis(lap_data, race_name, session_name, output_path, selected_driver=None, dark_mode=False):
    """
    Create visualization analyzing lap time trends within tire stints.
    
    Args:
        lap_data: Dictionary with processed lap data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        selected_driver: Specific driver to analyze (required)
        dark_mode: If True, use dark theme for visualization
    """
    if not lap_data or not lap_data['drivers']:
        print("Warning: Insufficient data for stint analysis visualization")
        return
    
    # This visualization requires a specific driver
    if selected_driver is None:
        # Try to use the first driver with stint data
        for driver, data in lap_data['drivers'].items():
            if data['tire_stints']:
                selected_driver = driver
                break
    
    if selected_driver not in lap_data['drivers']:
        print(f"Warning: Selected driver {selected_driver} not found or has no tire data")
        return
    
    # Get driver data
    driver_data = lap_data['drivers'][selected_driver]
    
    # Skip if no stint data
    if not driver_data['tire_stints']:
        print(f"Warning: No tire stint data available for driver {selected_driver}")
        return
    
    # Configure dark mode if requested
    if dark_mode:
        plt.style.use('dark_background')
        text_color = 'white'
        grid_color = 'gray'
        bg_color = '#333333'
    else:
        plt.style.use('default')
        text_color = 'black'
        grid_color = 'lightgray'
        bg_color = 'white'
    
    # Create figure
    fig, ax = plt.subplots(figsize=FIG_SIZE, dpi=100)
    
    # Get lap data
    lap_numbers = driver_data['lap_numbers']
    lap_times = driver_data['lap_times']
    
    # Create scatter plot of all lap times
    plt.scatter(lap_numbers, lap_times, color='gray', alpha=0.3, s=20)
    
    # Process each stint
    legend_elements = []
    for stint in driver_data['tire_stints']:
        # Get stint details
        stint_num = stint['stint']
        compound = stint['compound']
        start_lap = stint['start_lap']
        end_lap = stint['end_lap']
        
        # Get lap times for this stint
        stint_laps = []
        stint_times = []
        for n, t in zip(lap_numbers, lap_times):
            if start_lap <= n <= end_lap:
                stint_laps.append(n)
                stint_times.append(t)
        
        if not stint_laps:
            continue
        
        # Get compound color
        color = COMPOUND_COLORS.get(compound, COMPOUND_COLORS['UNKNOWN'])
        
        # Create normalized lap numbers (laps into stint)
        stint_lap_offset = [n - start_lap + 1 for n in stint_laps]
        
        # Plot lap times for this stint
        plt.plot(stint_laps, stint_times, 'o-', color=color, linewidth=2, 
                label=f"Stint {stint_num}: {compound}", alpha=0.8, markersize=6)
        
        # If there are enough laps, fit a trend line
        if len(stint_laps) >= 3:
            z = np.polyfit(stint_lap_offset, stint_times, 1)
            p = np.poly1d(z)
            
            # Calculate trend line values
            trend_x = np.linspace(start_lap, end_lap, 100)
            trend_offset = np.linspace(1, end_lap - start_lap + 1, 100)
            trend_y = p(trend_offset)
            
            # Plot trend line
            plt.plot(trend_x, trend_y, '--', color=color, alpha=0.6)
            
            # Add slope label (tire degradation rate)
            slope = z[0]
            slope_text = f"+{slope:.3f} sec/lap" if slope > 0 else f"{slope:.3f} sec/lap"
            plt.text(start_lap + (end_lap - start_lap)/2, min(stint_times), 
                    f"Deg: {slope_text}", ha='center', va='bottom', color=color,
                    bbox=dict(facecolor='white' if dark_mode else 'black', alpha=0.2, boxstyle='round,pad=0.2'))
        
        # Add stint legend element
        legend_elements.append(plt.Line2D([0], [0], color=color, lw=2, 
                                        label=f"Stint {stint_num}: {compound}"))
    
    # Configure title and labels
    plt.title(f"Tire Stint Analysis - {driver_data['name']} - {race_name} - {session_name}", 
             fontsize=14, color=text_color)
    plt.xlabel('Lap Number', fontsize=12, color=text_color)
    plt.ylabel('Lap Time (seconds)', fontsize=12, color=text_color)
    
    # Add legend
    plt.legend(handles=legend_elements, loc='upper right')
    
    # Add grid for easier reading
    plt.grid(True, alpha=0.3, color=grid_color)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Stint analysis visualization saved to: {output_path}")
    plt.close()

def create_fastest_lap_comparison(lap_data, race_name, session_name, output_path, dark_mode=False):
    """
    Create visualization comparing fastest laps between drivers.
    
    Args:
        lap_data: Dictionary with processed lap data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        dark_mode: If True, use dark theme for visualization
    """
    if not lap_data or not lap_data['drivers']:
        print("Warning: Insufficient data for fastest lap comparison visualization")
        return
    
    # Configure dark mode if requested
    if dark_mode:
        plt.style.use('dark_background')
        text_color = 'white'
        grid_color = 'gray'
        bg_color = '#333333'
    else:
        plt.style.use('default')
        text_color = 'black'
        grid_color = 'lightgray'
        bg_color = 'white'
    
    # Create figure
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Extract fastest lap data and sort by time
    fastest_laps = []
    for driver, data in lap_data['drivers'].items():
        if data['fastest_time'] is not None:
            fastest_laps.append({
                'driver': driver,
                'name': data['name'],
                'time': data['fastest_time'],
                'lap': data['fastest_lap'],
                'color': data['color']
            })
    
    # Sort by fastest time
    fastest_laps.sort(key=lambda x: x['time'])
    
    # Skip if no fastest laps
    if not fastest_laps:
        print("Warning: No fastest lap data available")
        return
    
    # Extract data for visualization
    driver_names = [lap['name'] for lap in fastest_laps]
    times = [lap['time'] for lap in fastest_laps]
    colors = [lap['color'] for lap in fastest_laps]
    
    # Calculate time deltas from fastest
    fastest_time = times[0]
    deltas = [time - fastest_time for time in times]
    
    # Create horizontal bar chart for time deltas
    bars = plt.barh(driver_names, deltas, color=colors, alpha=0.8, edgecolor='black')
    
    # Add time labels
    for i, bar in enumerate(bars):
        width = bar.get_width()
        if i == 0:
            # For the fastest lap, display the actual time
            label = f"{format_time(times[i])}"
        else:
            # For other laps, display the delta
            label = f"+{width:.3f}s"
        
        plt.text(width + 0.02, bar.get_y() + bar.get_height()/2,
                label, ha='left', va='center', color=text_color, fontweight='bold')
    
    # Configure title and labels
    plt.title(f"Fastest Lap Comparison - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel('Time Delta (seconds)', fontsize=12, color=text_color)
    
    # Add grid for easier reading
    plt.grid(axis='x', alpha=0.3, color=grid_color)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Fastest lap comparison visualization saved to: {output_path}")
    plt.close()

def main():
    """Main function of the script."""
    # Process command line arguments
    args = parse_args()
    
    # Extract arguments
    meeting_key = args.meeting
    session_key = args.session
    selected_driver = args.driver
    compare_drivers = args.compare
    dark_mode = args.dark_mode
    fastest_only = args.fastest_only
    include_tires = args.include_tires
    
    # Define output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Use the visualizations directory within processed data
        output_dir = Path(f"f1_data/processed/{meeting_key}/{session_key}/visualizations/laptimes")
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define names for event and session
    race_name = args.race_name or f"Meeting_{meeting_key}"
    session_name = args.session_name or f"Session_{session_key}"
    
    try:
        # Load lap time data
        lap_df = load_lap_data(meeting_key, session_key)
        
        if lap_df is None:
            print("Error: Could not load lap time data")
            return 1
        
        # Load tire data if requested
        tire_df = None
        if include_tires:
            tire_df = load_tire_data(meeting_key, session_key)
        
        # Load driver information
        driver_info = load_driver_info(meeting_key, session_key)
        
        # Process lap data
        lap_data = process_lap_data(lap_df, tire_df, driver_info, 
                                  selected_driver, compare_drivers, fastest_only)
        
        if lap_data is None:
            print("Error: Could not process lap data")
            return 1
        
        # Create lap time evolution visualization
        evolution_path = output_dir / f"lap_evolution_{meeting_key}_{session_key}.png"
        create_lap_time_evolution(lap_data, race_name, session_name, evolution_path, dark_mode)
        
        # Create lap time distribution visualization
        distribution_path = output_dir / f"lap_distribution_{meeting_key}_{session_key}.png"
        create_lap_time_distribution(lap_data, race_name, session_name, distribution_path, dark_mode)
        
        # Create stint analysis visualization (if tire data available and specific driver selected)
        if tire_df is not None and selected_driver:
            stint_path = output_dir / f"stint_analysis_{selected_driver}_{meeting_key}_{session_key}.png"
            create_stint_analysis(lap_data, race_name, session_name, stint_path, selected_driver, dark_mode)
        
        # Create fastest lap comparison
        fastest_path = output_dir / f"fastest_laps_{meeting_key}_{session_key}.png"
        create_fastest_lap_comparison(lap_data, race_name, session_name, fastest_path, dark_mode)
        
        print("All lap time visualizations were generated successfully!")
        print(f"Visualizations are available in: {output_dir}")
        
    except Exception as e:
        print(f"Error creating visualizations: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())