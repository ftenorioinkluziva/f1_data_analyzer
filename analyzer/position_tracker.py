#!/usr/bin/env python3
"""
position_tracker.py - Script for visualizing position changes in F1 races

This script reads processed data from TimingData and/or DriverList from the F1 Data Analyzer
and generates visualizations of driver position changes during a race.

Usage:
    python position_tracker.py --meeting 1264 --session 1297 --output-dir visualizations
"""

import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
from pathlib import Path
from matplotlib.colors import LinearSegmentedColormap
from datetime import datetime, timedelta

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

def parse_args():
    """Process command line arguments."""
    parser = argparse.ArgumentParser(description='Visualize F1 race position changes')
    
    parser.add_argument('--meeting', type=str, required=True,
                        help='Meeting key (e.g., 1264 for Miami GP)')
    
    parser.add_argument('--session', type=str, required=True,
                        help='Session key (e.g., 1297 for the main race)')
    
    parser.add_argument('--drivers', type=str, nargs='+', default=None,
                        help='Specific driver numbers to visualize (e.g., 1 16 44 to show only these drivers)')
    
    parser.add_argument('--top', type=int, default=None,
                        help='Number of top finishing drivers to show (e.g., 10 for top 10)')
    
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Directory to save visualizations')
    
    parser.add_argument('--race-name', type=str, default=None,
                        help='Custom name for the event')
    
    parser.add_argument('--session-name', type=str, default=None,
                        help='Custom name for the session')
    
    parser.add_argument('--dark-mode', action='store_true', default=False,
                        help='Use dark theme for visualizations')
    
    parser.add_argument('--by-lap', action='store_true', default=False,
                        help='Visualize position changes by lap instead of by time')
    
    return parser.parse_args()

def load_position_data(meeting_key, session_key):
    """
    Load position/timing data from the session.
    
    Args:
        meeting_key: Meeting key
        session_key: Session key
        
    Returns:
        pd.DataFrame: DataFrame containing position data or None if not available
    """
    # Check several possible sources of position data
    possible_files = [
        f"f1_data/processed/{meeting_key}/{session_key}/TimingData/positions.csv",
        f"f1_data/processed/{meeting_key}/{session_key}/DriverList/position_updates.csv",
        f"f1_data/processed/{meeting_key}/{session_key}/TimingAppData/grid_positions.csv"
    ]
    
    for file_path in possible_files:
        if os.path.exists(file_path):
            try:
                print(f"Loading position data from: {file_path}")
                df = pd.read_csv(file_path)
                
                # Convert driver numbers to strings for consistency
                if 'driver_number' in df.columns:
                    df['driver_number'] = df['driver_number'].astype(str)
                
                # Sort by timestamp if available
                if 'timestamp' in df.columns:
                    df = df.sort_values('timestamp')
                
                print(f"Position data loaded: {len(df)} records")
                return df
            except Exception as e:
                print(f"Error loading position data from {file_path}: {str(e)}")
    
    print("Warning: No position data file found")
    return None

def load_lap_data(meeting_key, session_key):
    """
    Load lap time data to correlate with position changes.
    
    Args:
        meeting_key: Meeting key
        session_key: Session key
        
    Returns:
        pd.DataFrame: DataFrame containing lap data or None if not available
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
        return df
    except Exception as e:
        print(f"Error loading lap time data: {str(e)}")
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

def process_position_data(position_df, lap_df=None, driver_info=None, selected_drivers=None, top_n=None, by_lap=False):
    """
    Process position data for visualization.
    
    Args:
        position_df: DataFrame with position data
        lap_df: DataFrame with lap time data
        driver_info: DataFrame with driver information
        selected_drivers: List of specific driver numbers to show
        top_n: Number of top finishing drivers to show
        by_lap: If True, process data by lap instead of by time
        
    Returns:
        dict: Dictionary with processed data ready for visualization
    """
    if position_df is None or position_df.empty:
        print("Error: No position data available for processing")
        return None
    
    # Check if we have the necessary columns
    required_columns = ['driver_number', 'position']
    column_mapping = {
        'position': ['position', 'Position', 'grid_position'],
        'driver_number': ['driver_number', 'DriverNumber', 'racing_number']
    }
    
    # Try to map alternative column names
    for req_col, alternatives in column_mapping.items():
        if req_col not in position_df.columns:
            for alt_col in alternatives:
                if alt_col in position_df.columns:
                    position_df[req_col] = position_df[alt_col]
                    print(f"Using column '{alt_col}' as '{req_col}'")
                    break
    
    # Check again if we have the necessary columns
    missing_columns = [col for col in required_columns if col not in position_df.columns]
    if missing_columns:
        print(f"Error: Position data missing essential columns: {missing_columns}")
        return None
    
    # Get unique driver numbers
    all_drivers = sorted(position_df['driver_number'].unique())
    print(f"Drivers in position data: {', '.join(all_drivers)}")
    
    # Filter to selected drivers if specified
    drivers_to_display = all_drivers
    if selected_drivers:
        selected_drivers = [str(d) for d in selected_drivers]
        drivers_to_display = [d for d in selected_drivers if d in all_drivers]
        print(f"Selected drivers to display: {', '.join(drivers_to_display)}")
    
    # Filter to top N finishing drivers if specified
    if top_n is not None:
        try:
            # Get the latest position for each driver to determine finishing order
            latest_positions = {}
            
            # Get the last recorded position for each driver
            for driver in all_drivers:
                driver_data = position_df[position_df['driver_number'] == driver]
                if not driver_data.empty:
                    latest_positions[driver] = driver_data['position'].iloc[-1]
            
            # Sort drivers by their latest position
            sorted_drivers = sorted(latest_positions.items(), key=lambda x: int(x[1]) if str(x[1]).isdigit() else 999)
            drivers_to_display = [d[0] for d in sorted_drivers[:top_n]]
            print(f"Top {top_n} drivers by finishing position: {', '.join(drivers_to_display)}")
        except Exception as e:
            print(f"Error determining top {top_n} drivers: {str(e)}")
    
    # Create a dictionary to store processed data
    processed_data = {
        'drivers': {},
        'timestamps': [],
        'laps': []
    }
    
    # Process data by lap if requested and lap data is available
    if by_lap and lap_df is not None:
        # Get unique laps
        all_laps = sorted(lap_df['lap_number'].unique()) if 'lap_number' in lap_df.columns else []
        processed_data['laps'] = all_laps
        
        # For each driver, get position at each lap
        for driver in drivers_to_display:
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
            
            # Initialize positions array for this driver
            positions = []
            
            # For each lap, find the driver's position
            for lap in all_laps:
                # Find the driver's position at this lap
                lap_data = lap_df[(lap_df['driver_number'] == driver) & (lap_df['lap_number'] == lap)]
                
                if not lap_data.empty:
                    # Get the timestamp of this lap
                    lap_timestamp = lap_data['timestamp'].iloc[0]
                    
                    # Find the closest position data to this timestamp
                    pos_data = position_df[position_df['driver_number'] == driver]
                    if not pos_data.empty:
                        # Find the position at or before this timestamp
                        if 'timestamp' in pos_data.columns:
                            pos_data = pos_data[pos_data['timestamp'] <= lap_timestamp]
                        
                        if not pos_data.empty:
                            positions.append(int(pos_data['position'].iloc[-1]))
                        else:
                            positions.append(None)
                    else:
                        positions.append(None)
                else:
                    positions.append(None)
            
            # Store driver's data
            processed_data['drivers'][driver] = {
                'name': driver_name,
                'team': team_name,
                'color': team_color,
                'positions': positions
            }
    
    # Process data by timestamp (default)
    else:
        # Get unique timestamps
        if 'timestamp' in position_df.columns:
            all_timestamps = sorted(position_df['timestamp'].unique())
            processed_data['timestamps'] = all_timestamps
        else:
            # If no timestamps, use sequence numbers
            max_records = max(len(position_df[position_df['driver_number'] == d]) for d in drivers_to_display)
            all_timestamps = list(range(max_records))
            processed_data['timestamps'] = all_timestamps
        
        # For each driver, get position at each timestamp
        for driver in drivers_to_display:
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
            
            # Get this driver's position data
            driver_positions = position_df[position_df['driver_number'] == driver]
            
            if 'timestamp' in position_df.columns:
                # Initialize positions array with None (no data)
                positions = [None] * len(all_timestamps)
                
                # Fill in positions where we have data
                for i, ts in enumerate(all_timestamps):
                    # Find position data at or before this timestamp
                    ts_data = driver_positions[driver_positions['timestamp'] <= ts]
                    if not ts_data.empty:
                        positions[i] = int(ts_data['position'].iloc[-1])
            else:
                # If no timestamps, just use the positions in order
                positions = driver_positions['position'].tolist()
                # Pad with None if needed
                positions.extend([None] * (len(all_timestamps) - len(positions)))
            
            # Store driver's data
            processed_data['drivers'][driver] = {
                'name': driver_name,
                'team': team_name,
                'color': team_color,
                'positions': positions
            }
    
    # Calculate position changes (start vs. end)
    for driver, data in processed_data['drivers'].items():
        positions = data['positions']
        # Filter out None values
        valid_positions = [p for p in positions if p is not None]
        if valid_positions:
            start_pos = valid_positions[0]
            end_pos = valid_positions[-1]
            data['position_change'] = start_pos - end_pos  # Positive = gained positions, negative = lost positions
        else:
            data['position_change'] = 0
    
    return processed_data

def create_position_timeline(position_data, race_name, session_name, output_path, dark_mode=False, by_lap=False):
    """
    Create timeline visualization of position changes.
    
    Args:
        position_data: Dictionary with processed position data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        dark_mode: If True, use dark theme for visualization
        by_lap: If True, x-axis is lap number instead of time
    """
    if not position_data or not position_data['drivers']:
        print("Warning: Insufficient data for position timeline visualization")
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
    
    # X-axis: laps or timestamps
    x_values = position_data['laps'] if by_lap else range(len(position_data['timestamps']))
    x_label = 'Lap' if by_lap else 'Time (sequence)'
    
    # Plot each driver's position
    for driver, data in position_data['drivers'].items():
        positions = data['positions']
        
        # Filter out None values and create corresponding x values
        valid_indices = []
        valid_positions = []
        for i, pos in enumerate(positions):
            if pos is not None:
                valid_indices.append(x_values[i] if i < len(x_values) else i)
                valid_positions.append(pos)
        
        if valid_positions:
            plt.plot(valid_indices, valid_positions, 'o-', color=data['color'], linewidth=2, 
                    label=data['name'], markersize=4)
    
    # Invert y-axis so position 1 is at the top
    plt.gca().invert_yaxis()
    
    # Set y-axis limits with some padding
    max_position = max([max([p for p in data['positions'] if p is not None], default=20) 
                       for data in position_data['drivers'].values()])
    plt.ylim(max_position + 1, 0.5)
    
    # Set y-axis ticks to integers
    plt.yticks(range(1, max_position + 1))
    
    # Configure title and labels
    plt.title(f"Position Changes - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel(x_label, fontsize=12, color=text_color)
    plt.ylabel('Position', fontsize=12, color=text_color)
    
    # Add legend
    plt.legend(loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Add grid for easier reading
    plt.grid(True, alpha=0.3, color=grid_color)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Position timeline visualization saved to: {output_path}")
    plt.close()

def create_position_heatmap(position_data, race_name, session_name, output_path, dark_mode=False, by_lap=False):
    """
    Create heatmap visualization of position changes.
    
    Args:
        position_data: Dictionary with processed position data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        dark_mode: If True, use dark theme for visualization
        by_lap: If True, x-axis is lap number instead of time
    """
    if not position_data or not position_data['drivers']:
        print("Warning: Insufficient data for position heatmap visualization")
        return
    
    # Configure dark mode if requested
    if dark_mode:
        plt.style.use('dark_background')
        text_color = 'white'
        cmap = 'inferno'
        bg_color = '#333333'
    else:
        plt.style.use('default')
        text_color = 'black'
        cmap = 'viridis'
        bg_color = 'white'
    
    # Create figure
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # X-axis: laps or timestamps
    x_values = position_data['laps'] if by_lap else range(len(position_data['timestamps']))
    x_label = 'Lap' if by_lap else 'Time (sequence)'
    
    # Create matrix for heatmap
    drivers = list(position_data['drivers'].keys())
    driver_names = [position_data['drivers'][d]['name'] for d in drivers]
    
    # Create position matrix (drivers x timestamps)
    # Initialize with NaN
    position_matrix = np.full((len(drivers), len(x_values)), np.nan)
    
    # Fill in the matrix with position data
    for i, driver in enumerate(drivers):
        data = position_data['drivers'][driver]
        for j, pos in enumerate(data['positions']):
            if j < len(x_values) and pos is not None:
                position_matrix[i, j] = pos
    
    # Create heatmap
    ax = plt.gca()
    im = ax.imshow(position_matrix, cmap=cmap, aspect='auto', interpolation='nearest')
    
    # Add colorbar
    cbar = plt.colorbar(im)
    cbar.set_label('Position', color=text_color)
    
    # Configure axes
    ax.set_yticks(range(len(drivers)))
    ax.set_yticklabels(driver_names)
    
    # Set x-ticks (skip some to avoid overcrowding)
    if len(x_values) > 10:
        step = len(x_values) // 10
        x_ticks = x_values[::step]
        ax.set_xticks(range(0, len(x_values), step))
        ax.set_xticklabels(x_ticks)
    else:
        ax.set_xticks(range(len(x_values)))
        ax.set_xticklabels(x_values)
    
    # Configure title and labels
    plt.title(f"Position Heatmap - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel(x_label, fontsize=12, color=text_color)
    plt.ylabel('Driver', fontsize=12, color=text_color)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Position heatmap visualization saved to: {output_path}")
    plt.close()

def create_position_change_chart(position_data, race_name, session_name, output_path, dark_mode=False):
    """
    Create bar chart of position gains/losses from start to finish.
    
    Args:
        position_data: Dictionary with processed position data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        dark_mode: If True, use dark theme for visualization
    """
    if not position_data or not position_data['drivers']:
        print("Warning: Insufficient data for position change visualization")
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
    
    # Extract position changes and sort drivers by it
    drivers = []
    changes = []
    colors = []
    
    sorted_drivers = sorted(
        position_data['drivers'].items(),
        key=lambda x: x[1]['position_change'],
        reverse=True  # Most gained positions first
    )
    
    for driver, data in sorted_drivers:
        drivers.append(data['name'])
        changes.append(data['position_change'])
        colors.append(data['color'])
    
    # Create horizontal bar chart
    bars = plt.barh(drivers, changes, color=colors, alpha=0.8, edgecolor='black')
    
    # Add value labels
    for bar in bars:
        width = bar.get_width()
        label = f"+{width}" if width > 0 else f"{width}"
        plt.text(width + (0.3 if width >= 0 else -0.3), 
                bar.get_y() + bar.get_height()/2,
                label, ha='left' if width >= 0 else 'right', va='center', 
                color=text_color, fontweight='bold')
    
    # Add a vertical line at x=0
    plt.axvline(x=0, color='gray', linestyle='-', alpha=0.7)
    
    # Configure title and labels
    plt.title(f"Position Gained/Lost - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel('Positions Gained (+) / Lost (-)', fontsize=12, color=text_color)
    
    # Add grid for easier reading
    plt.grid(axis='x', alpha=0.3, color=grid_color)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Position change visualization saved to: {output_path}")
    plt.close()

def main():
    """Main function of the script."""
    # Process command line arguments
    args = parse_args()
    
    # Extract arguments
    meeting_key = args.meeting
    session_key = args.session
    selected_drivers = args.drivers
    top_n = args.top
    dark_mode = args.dark_mode
    by_lap = args.by_lap
    
    # Define output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Use the visualizations directory within processed data
        output_dir = Path(f"f1_data/processed/{meeting_key}/{session_key}/visualizations/positions")
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define names for event and session
    race_name = args.race_name or f"Meeting_{meeting_key}"
    session_name = args.session_name or f"Session_{session_key}"
    
    try:
        # Load position data
        position_df = load_position_data(meeting_key, session_key)
        
        if position_df is None:
            print("Error: Could not load position data")
            return 1
        
        # Load lap data if available
        lap_df = load_lap_data(meeting_key, session_key)
        
        # Load driver information
        driver_info = load_driver_info(meeting_key, session_key)
        
        # Process position data
        position_data = process_position_data(position_df, lap_df, driver_info, 
                                             selected_drivers, top_n, by_lap)
        
        if position_data is None:
            print("Error: Could not process position data")
            return 1
        
        # Create position timeline visualization
        timeline_path = output_dir / f"position_timeline_{meeting_key}_{session_key}.png"
        create_position_timeline(position_data, race_name, session_name, timeline_path, dark_mode, by_lap)
        
        # Create position heatmap visualization
        heatmap_path = output_dir / f"position_heatmap_{meeting_key}_{session_key}.png"
        create_position_heatmap(position_data, race_name, session_name, heatmap_path, dark_mode, by_lap)
        
        # Create position change chart
        change_path = output_dir / f"position_change_{meeting_key}_{session_key}.png"
        create_position_change_chart(position_data, race_name, session_name, change_path, dark_mode)
        
        print("All position visualizations were generated successfully!")
        print(f"Visualizations are available in: {output_dir}")
        
    except Exception as e:
        print(f"Error creating visualizations: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())