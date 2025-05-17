#!/usr/bin/env python3
"""
team_radio_analyzer.py - Script for visualizing F1 team radio communications

This script reads the processed TeamRadio data from F1 Data Analyzer and analyzes
communication patterns between drivers and teams during F1 sessions.

Usage:
    python team_radio_analyzer.py --meeting 1264 --session 1297 --output-dir visualizations
"""

import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime, timedelta
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec

# Constants and settings
FIG_SIZE = (16, 10)  # Default size for plots
DPI = 300  # Resolution for saved images

def parse_args():
    """Process command line arguments."""
    parser = argparse.ArgumentParser(description='Visualize team radio communications in F1 sessions')
    
    parser.add_argument('--meeting', type=str, required=True,
                        help='Meeting key (e.g., 1264 for Miami GP)')
    
    parser.add_argument('--session', type=str, required=True,
                        help='Session key (e.g., 1297 for main race)')
    
    parser.add_argument('--driver', type=str, default=None,
                        help='Specific driver number to analyze')
    
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Directory to save visualizations')
    
    parser.add_argument('--race-name', type=str, default=None,
                        help='Custom name for the event')
    
    parser.add_argument('--session-name', type=str, default=None,
                        help='Custom name for the session')
    
    parser.add_argument('--dark-mode', action='store_true', default=False,
                        help='Use dark theme for visualizations')
    
    return parser.parse_args()

def load_team_radio_data(meeting_key, session_key):
    """
    Load team radio data for the session.
    
    Args:
        meeting_key: Meeting key
        session_key: Session key
        
    Returns:
        pd.DataFrame: DataFrame containing team radio data or None if not available
    """
    # Check for team radio data file
    radio_file = f"f1_data/processed/{meeting_key}/{session_key}/TeamRadio/team_radio_messages.csv"
    
    if not os.path.exists(radio_file):
        print(f"Warning: Team radio data not found: {radio_file}")
        return None
    
    try:
        print(f"Loading team radio data from: {radio_file}")
        df = pd.read_csv(radio_file)
        
        # If file exists but is empty
        if df.empty:
            print(f"Warning: Empty team radio file: {radio_file}")
            return None
        
        # Standardize column names (may vary depending on source)
        column_mapping = {
            'driver_number': 'driver_number',
            'racing_number': 'driver_number',
            'audio_path': 'audio_path',
            'timestamp': 'timestamp',
            'utc_time': 'utc_time'
        }
        
        # Apply mapping for existing columns
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns and new_name not in df.columns:
                df[new_name] = df[old_name]
        
        # Try to convert timestamp to datetime if it's not already
        if 'timestamp' in df.columns:
            try:
                # Check timestamp format
                sample_timestamp = df['timestamp'].iloc[0]
                
                # If string in HH:MM:SS.mmm format
                if isinstance(sample_timestamp, str) and ':' in sample_timestamp:
                    # Check if we already have a full date or just time
                    if 'T' in sample_timestamp or '-' in sample_timestamp:
                        # Seems to be a full ISO timestamp
                        df['datetime'] = pd.to_datetime(df['timestamp'])
                    else:
                        # Just time, add a dummy date for plotting
                        base_date = '2023-01-01 '  # Dummy date
                        df['datetime'] = pd.to_datetime(base_date + df['timestamp'])
            except Exception as e:
                print(f"Warning: Could not convert timestamps to datetime: {str(e)}")
        
        # Sort by timestamp
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp')
        
        # Convert driver numbers to strings for consistency
        if 'driver_number' in df.columns:
            df['driver_number'] = df['driver_number'].astype(str)
        
        print(f"Team radio data loaded: {len(df)} messages")
        return df
    except Exception as e:
        print(f"Error loading team radio data: {str(e)}")
        return None

def load_driver_info(meeting_key, session_key):
    """
    Load driver information to correlate with team radio data.
    
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

def load_race_control_data(meeting_key, session_key):
    """
    Load race control messages to correlate with team radio communications.
    
    Args:
        meeting_key: Meeting key
        session_key: Session key
        
    Returns:
        pd.DataFrame: DataFrame with race control messages, or None if not available
    """
    messages_file = f"f1_data/processed/{meeting_key}/{session_key}/RaceControlMessages/race_control_messages.csv"
    
    if not os.path.exists(messages_file):
        print(f"Warning: Race control messages not found: {messages_file}")
        return None
    
    try:
        print(f"Loading race control messages from: {messages_file}")
        df = pd.read_csv(messages_file)
        
        # Sort by timestamp
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp')
        
        # Try to convert timestamp to datetime
        if 'timestamp' in df.columns:
            try:
                # Check timestamp format
                sample_timestamp = df['timestamp'].iloc[0]
                
                # If string in HH:MM:SS.mmm format
                if isinstance(sample_timestamp, str) and ':' in sample_timestamp:
                    # Check if we already have a full date or just time
                    if 'T' in sample_timestamp or '-' in sample_timestamp:
                        # Seems to be a full ISO timestamp
                        df['datetime'] = pd.to_datetime(df['timestamp'])
                    else:
                        # Just time, add a dummy date for plotting
                        base_date = '2023-01-01 '  # Dummy date
                        df['datetime'] = pd.to_datetime(base_date + df['timestamp'])
            except Exception as e:
                print(f"Warning: Could not convert race control timestamps to datetime: {str(e)}")
        
        print(f"Race control messages loaded: {len(df)} messages")
        return df
    except Exception as e:
        print(f"Error loading race control messages: {str(e)}")
        return None

def load_pit_data(meeting_key, session_key):
    """
    Load pit stop data to correlate with team radio communications.
    
    Args:
        meeting_key: Meeting key
        session_key: Session key
        
    Returns:
        pd.DataFrame: DataFrame with pit stop data, or None if not available
    """
    # Check various possible pit stop data file locations
    possible_files = [
        f"f1_data/processed/{meeting_key}/{session_key}/PitLaneTimeCollection/pit_stops.csv",
        f"f1_data/processed/{meeting_key}/{session_key}/StintAnalysis/pit_stops.csv",
        f"f1_data/processed/{meeting_key}/{session_key}/PitLaneTimeCollection/raw_pit_stops.csv"
    ]
    
    for file_path in possible_files:
        if os.path.exists(file_path):
            try:
                print(f"Loading pit stop data from: {file_path}")
                df = pd.read_csv(file_path)
                
                # Convert driver numbers to strings for consistency
                if 'driver_number' in df.columns:
                    df['driver_number'] = df['driver_number'].astype(str)
                
                # If the file exists but is empty
                if df.empty:
                    print(f"Warning: Empty pit stop file: {file_path}")
                    continue
                
                # Sort by timestamp
                if 'timestamp' in df.columns:
                    df = df.sort_values('timestamp')
                
                print(f"Pit stop data loaded: {len(df)} records")
                return df
            except Exception as e:
                print(f"Error loading pit stop data from {file_path}: {str(e)}")
    
    print("Warning: No pit stop data file found or usable")
    return None

def process_radio_data(radio_df, driver_info=None, race_control_df=None, pit_df=None, specific_driver=None):
    """
    Process team radio data for analysis.
    
    Args:
        radio_df: DataFrame with team radio data
        driver_info: DataFrame with driver information
        race_control_df: DataFrame with race control messages
        pit_df: DataFrame with pit stop data
        specific_driver: Specific driver number to filter
        
    Returns:
        dict: Dictionary with processed data
    """
    if radio_df is None or radio_df.empty:
        print("Error: No team radio data available for processing")
        return None
    
    # Check required columns
    if 'driver_number' not in radio_df.columns:
        print("Error: Team radio data missing driver_number column")
        return None
    
    # If specified, filter for a specific driver
    if specific_driver:
        radio_df = radio_df[radio_df['driver_number'] == str(specific_driver)].copy()
        if radio_df.empty:
            print(f"Error: No team radio data found for driver #{specific_driver}")
            return None
    
    # Add driver names if driver info is available
    if driver_info is not None:
        # Create a mapping of driver_number to driver name
        driver_names = {}
        
        for _, row in driver_info.iterrows():
            driver_number = row['driver_number']
            
            # Choose the best available name field
            if 'last_name' in driver_info.columns and not pd.isna(row.get('last_name')):
                driver_names[driver_number] = row['last_name']
            elif 'full_name' in driver_info.columns and not pd.isna(row.get('full_name')):
                driver_names[driver_number] = row['full_name']
            elif 'tla' in driver_info.columns and not pd.isna(row.get('tla')):
                driver_names[driver_number] = row['tla']
            else:
                driver_names[driver_number] = f"Driver #{driver_number}"
        
        # Add a name column to the radio data
        radio_df['driver_name'] = radio_df['driver_number'].apply(
            lambda x: driver_names.get(x, f"Driver #{x}")
        )
        
        # Also add team information if available
        if 'team_name' in driver_info.columns:
            team_names = {}
            for _, row in driver_info.iterrows():
                if not pd.isna(row.get('team_name')):
                    team_names[row['driver_number']] = row['team_name']
            
            radio_df['team_name'] = radio_df['driver_number'].apply(
                lambda x: team_names.get(x, "Unknown Team")
            )
    else:
        # Add default driver names if driver info not available
        radio_df['driver_name'] = radio_df['driver_number'].apply(lambda x: f"Driver #{x}")
    
    # Calculate message counts by driver
    driver_message_counts = radio_df['driver_number'].value_counts().to_dict()
    
    # Calculate message distribution over time
    # Create time bins (e.g., 5-minute intervals)
    if 'datetime' in radio_df.columns:
        # Calculate session duration
        session_start = radio_df['datetime'].min()
        session_end = radio_df['datetime'].max()
        session_duration = (session_end - session_start).total_seconds() / 60  # in minutes
        
        # Create bins (adjust bin size based on session duration)
        if session_duration > 90:  # For races (longer sessions)
            bin_size = 5  # 5-minute bins
        else:  # For shorter sessions
            bin_size = 2  # 2-minute bins
        
        # Create time bins
        bins = pd.date_range(start=session_start, end=session_end, freq=f'{bin_size}min')
        
        # Count messages in each bin
        radio_df['time_bin'] = pd.cut(radio_df['datetime'], bins=bins)
        time_distribution = radio_df.groupby('time_bin').size()
    else:
        # If datetime not available, use sequence bins
        total_messages = len(radio_df)
        bin_count = min(20, total_messages // 5)  # Adjust number of bins
        if bin_count <= 0:
            bin_count = 1
        
        radio_df['sequence_bin'] = pd.cut(range(total_messages), bins=bin_count)
        time_distribution = radio_df.groupby('sequence_bin').size()
    
    # Correlate with race events if available
    events_correlation = {}
    
    # Add race control events
    if race_control_df is not None and 'datetime' in radio_df.columns and 'datetime' in race_control_df.columns:
        # Filter for significant events (flags, safety car, etc.)
        significant_events = []
        if 'category' in race_control_df.columns:
            significant_events = race_control_df[race_control_df['category'] == 'Flag'].copy()
        
        for _, event in significant_events.iterrows():
            event_time = event['datetime']
            event_desc = f"Flag: {event['flag']}" if 'flag' in event.columns else "Race Event"
            
            # Find radio messages near this event (within 1 minute before and 2 minutes after)
            window_start = event_time - timedelta(minutes=1)
            window_end = event_time + timedelta(minutes=2)
            
            related_messages = radio_df[
                (radio_df['datetime'] >= window_start) & 
                (radio_df['datetime'] <= window_end)
            ]
            
            if not related_messages.empty:
                events_correlation[event_time] = {
                    'event': event_desc,
                    'message_count': len(related_messages),
                    'related_messages': related_messages.to_dict('records')
                }
    
    # Add pit stop events
    if pit_df is not None and 'datetime' in radio_df.columns and 'timestamp' in pit_df.columns:
        try:
            # Try to create datetime for pit stops
            if 'datetime' not in pit_df.columns:
                # Use the same base date as radio messages
                base_date = radio_df['datetime'].dt.date.iloc[0].strftime('%Y-%m-%d ')
                pit_df['datetime'] = pd.to_datetime(base_date + pit_df['timestamp'])
            
            # Process each pit stop
            for _, pit in pit_df.iterrows():
                pit_time = pit['datetime']
                driver = pit['driver_number']
                
                # Find radio messages near this pit stop (within 1 minute before and 1 minute after)
                window_start = pit_time - timedelta(minutes=1)
                window_end = pit_time + timedelta(minutes=1)
                
                related_messages = radio_df[
                    (radio_df['datetime'] >= window_start) & 
                    (radio_df['datetime'] <= window_end) &
                    (radio_df['driver_number'] == driver)
                ]
                
                if not related_messages.empty:
                    pit_desc = f"Pit Stop: Driver #{driver}"
                    events_correlation[pit_time] = {
                        'event': pit_desc,
                        'message_count': len(related_messages),
                        'related_messages': related_messages.to_dict('records')
                    }
        except Exception as e:
            print(f"Warning: Could not correlate with pit stops: {str(e)}")
    
    # Return processed data
    result = {
        'radio_messages': radio_df,
        'driver_message_counts': driver_message_counts,
        'time_distribution': time_distribution,
        'events_correlation': events_correlation
    }
    
    return result

def create_message_frequency_chart(processed_data, race_name, session_name, output_path, dark_mode=False):
    """
    Create visualization of team radio message frequency by driver.
    
    Args:
        processed_data: Dictionary with processed team radio data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        dark_mode: If True, use dark theme for visualization
    """
    if not processed_data or 'driver_message_counts' not in processed_data:
        print("Warning: Insufficient data for message frequency visualization")
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
    
    # Extract message counts
    message_counts = processed_data['driver_message_counts']
    radio_df = processed_data['radio_messages']
    
    # Sort drivers by message count (descending)
    sorted_drivers = sorted(message_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Extract driver names if available
    if 'driver_name' in radio_df.columns:
        driver_names = {}
        for driver, group in radio_df.groupby('driver_number'):
            driver_names[driver] = group['driver_name'].iloc[0]
    else:
        driver_names = {d: f"Driver #{d}" for d, _ in sorted_drivers}
    
    # Extract team names if available for coloring
    if 'team_name' in radio_df.columns:
        team_names = {}
        for driver, group in radio_df.groupby('driver_number'):
            team_names[driver] = group['team_name'].iloc[0]
        
        # Define colors for teams
        team_colors = {
            'Mercedes': '#00D2BE',
            'Red Bull': '#0600EF',
            'Ferrari': '#DC0000',
            'Alpine': '#0090FF',
            'McLaren': '#FF8700',
            'Alfa Romeo': '#900000',
            'Aston Martin': '#006F62',
            'Haas': '#FFFFFF',
            'AlphaTauri': '#2B4562',
            'Williams': '#005AFF',
            'Unknown Team': '#777777'
        }
        
        # Assign colors to drivers
        colors = [team_colors.get(team_names.get(d, 'Unknown Team'), '#777777') for d, _ in sorted_drivers]
        
        # Handle white color for dark mode
        if dark_mode and 'Haas' in team_names.values():
            colors = ['#CCCCCC' if c == '#FFFFFF' else c for c in colors]
    else:
        # Default color scheme
        colors = plt.cm.tab10.colors[:len(sorted_drivers)]
    
    # Create figure
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Prepare data for the bar chart
    drivers = [driver_names.get(d, f"Driver #{d}") for d, _ in sorted_drivers]
    counts = [c for _, c in sorted_drivers]
    
    # Create bar chart
    bars = plt.bar(drivers, counts, color=colors)
    
    # Add count labels
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                 str(int(height)), ha='center', va='bottom', fontsize=10, color=text_color)
    
    # Configure title and labels
    plt.title(f"Team Radio Messages by Driver - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel("Driver", fontsize=12, color=text_color)
    plt.ylabel("Number of Messages", fontsize=12, color=text_color)
    
    # Rotate x-axis labels if there are many drivers
    if len(drivers) > 5:
        plt.xticks(rotation=45, ha='right')
    
    # Add grid for easier reading
    plt.grid(axis='y', linestyle='--', alpha=0.3, color=grid_color)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Message frequency visualization saved to: {output_path}")
    plt.close()

def create_message_timeline(processed_data, race_name, session_name, output_path, dark_mode=False):
    """
    Create visualization of team radio message distribution over time.
    
    Args:
        processed_data: Dictionary with processed team radio data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        dark_mode: If True, use dark theme for visualization
    """
    if not processed_data or 'time_distribution' not in processed_data:
        print("Warning: Insufficient data for message timeline visualization")
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
    
    # Extract time distribution
    time_distribution = processed_data['time_distribution']
    radio_df = processed_data['radio_messages']
    
    # Create figure
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Plot the time distribution
    if 'time_bin' in radio_df.columns:
        # Using datetime bins
        # Extract bin midpoints for x-axis
        bin_centers = []
        for interval in time_distribution.index:
            bin_centers.append(interval.mid)
        
        # Create the bar chart
        plt.bar(bin_centers, time_distribution.values, width=0.8*(bin_centers[1]-bin_centers[0]) if len(bin_centers) > 1 else 0.8, 
                color='skyblue', alpha=0.7)
        
        # Format x-axis
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        plt.gcf().autofmt_xdate()
        x_label = "Time"
    else:
        # Using sequence bins
        # Extract bin midpoints for x-axis
        bin_centers = []
        bin_labels = []
        for i, interval in enumerate(time_distribution.index):
            mid = (interval.left + interval.right) / 2
            bin_centers.append(mid)
            bin_labels.append(f"{i+1}")
        
        # Create the bar chart
        plt.bar(bin_centers, time_distribution.values, width=0.8*(bin_centers[1]-bin_centers[0]) if len(bin_centers) > 1 else 0.8, 
                color='skyblue', alpha=0.7)
        
        # Format x-axis
        plt.xticks(bin_centers, bin_labels)
        x_label = "Session Progress"
    
    # Add labels
    for i, v in enumerate(time_distribution.values):
        if v > 0:  # Only add labels for non-zero values
            plt.text(bin_centers[i], v + 0.1, str(int(v)), ha='center', va='bottom', fontsize=9, color=text_color)
    
    # Configure title and labels
    plt.title(f"Team Radio Messages Over Time - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel(x_label, fontsize=12, color=text_color)
    plt.ylabel("Number of Messages", fontsize=12, color=text_color)
    
    # Add grid for easier reading
    plt.grid(axis='y', linestyle='--', alpha=0.3, color=grid_color)
    
    # Add race events if available
    if 'events_correlation' in processed_data and processed_data['events_correlation'] and 'datetime' in radio_df.columns:
        events = processed_data['events_correlation']
        
        # Plot significant events as vertical lines
        for event_time, event_data in events.items():
            plt.axvline(x=event_time, color='red', linestyle='--', alpha=0.7)
            
            # Try to add event label (might be crowded)
            try:
                y_pos = plt.ylim()[1] * 0.9
                plt.text(event_time, y_pos, event_data['event'], 
                         rotation=90, ha='right', va='top', fontsize=8, color='red',
                         bbox=dict(boxstyle="round,pad=0.3", fc='white' if not dark_mode else '#555555', alpha=0.7))
            except:
                pass  # Skip label if it causes an error
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Message timeline visualization saved to: {output_path}")
    plt.close()

def create_driver_activity_heatmap(processed_data, race_name, session_name, output_path, dark_mode=False):
    """
    Create a heatmap of radio activity by driver over time.
    
    Args:
        processed_data: Dictionary with processed team radio data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        dark_mode: If True, use dark theme for visualization
    """
    if not processed_data or 'radio_messages' not in processed_data:
        print("Warning: Insufficient data for driver activity heatmap")
        return
    
    radio_df = processed_data['radio_messages']
    
    # Check if we have the necessary columns
    if 'driver_number' not in radio_df.columns:
        print("Warning: Driver number data missing for activity heatmap")
        return
    
    # Configure dark mode if requested
    if dark_mode:
        plt.style.use('dark_background')
        text_color = 'white'
        grid_color = 'gray'
        bg_color = '#333333'
        cmap = 'hot'
    else:
        plt.style.use('default')
        text_color = 'black'
        grid_color = 'lightgray'
        bg_color = 'white'
        cmap = 'viridis'
    
    # Create time bins
    if 'datetime' in radio_df.columns:
        # Calculate session duration
        session_start = radio_df['datetime'].min()
        session_end = radio_df['datetime'].max()
        session_duration = (session_end - session_start).total_seconds() / 60  # in minutes
        
        # Create bins (adjust bin size based on session duration)
        if session_duration > 90:  # For races (longer sessions)
            n_bins = 20  # 20 time segments
        else:  # For shorter sessions
            n_bins = 10  # 10 time segments
        
        # Create time bins
        bins = pd.date_range(start=session_start, end=session_end, periods=n_bins+1)
        radio_df['time_bin'] = pd.cut(radio_df['datetime'], bins=bins, labels=range(n_bins))
        
        # Get bin midpoints for x-axis labels
        bin_midpoints = [(bins[i] + (bins[i+1] - bins[i])/2) for i in range(n_bins)]
        x_tick_labels = [mp.strftime('%H:%M') for mp in bin_midpoints]
    else:
        # If datetime not available, use sequence bins
        total_messages = len(radio_df)
        n_bins = min(20, total_messages // 5)  # Adjust number of bins
        if n_bins <= 0:
            n_bins = 5  # Minimum number of bins
        
        radio_df['time_bin'] = pd.cut(range(total_messages), bins=n_bins, labels=range(n_bins))
        x_tick_labels = [f"{int(100*i/n_bins)}%" for i in range(n_bins)]
    
    # Get unique drivers
    drivers = sorted(radio_df['driver_number'].unique())
    
    # Extract driver names if available
    if 'driver_name' in radio_df.columns:
        driver_names = {}
        for driver, group in radio_df.groupby('driver_number'):
            driver_names[driver] = group['driver_name'].iloc[0]
        y_tick_labels = [driver_names.get(d, f"Driver #{d}") for d in drivers]
    else:
        y_tick_labels = [f"Driver #{d}" for d in drivers]
    
    # Create data for heatmap
    heatmap_data = np.zeros((len(drivers), n_bins))
    
    # Fill heatmap data
    for i, driver in enumerate(drivers):
        driver_data = radio_df[radio_df['driver_number'] == driver]
        for bin_idx in range(n_bins):
            heatmap_data[i, bin_idx] = len(driver_data[driver_data['time_bin'] == bin_idx])
    
    # Create figure
    plt.figure(figsize=FIG_SIZE, dpi=100)
    
    # Create heatmap
    im = plt.imshow(heatmap_data, cmap=cmap, aspect='auto')
    
    # Configure axes
    plt.yticks(range(len(drivers)), y_tick_labels)
    plt.xticks(range(n_bins), x_tick_labels, rotation=45, ha='right')
    
    # Configure title and labels
    plt.title(f"Team Radio Activity Heatmap - {race_name} - {session_name}", fontsize=14, color=text_color)
    plt.xlabel("Time", fontsize=12, color=text_color)
    plt.ylabel("Driver", fontsize=12, color=text_color)
    
    # Add colorbar
    cbar = plt.colorbar(im)
    cbar.set_label('Number of Messages', color=text_color)
    
    # Add values in cells
    for i in range(len(drivers)):
        for j in range(n_bins):
            if heatmap_data[i, j] > 0:
                text_color_cell = 'white' if heatmap_data[i, j] > heatmap_data.max()/2 else 'black'
                plt.text(j, i, int(heatmap_data[i, j]), ha="center", va="center", color=text_color_cell)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Driver activity heatmap saved to: {output_path}")
    plt.close()

def create_events_correlation_chart(processed_data, race_name, session_name, output_path, dark_mode=False):
    """
    Create a visualization showing correlation between team radio and important events.
    
    Args:
        processed_data: Dictionary with processed team radio data
        race_name: Event name for the title
        session_name: Session name for the title
        output_path: Path to save the visualization
        dark_mode: If True, use dark theme for visualization
    """
    if (not processed_data or 'events_correlation' not in processed_data or 
            not processed_data['events_correlation']):
        print("Warning: No event correlation data available for visualization")
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
    
    # Extract event correlation data
    events = processed_data['events_correlation']
    event_times = sorted(events.keys())
    
    # Create figure
    fig, ax = plt.subplots(figsize=FIG_SIZE, dpi=100)
    
    # Prepare data for plotting
    event_descriptions = []
    message_counts = []
    colors = []
    
    for event_time in event_times:
        event_data = events[event_time]
        event_descriptions.append(event_data['event'])
        message_counts.append(event_data['message_count'])
        
        # Set color based on event type
        if 'Flag' in event_data['event']:
            colors.append('red')
        elif 'Pit Stop' in event_data['event']:
            colors.append('blue')
        else:
            colors.append('green')
    
    # Create horizontal bar chart
    y_pos = range(len(event_descriptions))
    ax.barh(y_pos, message_counts, align='center', color=colors, alpha=0.7)
    
    # Add count labels
    for i, count in enumerate(message_counts):
        ax.text(count + 0.1, i, str(count), va='center', color=text_color)
    
    # Configure axes
    ax.set_yticks(y_pos)
    ax.set_yticklabels(event_descriptions)
    ax.invert_yaxis()  # Events at the top
    
    # Configure title and labels
    ax.set_title(f"Team Radio Messages Around Key Events - {race_name} - {session_name}", fontsize=14, color=text_color)
    ax.set_xlabel('Number of Radio Messages', fontsize=12, color=text_color)
    
    # Add grid for easier reading
    ax.grid(axis='x', linestyle='--', alpha=0.3, color=grid_color)
    
    # Add legend for event types
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='red', alpha=0.7, label='Flags'),
        Patch(facecolor='blue', alpha=0.7, label='Pit Stops'),
        Patch(facecolor='green', alpha=0.7, label='Other Events')
    ]
    ax.legend(handles=legend_elements, loc='lower right')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the visualization
    plt.savefig(output_path, dpi=DPI, facecolor=bg_color)
    print(f"Event correlation visualization saved to: {output_path}")
    plt.close()

def main():
    """Main function of the script."""
    # Process command line arguments
    args = parse_args()
    
    # Extract arguments
    meeting_key = args.meeting
    session_key = args.session
    specific_driver = args.driver
    dark_mode = args.dark_mode
    
    # Define output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Use visualizations directory within processed data
        output_dir = Path(f"f1_data/processed/{meeting_key}/{session_key}/visualizations/team_radio")
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define names for event and session
    race_name = args.race_name or f"Meeting_{meeting_key}"
    session_name = args.session_name or f"Session_{session_key}"
    
    try:
        # Load team radio data
        radio_df = load_team_radio_data(meeting_key, session_key)
        
        if radio_df is None:
            print("Error: Could not load team radio data")
            return 1
        
        # Load auxiliary data
        driver_info = load_driver_info(meeting_key, session_key)
        race_control_df = load_race_control_data(meeting_key, session_key)
        pit_df = load_pit_data(meeting_key, session_key)
        
        # Process radio data
        processed_data = process_radio_data(radio_df, driver_info, race_control_df, pit_df, specific_driver)
        
        if processed_data is None:
            print("Error: Could not process team radio data")
            return 1
        
        # Generate visualizations
        # Add driver suffix if specific driver is selected
        driver_suffix = f"_driver_{specific_driver}" if specific_driver else ""
        
        # 1. Message frequency by driver
        freq_path = output_dir / f"radio_frequency{driver_suffix}_{meeting_key}_{session_key}.png"
        create_message_frequency_chart(processed_data, race_name, session_name, freq_path, dark_mode)
        
        # 2. Message timeline
        timeline_path = output_dir / f"radio_timeline{driver_suffix}_{meeting_key}_{session_key}.png"
        create_message_timeline(processed_data, race_name, session_name, timeline_path, dark_mode)
        
        # 3. Driver activity heatmap
        if not specific_driver:  # Only create if multiple drivers
            heatmap_path = output_dir / f"radio_heatmap_{meeting_key}_{session_key}.png"
            create_driver_activity_heatmap(processed_data, race_name, session_name, heatmap_path, dark_mode)
        
        # 4. Event correlation chart
        if processed_data['events_correlation']:
            events_path = output_dir / f"radio_events{driver_suffix}_{meeting_key}_{session_key}.png"
            create_events_correlation_chart(processed_data, race_name, session_name, events_path, dark_mode)
        
        print("All team radio visualizations were successfully generated!")
        print(f"Visualizations are available in: {output_dir}")
        
    except Exception as e:
        print(f"Error creating visualizations: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())