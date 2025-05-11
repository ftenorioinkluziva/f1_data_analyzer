"""
Time utility functions for the F1 Data Analyzer.
"""
import datetime
import pandas as pd


def convert_lap_time_to_seconds(lap_time):
    """
    Convert a lap time string (MM:SS.mmm or SS.mmm) to seconds.
    
    Args:
        lap_time: The lap time string
        
    Returns:
        float: The lap time in seconds
    """
    try:
        if not lap_time or pd.isna(lap_time):
            return None
        
        lap_time = str(lap_time).strip()
        
        # Check if format is MM:SS.mmm
        if ':' in lap_time:
            minutes, seconds = lap_time.split(':')
            return float(minutes) * 60 + float(seconds)
        # Format is SS.mmm
        else:
            return float(lap_time)
    except (ValueError, TypeError):
        return None


def timestamp_to_datetime(timestamp):
    """
    Convert a timestamp string (HH:MM:SS.mmm) to a datetime object.
    
    Args:
        timestamp: The timestamp string
        
    Returns:
        datetime: The datetime object
    """
    try:
        if not timestamp or pd.isna(timestamp):
            return None
        
        timestamp = str(timestamp).strip()
        
        # Add date part (1900-01-01) for parsing
        datetime_str = f"1900-01-01 {timestamp}"
        return datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f")
    except (ValueError, TypeError):
        return None


def format_time_delta(seconds):
    """
    Format a time delta in seconds to a readable string.
    
    Args:
        seconds: The time in seconds
        
    Returns:
        str: Formatted time string
    """
    if seconds is None:
        return "N/A"
    
    # For negative values
    sign = "-" if seconds < 0 else ""
    seconds = abs(seconds)
    
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    
    if minutes > 0:
        return f"{sign}{minutes}:{remaining_seconds:06.3f}"
    else:
        return f"{sign}{remaining_seconds:.3f}"