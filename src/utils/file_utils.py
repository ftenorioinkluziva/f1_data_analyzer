"""
File utility functions for the F1 Data Analyzer.
"""
from pathlib import Path


def ensure_directory(directory_path):
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        directory_path: Path to the directory
        
    Returns:
        Path: The directory path
    """
    directory = Path(directory_path)
    directory.mkdir(exist_ok=True, parents=True)
    return directory


def get_available_races(data_dir):
    """
    Get a list of available races in the data directory.
    
    Args:
        data_dir: Path to the data directory
        
    Returns:
        list: List of race names
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        return []
    
    return [d.name for d in data_path.iterdir() if d.is_dir()]


def get_available_sessions(data_dir, race_name):
    """
    Get a list of available sessions for a race.
    
    Args:
        data_dir: Path to the data directory
        race_name: Name of the race
        
    Returns:
        list: List of session names
    """
    race_path = Path(data_dir) / race_name
    if not race_path.exists():
        return []
    
    return [d.name for d in race_path.iterdir() if d.is_dir()]


def get_available_topics(data_dir, race_name, session_name):
    """
    Get a list of available data topics for a session.
    
    Args:
        data_dir: Path to the data directory
        race_name: Name of the race
        session_name: Name of the session
        
    Returns:
        list: List of topic names
    """
    session_path = Path(data_dir) / race_name / session_name
    if not session_path.exists():
        return []
    
    topics = []
    for file_path in session_path.glob("*.jsonStream"):
        topic = file_path.name.replace(".jsonStream", "")
        topics.append(topic)
    
    return topics