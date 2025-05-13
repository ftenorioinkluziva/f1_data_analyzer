"""
Configuration settings for the F1 Data Analyzer project.
"""
from pathlib import Path

# Base URL for F1 live timing data
BASE_URL = "https://livetiming.formula1.com/static"

# Directory structure
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "f1_data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"


# Ensure directories exist
for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR,]:
    directory.mkdir(exist_ok=True, parents=True)

# Important data topics to collect
IMPORTANT_TOPICS = [
    "DriverList",        # Driver information
    "Position.z",        # Car positions on track
    "CarData.z",         # Car telemetry (speed, RPM, etc.)
    "TimingData",        # Lap times
    "TimingAppData",     # Additional timing data and tire information
    "WeatherData",       # Weather data
    "RaceControlMessages", # Race control messages
    "SessionInfo",       # Session information
    "TeamRadio"          # Team radio communications
]

# Dictionary of tire compounds and their visualization colors
TIRE_COMPOUND_COLORS = {
    "SOFT": "red",
    "MEDIUM": "yellow",
    "HARD": "white",
    "INTERMEDIATE": "green",
    "WET": "blue"
}

# Figure sizes for different visualizations
FIGURE_SIZES = {
    "lap_times": (14, 8),
    "position_chart": (14, 8),
    "telemetry": (16, 8),
    "track_layout": (12, 12),
    "tire_strategy": (16, 8)
}

# Visualization settings
DEFAULT_DPI = 100
SAVE_FIGURES = True  # Set to False to display figures instead of saving them