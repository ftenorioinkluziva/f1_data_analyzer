"""
Example script for analyzing an F1 race using the F1 Data Analyzer.
"""
import asyncio
import argparse
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.collectors.race_data_collector import RaceDataCollector
from src.processors.timing_data_processor import TimingDataProcessor
from src.processors.car_data_processor import CarDataProcessor
from src.processors.timing_app_processor import TimingAppProcessor
from src.processors.stint_analyzer import StintAnalyzer
from src.visualizers.lap_time_visualizer import LapTimeVisualizer
from src.visualizers.telemetry_visualizer import TelemetryVisualizer
from src.visualizers.position_visualizer import PositionVisualizer
from src.visualizers.tire_strategy_visualizer import TireStrategyVisualizer


async def analyze_race(year, race, session, driver_numbers=None):
    """
    Full analysis workflow for an F1 race session.
    
    Args:
        year: Year of the race
        race: Race name
        session: Session name
        driver_numbers: Optional list of driver numbers to focus on
    """
    print(f"=== Analyzing F1 Race: {year} {race} {session} ===")
    
    # Step 1: Collect Data
    print("\n=== Step 1: Collecting Data ===")
    collector = RaceDataCollector()
    collected_data = await collector.collect_session_data(year, race, session)
    
    if not collected_data:
        print("Error: No data collected. Exiting.")
        return
    
    print(f"Successfully collected data for topics: {', '.join(collected_data.keys())}")
    
    # Step 2: Process Data
    print("\n=== Step 2: Processing Data ===")
    
    # Process timing data
    if "TimingData" in collected_data:
        print("\nProcessing timing data...")
        timing_processor = TimingDataProcessor()
        timing_results = timing_processor.process(race, session)
        print(f"Processed timing data with {len(timing_results)} result files")
    
    # Process car telemetry data
    if "CarData.z" in collected_data:
        print("\nProcessing car telemetry data...")
        car_processor = CarDataProcessor()
        car_results = car_processor.process(race, session)
        print(f"Processed car telemetry data with {len(car_results)} result files")
    
    # Process timing app data
    if "TimingAppData" in collected_data:
        print("\nProcessing timing app data...")
        app_processor = TimingAppProcessor()
        app_results = app_processor.process(race, session)
        print(f"Processed timing app data with {len(app_results)} result files")
    
    # Analyze stints
    if "TimingData" in collected_data and "TimingAppData" in collected_data:
        print("\nAnalyzing tire stints...")
        stint_analyzer = StintAnalyzer()
        stint_results = stint_analyzer.analyze(race, session)
        print(f"Completed stint analysis with {len(stint_results)} result files")
    
    # Step 3: Create Visualizations
    print("\n=== Step 3: Creating Visualizations ===")
    
    # Create lap time visualizations
    print("\nCreating lap time visualizations...")
    lap_time_viz = LapTimeVisualizer()
    lap_time_results = lap_time_viz.create_visualizations(race, session, driver_numbers)
    
    # Create telemetry visualizations
    print("\nCreating telemetry visualizations...")
    telemetry_viz = TelemetryVisualizer()
    telemetry_results = telemetry_viz.create_visualizations(race, session, driver_numbers)
    
    # Create position visualizations
    print("\nCreating position visualizations...")
    position_viz = PositionVisualizer()
    position_results = position_viz.create_visualizations(race, session, driver_numbers)
    
    # Create tire strategy visualizations
    print("\nCreating tire strategy visualizations...")
    tire_viz = TireStrategyVisualizer()
    tire_results = tire_viz.create_visualizations(race, session, driver_numbers)
    
    print("\n=== Analysis Complete ===")
    print("Check the data and visualization directories for the results.")


def main():
    """Parse command line arguments and run the analysis."""
    parser = argparse.ArgumentParser(description="Analyze an F1 race session.")
    parser.add_argument("--year", type=str, required=True, help="Year of the race (e.g., 2024)")
    parser.add_argument("--race", type=str, required=True, help="Race name (e.g., Miami_Grand_Prix)")
    parser.add_argument("--session", type=str, required=True, help="Session name (e.g., Race, Qualifying, Practice_1)")
    parser.add_argument("--drivers", nargs="+", type=str, help="Optional driver numbers to focus on (e.g., 1 44 16)")
    
    args = parser.parse_args()
    
    # Run the analysis
    asyncio.run(analyze_race(args.year, args.race, args.session, args.drivers))


if __name__ == "__main__":
    main()