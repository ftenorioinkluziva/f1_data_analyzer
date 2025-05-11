"""
Main entry point for the F1 Data Analyzer application.
Provides a command-line interface to collect, process and visualize F1 race data.
"""
import argparse
import sys
from pathlib import Path

from src.collectors.race_data_collector import RaceDataCollector
from src.processors.timing_data_processor import TimingDataProcessor
from src.processors.car_data_processor import CarDataProcessor
from src.processors.timing_app_processor import TimingAppProcessor
from src.processors.stint_analyzer import StintAnalyzer
from src.visualizers.lap_time_visualizer import LapTimeVisualizer
from src.visualizers.telemetry_visualizer import TelemetryVisualizer
from src.visualizers.position_visualizer import PositionVisualizer
from src.visualizers.tire_strategy_visualizer import TireStrategyVisualizer
from src.processors.weather_data_processor import WeatherDataProcessor
from src.processors.current_tyres_processor import CurrentTyresProcessor
from src.processors.driver_list_processor import DriverListProcessor
from src.processors.pit_lane_processor import PitLaneProcessor
from src.processors.position_processor import PositionProcessor
from src.processors.race_control_messages_processor import RaceControlMessagesProcessor
from src.processors.team_radio_processor import TeamRadioProcessor


import config


def parse_arguments():
    parser = argparse.ArgumentParser(description="F1 Data Analyzer - Collect and analyze Formula 1 race data")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Explore command
    explore_parser = subparsers.add_parser("explore", help="Explore available F1 data")

    # Collect command
    collect_parser = subparsers.add_parser("collect", help="Collect F1 data from the official API")
    collect_parser.add_argument("--year", type=str, help="Year of the race (e.g., 2024)")
    collect_parser.add_argument("--race", type=str, help="Race name (e.g., Miami_Grand_Prix)")
    collect_parser.add_argument("--session", type=str, help="Session name (e.g., Race, Qualifying, Practice_1)")
    collect_parser.add_argument("--topics", nargs="+", help="Specific topics to collect (default: all important topics)")
    
    # Process command
    process_parser = subparsers.add_parser("process", help="Process collected raw data")
    process_parser.add_argument("--race", type=str, required=True, help="Race name to process")
    process_parser.add_argument("--session", type=str, required=True, help="Session name to process")
    process_parser.add_argument("--topics", nargs="+", help="Specific topics to process (default: all available)")
    
    # Visualize command
    visualize_parser = subparsers.add_parser("visualize", help="Create visualizations from processed data")
    visualize_parser.add_argument("--race", type=str, required=True, help="Race name to visualize")
    visualize_parser.add_argument("--session", type=str, required=True, help="Session name to visualize")
    visualize_parser.add_argument("--type", choices=["lap_times", "telemetry", "positions", "tire_strategy", "all"], 
                               default="all", help="Type of visualization to create")
    visualize_parser.add_argument("--drivers", nargs="+", help="Driver numbers to include in visualization")
    
    # Full pipeline command
    pipeline_parser = subparsers.add_parser("pipeline", help="Run the full data pipeline (collect, process, visualize)")
    pipeline_parser.add_argument("--year", type=str, required=True, help="Year of the race")
    pipeline_parser.add_argument("--race", type=str, required=True, help="Race name")
    pipeline_parser.add_argument("--session", type=str, required=True, help="Session name")
    
    return parser.parse_args()

def run_explore(args):
    """Explore available F1 data in the API."""
    print("Exploring available F1 data...")
    
    # This function is defined in f1_explorer.py
    from f1_explorer import explore_available_data
    results = explore_available_data()
    
    if results:
        print("Exploration completed successfully.")
    else:
        print("Exploration failed or no data found.")

async def run_collect(args):
    """Collect F1 data from the official API"""
    collector = RaceDataCollector()
    
    year = args.year
    race = args.race
    session = args.session
    topics = args.topics or config.IMPORTANT_TOPICS
    
    print(f"Collecting data for {year}/{race}/{session}...")
    await collector.collect_session_data(year, race, session, topics)
    print("Data collection completed")

def run_process(args):
    """Process collected raw data"""
    race = args.race
    session = args.session
    topics = args.topics
    
    print(f"Processing data for {race}/{session}...")
    
    # Process timing data if available or requested
    if topics is None or "TimingData" in topics:
        timing_processor = TimingDataProcessor()
        timing_processor.process(race, session)
    
    # Process car data if available or requested
    if topics is None or "CarData.z" in topics:
        car_processor = CarDataProcessor()
        car_processor.process(race, session)
    
    # Process timing app data if available or requested
    if topics is None or "TimingAppData" in topics:
        app_processor = TimingAppProcessor()
        app_processor.process(race, session)

    # Process weather data if available or requested
    if topics is None or "WeatherData" in topics:
        weather_processor = WeatherDataProcessor()
        weather_processor.process(race, session)  

    # Process current tyres data if available or requested
    if topics is None or "CurrentTyres" in topics:
        tyres_processor = CurrentTyresProcessor()
        tyres_processor.process(race, session)    

    # Process driver list data if available or requested
    if topics is None or "DriverList" in topics:
        driver_list_processor = DriverListProcessor()
        driver_list_processor.process(race, session)        

    # Process pit lane time collection data if available or requested
    if topics is None or "PitLaneTimeCollection" in topics:
        pit_lane_processor = PitLaneProcessor()
        pit_lane_processor.process(race, session)           

    # Process position data if available or requested
    if topics is None or "Position.z" in topics:
        position_processor = PositionProcessor()
        position_processor.process(race, session)       

    # Process race control messages if available or requested
    if topics is None or "RaceControlMessages" in topics:
        messages_processor = RaceControlMessagesProcessor()
        messages_processor.process(race, session)     
        
    # Process team radio data if available or requested
    if topics is None or "TeamRadio" in topics:
        radio_processor = TeamRadioProcessor()
        radio_processor.process(race, session)           
    
    # Analyze stints if both timing data and timing app data are available
    timing_data_path = config.RAW_DATA_DIR / race / session / "TimingData.jsonStream"
    timing_app_path = config.RAW_DATA_DIR / race / session / "TimingAppData.jsonStream"
    
    if timing_data_path.exists() and timing_app_path.exists():
        stint_analyzer = StintAnalyzer()
        stint_analyzer.analyze(race, session)
    
    print("Data processing completed")

def run_visualize(args):
    """Create visualizations from processed data"""
    race = args.race
    session = args.session
    viz_type = args.type
    driver_numbers = args.drivers
    
    print(f"Creating visualizations for {race}/{session}...")
    
    # Create visualizations based on the requested type
    if viz_type in ["lap_times", "all"]:
        lap_time_viz = LapTimeVisualizer()
        lap_time_viz.create_visualizations(race, session, driver_numbers)
    
    if viz_type in ["telemetry", "all"]:
        telemetry_viz = TelemetryVisualizer()
        telemetry_viz.create_visualizations(race, session, driver_numbers)
    
    if viz_type in ["positions", "all"]:
        position_viz = PositionVisualizer()
        position_viz.create_visualizations(race, session, driver_numbers)
    
    if viz_type in ["tire_strategy", "all"]:
        tire_viz = TireStrategyVisualizer()
        tire_viz.create_visualizations(race, session, driver_numbers)
    
    print("Visualization completed")

async def run_pipeline(args):
    """Run the full data pipeline (collect, process, visualize)"""
    # First collect the data
    await run_collect(args)
    
    # Then process it
    process_args = argparse.Namespace(
        race=args.race,
        session=args.session,
        topics=None
    )
    run_process(process_args)
    
    # Finally visualize it
    visualize_args = argparse.Namespace(
        race=args.race,
        session=args.session,
        type="all",
        drivers=None
    )
    run_visualize(visualize_args)

async def main():
    """Main entry point for the application"""
    args = parse_arguments()
    if args.command == "explore":
       run_explore(args)

    elif args.command == "collect":
      await run_collect(args)

    if args.command == "collect":
        await run_collect(args)
        
    elif args.command == "process":
        run_process(args)
    elif args.command == "visualize":
        run_visualize(args)
    elif args.command == "pipeline":
        await run_pipeline(args)
    else:
        print("Please specify a command: collect, process, visualize, or pipeline")
        sys.exit(1)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())