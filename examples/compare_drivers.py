"""
Example script for comparing the performance of specific F1 drivers.
"""
import asyncio
import argparse
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config
from src.utils.time_utils import convert_lap_time_to_seconds
from src.utils.file_utils import ensure_directory


async def compare_drivers(race, session, driver_numbers):
    """
    Compare the performance of specific drivers in a race session.
    
    Args:
        race: Race name
        session: Session name
        driver_numbers: List of driver numbers to compare
    """
    if not driver_numbers or len(driver_numbers) < 2:
        print("Error: At least two driver numbers are required for comparison.")
        return
    
    print(f"=== Comparing Drivers: {', '.join(driver_numbers)} in {race} {session} ===")
    
    # Create output directory for comparison results
    output_dir = config.ANALYSIS_DIR / race / session / "driver_comparison"
    ensure_directory(output_dir)
    
    # Load lap time data
    lap_times_path = config.PROCESSED_DIR / race / session / "TimingData" / "lap_times.csv"
    if not lap_times_path.exists():
        print(f"Error: Lap times data not found at {lap_times_path}")
        return
    
    lap_times_df = pd.read_csv(lap_times_path)
    
    # Filter for the specified drivers
    selected_drivers = [str(d) for d in driver_numbers]
    filtered_df = lap_times_df[lap_times_df['driver_number'].isin(selected_drivers)]
    
    if filtered_df.empty:
        print("No lap time data found for the specified drivers.")
        return
    
    # Convert lap times to seconds
    filtered_df['lap_time_seconds'] = filtered_df['lap_time'].apply(convert_lap_time_to_seconds)
    
    # Create lap time comparison chart
    plt.figure(figsize=(14, 8))
    
    # Create a color map for consistent driver colors
    cmap = plt.cm.get_cmap('tab10', len(selected_drivers))
    
    for i, driver in enumerate(selected_drivers):
        driver_data = filtered_df[filtered_df['driver_number'] == driver]
        
        # Group by lap number and get the fastest time if there are duplicates
        driver_laps = driver_data.groupby('lap_number')['lap_time_seconds'].min().reset_index()
        
        # Sort by lap number
        driver_laps = driver_laps.sort_values('lap_number')
        
        # Plot lap times
        plt.plot(
            driver_laps['lap_number'], 
            driver_laps['lap_time_seconds'], 
            'o-', 
            label=f"Driver #{driver}",
            color=cmap(i)
        )
    
    plt.xlabel('Lap Number')
    plt.ylabel('Lap Time (seconds)')
    plt.title(f'Lap Time Comparison - {race} - {session}')
    plt.grid(True)
    plt.legend()
    
    # Save the figure
    plt.tight_layout()
    comparison_file = output_dir / "lap_time_comparison.png"
    plt.savefig(comparison_file)
    plt.close()
    
    print(f"Lap time comparison saved to {comparison_file}")
    
    # Load telemetry data if available
    telemetry_paths = {}
    for driver in selected_drivers:
        path = config.PROCESSED_DIR / race / session / "CarData.z" / "drivers" / f"telemetry_driver_{driver}.csv"
        if path.exists():
            telemetry_paths[driver] = path
    
    if telemetry_paths:
        print(f"Found telemetry data for {len(telemetry_paths)} drivers")
        
        # Load telemetry data
        telemetry_data = {}
        for driver, path in telemetry_paths.items():
            telemetry_data[driver] = pd.read_csv(path)
        
        # Create speed comparison chart
        plt.figure(figsize=(14, 8))
        
        for i, (driver, data) in enumerate(telemetry_data.items()):
            # Sample the data to avoid overcrowding the chart (every 10th point)
            sampled_data = data.iloc[::10].reset_index()
            
            plt.plot(
                sampled_data.index,
                sampled_data['speed'],
                '-',
                label=f"Driver #{driver}",
                color=cmap(selected_drivers.index(driver))
            )
        
        plt.xlabel('Sample Index')
        plt.ylabel('Speed (km/h)')
        plt.title(f'Speed Comparison - {race} - {session}')
        plt.grid(True)
        plt.legend()
        
        # Save the figure
        plt.tight_layout()
        speed_file = output_dir / "speed_comparison.png"
        plt.savefig(speed_file)
        plt.close()
        
        print(f"Speed comparison saved to {speed_file}")
        
        # Create throttle/brake usage comparison
        # Calculate average throttle and brake usage for each driver
        throttle_brake_stats = {}
        
        for driver, data in telemetry_data.items():
            throttle_avg = data['throttle'].mean()
            brake_avg = data['brake'].mean()
            
            throttle_brake_stats[driver] = {
                'avg_throttle': throttle_avg,
                'avg_brake': brake_avg
            }
        
        # Create bar chart
        fig, ax = plt.subplots(figsize=(10, 6))
        
        drivers = list(throttle_brake_stats.keys())
        x = np.arange(len(drivers))
        width = 0.35
        
        throttle_values = [throttle_brake_stats[d]['avg_throttle'] for d in drivers]
        brake_values = [throttle_brake_stats[d]['avg_brake'] for d in drivers]
        
        ax.bar(x - width/2, throttle_values, width, label='Avg Throttle', color='green')
        ax.bar(x + width/2, brake_values, width, label='Avg Brake', color='red')
        
        ax.set_xlabel('Driver')
        ax.set_ylabel('Average Usage (%)')
        ax.set_title(f'Throttle/Brake Usage Comparison - {race} - {session}')
        ax.set_xticks(x)
        ax.set_xticklabels([f"Driver #{d}" for d in drivers])
        ax.legend()
        
        plt.tight_layout()
        tb_file = output_dir / "throttle_brake_comparison.png"
        plt.savefig(tb_file)
        plt.close()
        
        print(f"Throttle/brake comparison saved to {tb_file}")
    
    # Load stint data if available
    stint_path = config.PROCESSED_DIR / race / session / "StintAnalysis" / "stint_laps.csv"
    if stint_path.exists():
        print("Found stint data for performance comparison")
        
        stint_df = pd.read_csv(stint_path)
        
        # Filter for the specified drivers
        filtered_stints = stint_df[stint_df['driver_number'].isin(selected_drivers)]
        
        if not filtered_stints.empty:
            # Create stint comparison chart
            plt.figure(figsize=(14, 8))
            
            # Set up y-positions and labels
            y_positions = []
            y_labels = []
            
            for i, driver in enumerate(selected_drivers):
                driver_stints = filtered_stints[filtered_stints['driver_number'] == driver].sort_values('stint_number')
                
                if driver_stints.empty:
                    continue
                
                y_pos = i
                y_positions.append(y_pos)
                y_labels.append(f"Driver #{driver}")
                
                for _, stint in driver_stints.iterrows():
                    compound = stint['compound']
                    color = config.TIRE_COMPOUND_COLORS.get(compound, "gray")
                    
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
            plt.title(f'Tire Strategy Comparison - {race} - {session}')
            plt.grid(axis='x')
            
            # Add legend
            legend_elements = [
                plt.Rectangle((0, 0), 1, 1, color=color, edgecolor='black', label=compound)
                for compound, color in config.TIRE_COMPOUND_COLORS.items()
            ]
            plt.legend(handles=legend_elements, bbox_to_anchor=(1.05, 1), loc='upper left')
            
            plt.tight_layout()
            
            # Save the figure
            tire_file = output_dir / "tire_strategy_comparison.png"
            plt.savefig(tire_file)
            plt.close()
            
            print(f"Tire strategy comparison saved to {tire_file}")
    
    print("\n=== Driver Comparison Complete ===")
    print(f"All comparison charts saved to {output_dir}")


def main():
    """Parse command line arguments and run the driver comparison."""
    parser = argparse.ArgumentParser(description="Compare F1 drivers in a race session.")
    parser.add_argument("--race", type=str, required=True, help="Race name (e.g., Miami_Grand_Prix)")
    parser.add_argument("--session", type=str, required=True, help="Session name (e.g., Race, Qualifying)")
    parser.add_argument("--drivers", nargs="+", type=str, required=True, help="Driver numbers to compare (e.g., 1 44 16)")
    
    args = parser.parse_args()
    
    # Run the comparison
    asyncio.run(compare_drivers(args.race, args.session, args.drivers))


if __name__ == "__main__":
    main()