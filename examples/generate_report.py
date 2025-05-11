"""
Example script for generating a comprehensive race report in HTML format.
"""
import asyncio
import argparse
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config
from src.utils.file_utils import ensure_directory
from src.utils.time_utils import convert_lap_time_to_seconds


def generate_race_report(race, session):
    """
    Generate a comprehensive HTML race report with embedded visualizations.
    
    Args:
        race: Race name
        session: Session name
    """
    print(f"=== Generating Race Report: {race} {session} ===")
    
    # Create output directory for the report
    report_dir = config.REPORTS_DIR / race
    ensure_directory(report_dir)
    
    # Define the report file path
    report_path = report_dir / f"{session}_report.html"
    
    # Check if necessary data exists
    lap_times_path = config.PROCESSED_DIR / race / session / "TimingData" / "lap_times.csv"
    positions_path = config.PROCESSED_DIR / race / session / "TimingData" / "positions.csv"
    tire_stints_path = config.PROCESSED_DIR / race / session / "TimingAppData" / "tire_stints.csv"
    stint_laps_path = config.PROCESSED_DIR / race / session / "StintAnalysis" / "stint_laps.csv"
    
    # Get visualization paths
    analysis_dir = config.ANALYSIS_DIR / race / session
    
    # Initialize HTML content
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>F1 Race Report: {race} - {session}</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
            }}
            h1, h2, h3 {{
                color: #e10600;  /* F1 red */
            }}
            .header {{
                background-color: #15151e;  /* F1 dark blue */
                color: white;
                padding: 20px;
                text-align: center;
                margin-bottom: 30px;
            }}
            .section {{
                margin-bottom: 40px;
                border-bottom: 1px solid #ddd;
                padding-bottom: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
            }}
            th, td {{
                padding: 10px;
                border: 1px solid #ddd;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            .visualization {{
                max-width: 100%;
                margin: 20px 0;
                text-align: center;
            }}
            .visualization img {{
                max-width: 100%;
                height: auto;
                border: 1px solid #ddd;
            }}
            .footer {{
                text-align: center;
                margin-top: 40px;
                color: #777;
                font-size: 0.9em;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>F1 Race Report</h1>
            <h2>{race.replace('_', ' ')} - {session.replace('_', ' ')}</h2>
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    """
    
    # Add race summary section
    html_content += """
        <div class="section">
            <h2>Race Summary</h2>
    """
    
    # Load position data if available
    if positions_path.exists():
        positions_df = pd.read_csv(positions_path)
        
        # Get final positions
        latest_positions = positions_df.sort_values('timestamp').groupby('driver_number').last()
        final_positions = latest_positions.sort_values('position')
        
        html_content += """
            <h3>Final Standings</h3>
            <table>
                <tr>
                    <th>Position</th>
                    <th>Driver</th>
                </tr>
        """
        
        for _, row in final_positions.iterrows():
            html_content += f"""
                <tr>
                    <td>{row['position']}</td>
                    <td>Driver #{row['driver_number']}</td>
                </tr>
            """
        
        html_content += """
            </table>
        """
    
    # Add position chart if available
    position_chart_path = analysis_dir / "positions" / "position_chart.png"
    if position_chart_path.exists():
        html_content += f"""
            <div class="visualization">
                <h3>Position Changes Throughout the Race</h3>
                <img src="../../{position_chart_path.relative_to(config.PROJECT_ROOT)}" alt="Position Chart">
            </div>
        """
    
    html_content += """
        </div>
    """
    
    # Add lap times section
    html_content += """
        <div class="section">
            <h2>Lap Time Analysis</h2>
    """
    
    # Load lap time data if available
    if lap_times_path.exists():
        lap_times_df = pd.read_csv(lap_times_path)
        
        # Get fastest laps
        lap_times_df['lap_time_seconds'] = lap_times_df['lap_time'].apply(convert_lap_time_to_seconds)
        fastest_laps = lap_times_df.sort_values('lap_time_seconds').groupby('driver_number').first()
        fastest_overall = fastest_laps.sort_values('lap_time_seconds').iloc[0]
        
        html_content += f"""
            <h3>Fastest Lap</h3>
            <p>Driver #{fastest_overall['driver_number']} - {fastest_overall['lap_time']} (Lap {fastest_overall['lap_number']})</p>
            
            <h3>Fastest Laps by Driver</h3>
            <table>
                <tr>
                    <th>Driver</th>
                    <th>Fastest Lap Time</th>
                    <th>Lap Number</th>
                </tr>
        """
        
        for driver, row in fastest_laps.iterrows():
            html_content += f"""
                <tr>
                    <td>Driver #{driver}</td>
                    <td>{row['lap_time']}</td>
                    <td>{row['lap_number']}</td>
                </tr>
            """
        
        html_content += """
            </table>
        """
    
    # Add lap time comparison chart if available
    lap_time_chart_path = analysis_dir / "lap_times" / "lap_times_comparison.png"
    if lap_time_chart_path.exists():
        html_content += f"""
            <div class="visualization">
                <h3>Lap Time Comparison</h3>
                <img src="../../{lap_time_chart_path.relative_to(config.PROJECT_ROOT)}" alt="Lap Time Comparison">
            </div>
        """
    
    html_content += """
        </div>
    """
    
    # Add tire strategy section
    html_content += """
        <div class="section">
            <h2>Tire Strategy</h2>
    """
    
    # Load stint data if available
    if stint_laps_path.exists():
        stint_laps_df = pd.read_csv(stint_laps_path)
        
        html_content += """
            <h3>Tire Strategies</h3>
            <table>
                <tr>
                    <th>Driver</th>
                    <th>Stint 1</th>
                    <th>Stint 2</th>
                    <th>Stint 3</th>
                    <th>Stint 4</th>
                </tr>
        """
        
        for driver in stint_laps_df['driver_number'].unique():
            driver_stints = stint_laps_df[stint_laps_df['driver_number'] == driver].sort_values('stint_number')
            
            row_content = f"<tr><td>Driver #{driver}</td>"
            
            # Add up to 4 stints (add empty cells if fewer)
            for i in range(1, 5):
                stint = driver_stints[driver_stints['stint_number'] == i]
                if not stint.empty:
                    compound = stint.iloc[0]['compound']
                    new_tire = stint.iloc[0]['new_tire']
                    stint_length = stint.iloc[0]['stint_length']
                    row_content += f"<td>{compound} ({'New' if new_tire else 'Used'}) - {stint_length} laps</td>"
                else:
                    row_content += "<td>-</td>"
            
            row_content += "</tr>"
            html_content += row_content
        
        html_content += """
            </table>
        """
    
    # Add tire strategy chart if available
    tire_strategy_chart_path = analysis_dir / "tire_strategy" / "tire_strategy_by_lap.png"
    if tire_strategy_chart_path.exists():
        html_content += f"""
            <div class="visualization">
                <h3>Tire Strategies Visualization</h3>
                <img src="../../{tire_strategy_chart_path.relative_to(config.PROJECT_ROOT)}" alt="Tire Strategies">
            </div>
        """
    
    html_content += """
        </div>
    """
    
    # Add telemetry section
    html_content += """
        <div class="section">
            <h2>Telemetry Analysis</h2>
    """
    
    # Add telemetry charts if available
    telemetry_dir = analysis_dir / "telemetry"
    if telemetry_dir.exists():
        # Find telemetry detail charts
        telemetry_charts = list(telemetry_dir.glob("telemetry_detail_driver_*.png"))
        
        for chart_path in telemetry_charts[:3]:  # Limit to 3 drivers
            driver = chart_path.stem.split("_")[-1]
            html_content += f"""
                <div class="visualization">
                    <h3>Telemetry Detail - Driver #{driver}</h3>
                    <img src="../../{chart_path.relative_to(config.PROJECT_ROOT)}" alt="Telemetry Detail Driver {driver}">
                </div>
            """
    
    html_content += """
        </div>
    """
    
    # Add track analysis section if available
    track_layout_path = analysis_dir / "positions" / "track_layout_2d.png"
    if track_layout_path.exists():
        html_content += """
            <div class="section">
                <h2>Track Analysis</h2>
        """
        
        html_content += f"""
            <div class="visualization">
                <h3>Track Layout</h3>
                <img src="../../{track_layout_path.relative_to(config.PROJECT_ROOT)}" alt="Track Layout">
            </div>
        """
        
        # Add 3D track layout if available
        track_3d_path = analysis_dir / "positions" / "track_layout_3d.png"
        if track_3d_path.exists():
            html_content += f"""
                <div class="visualization">
                    <h3>3D Track Profile</h3>
                    <img src="../../{track_3d_path.relative_to(config.PROJECT_ROOT)}" alt="3D Track Profile">
                </div>
            """
        
        html_content += """
            </div>
        """
    
    # Add footer
    html_content += """
        <div class="footer">
            <p>Generated using F1 Data Analyzer</p>
            <p>Data source: F1 Live Timing API</p>
        </div>
    </body>
    </html>
    """
    
    # Write the HTML to file
    with open(report_path, 'w') as f:
        f.write(html_content)
    
    print(f"Race report generated successfully at {report_path}")
    return report_path


def main():
    """Parse command line arguments and generate the race report."""
    parser = argparse.ArgumentParser(description="Generate an F1 race report.")
    parser.add_argument("--race", type=str, required=True, help="Race name (e.g., Miami_Grand_Prix)")
    parser.add_argument("--session", type=str, required=True, help="Session name (e.g., Race, Qualifying)")
    
    args = parser.parse_args()
    
    # Generate the report
    generate_race_report(args.race, args.session)


if __name__ == "__main__":
    main()