"""
Unit tests for the visualizers module.
"""
import unittest
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys
import os
import shutil

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.visualizers.lap_time_visualizer import LapTimeVisualizer
from src.visualizers.telemetry_visualizer import TelemetryVisualizer
from src.visualizers.position_visualizer import PositionVisualizer
from src.visualizers.tire_strategy_visualizer import TireStrategyVisualizer


class TestLapTimeVisualizer(unittest.TestCase):
    """Test the LapTimeVisualizer class."""
    
    def setUp(self):
        """Set up the test environment."""
        self.visualizer = LapTimeVisualizer()
        
        # Create a mock DataFrame for lap times
        self.lap_times_df = pd.DataFrame({
            "timestamp": ["00:00:01.123", "00:00:02.456", "00:00:03.789"],
            "driver_number": ["1", "1", "1"],
            "lap_number": [1, 2, 3],
            "lap_time": ["1:30.123", "1:29.456", "1:28.789"],
            "fastest": [False, False, True],
            "personal_fastest": [False, True, True]
        })
    
    def test_create_lap_time_chart(self):
        """Test the create_lap_time_chart method."""
        # Save the current backend and switch to a non-interactive one for testing
        original_backend = plt.get_backend()
        plt.switch_backend('Agg')
        
        try:
            # Create a lap time chart
            figure_paths = self.visualizer.create_lap_time_chart(
                self.lap_times_df,
                "test_race",
                "test_session"
            )
            
            # Check that the figure paths dictionary contains expected keys
            self.assertIn("comparison", figure_paths)
            
            # Check that the figure files exist
            for path in figure_paths.values():
                self.assertTrue(path.exists())
                
                # Clean up
                if path.exists():
                    path.unlink()
            
            # Clean up directories
            for path in figure_paths.values():
                dir_path = path.parent
                if dir_path.exists() and not any(dir_path.iterdir()):
                    dir_path.rmdir()
        
        finally:
            # Restore the original backend
            plt.switch_backend(original_backend)
    
    def test_get_processed_file_path(self):
        """Test the get_processed_file_path method."""
        expected_path = self.visualizer.processed_dir / "test_race" / "test_session" / "TimingData" / "lap_times.csv"
        result = self.visualizer.get_processed_file_path("test_race", "test_session", "TimingData", "lap_times.csv")
        self.assertEqual(result, expected_path)


class TestTelemetryVisualizer(unittest.TestCase):
    """Test the TelemetryVisualizer class."""
    
    def setUp(self):
        """Set up the test environment."""
        self.visualizer = TelemetryVisualizer()
        
        # Create a mock DataFrame for car telemetry
        self.car_data_df = pd.DataFrame({
            "timestamp": ["00:00:01.123", "00:00:02.456", "00:00:03.789"],
            "driver_number": ["1", "1", "1"],
            "speed": [250, 260, 270],
            "rpm": [12000, 12500, 13000],
            "gear": [7, 8, 8],
            "throttle": [100, 100, 100],
            "brake": [0, 0, 0],
            "drs": [1, 1, 1]
        })
    
    def test_create_speed_chart(self):
        """Test the create_speed_chart method."""
        # Save the current backend and switch to a non-interactive one for testing
        original_backend = plt.get_backend()
        plt.switch_backend('Agg')
        
        try:
            # Create a speed chart
            figure_paths = self.visualizer.create_speed_chart(
                self.car_data_df,
                "test_race",
                "test_session"
            )
            
            # Check that the figure paths dictionary contains expected keys
            expected_key = "speed_driver_1"
            self.assertIn(expected_key, figure_paths)
            
            # Check that the figure files exist
            for path in figure_paths.values():
                self.assertTrue(path.exists())
                
                # Clean up
                if path.exists():
                    path.unlink()
            
            # Clean up directories
            for path in figure_paths.values():
                dir_path = path.parent
                if dir_path.exists() and not any(dir_path.iterdir()):
                    dir_path.rmdir()
        
        finally:
            # Restore the original backend
            plt.switch_backend(original_backend)


class TestPositionVisualizer(unittest.TestCase):
    """Test the PositionVisualizer class."""
    
    def setUp(self):
        """Set up the test environment."""
        self.visualizer = PositionVisualizer()
        
        # Create a mock DataFrame for positions
        self.positions_df = pd.DataFrame({
            "timestamp": ["00:00:01.123", "00:00:02.456", "00:00:03.789"],
            "driver_number": ["1", "1", "1"],
            "position": [1, 1, 1]
        })
    
    def test_create_position_chart(self):
        """Test the create_position_chart method."""
        # Save the current backend and switch to a non-interactive one for testing
        original_backend = plt.get_backend()
        plt.switch_backend('Agg')
        
        try:
            # Create a position chart
            figure_paths = self.visualizer.create_position_chart(
                self.positions_df,
                "test_race",
                "test_session"
            )
            
            # Check that the figure paths dictionary contains expected keys
            expected_key = "position_chart"
            self.assertIn(expected_key, figure_paths)
            
            # Check that the figure files exist
            for path in figure_paths.values():
                self.assertTrue(path.exists())
                
                # Clean up
                if path.exists():
                    path.unlink()
            
            # Clean up directories
            for path in figure_paths.values():
                dir_path = path.parent
                if dir_path.exists() and not any(dir_path.iterdir()):
                    dir_path.rmdir()
        
        finally:
            # Restore the original backend
            plt.switch_backend(original_backend)


class TestTireStrategyVisualizer(unittest.TestCase):
    """Test the TireStrategyVisualizer class."""
    
    def setUp(self):
        """Set up the test environment."""
        self.visualizer = TireStrategyVisualizer()
        
        # Create a mock DataFrame for tire stints
        self.tire_stints_df = pd.DataFrame({
            "timestamp": ["00:00:01.123", "00:00:02.456", "00:00:03.789"],
            "driver_number": ["1", "1", "1"],
            "stint_number": [1, 2, 3],
            "compound": ["SOFT", "MEDIUM", "HARD"],
            "new_tire": [True, False, True],
            "total_laps": [20, 25, 15],
            "start_laps": [1, 21, 46]
        })
    
    def test_create_tire_strategy_chart(self):
        """Test the create_tire_strategy_chart method."""
        # Save the current backend and switch to a non-interactive one for testing
        original_backend = plt.get_backend()
        plt.switch_backend('Agg')
        
        try:
            # Create a tire strategy chart
            figure_paths = self.visualizer.create_tire_strategy_chart(
                self.tire_stints_df,
                "test_race",
                "test_session"
            )
            
            # Check that the figure paths dictionary contains expected keys
            expected_key = "tire_strategy"
            self.assertIn(expected_key, figure_paths)
            
            # Check that the figure files exist
            for path in figure_paths.values():
                self.assertTrue(path.exists())
                
                # Clean up
                if path.exists():
                    path.unlink()
            
            # Clean up directories
            for path in figure_paths.values():
                dir_path = path.parent
                if dir_path.exists() and not any(dir_path.iterdir()):
                    try:
                        dir_path.rmdir()
                    except OSError:
                        # Directory may not be empty if other tests are running
                        pass
        
        finally:
            # Restore the original backend
            plt.switch_backend(original_backend)


def tearDownModule():
    """Clean up after all tests."""
    # Remove test directories
    test_dirs = [
        Path("data"),
        Path("reports")
    ]
    
    for dir_path in test_dirs:
        if dir_path.exists():
            shutil.rmtree(dir_path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()