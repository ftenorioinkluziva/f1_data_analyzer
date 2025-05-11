"""
Unit tests for the processors module.
"""
import unittest
import pandas as pd
from pathlib import Path
import sys
import os
import json

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.processors.timing_data_processor import TimingDataProcessor
from src.processors.car_data_processor import CarDataProcessor
from src.utils.data_decoders import decode_compressed_data


class TestTimingDataProcessor(unittest.TestCase):
    """Test the TimingDataProcessor class."""
    
    def setUp(self):
        """Set up the test environment."""
        self.processor = TimingDataProcessor()
    
    def test_extract_timestamped_data(self):
        """Test the extract_timestamped_data method with a mock file."""
        # Create a mock file with timestamped data
        mock_content = """
        00:00:01.123{"key": "value1"}
        00:00:02.456{"key": "value2"}
        00:00:03.789{"key": "value3"}
        """
        mock_file = Path("mock_timing_data.txt")
        with open(mock_file, "w") as f:
            f.write(mock_content)
        
        # Extract the timestamped data
        timestamped_data = self.processor.extract_timestamped_data(mock_file)
        
        # Clean up
        mock_file.unlink()
        
        # Check the results
        self.assertEqual(len(timestamped_data), 3)
        self.assertEqual(timestamped_data[0][0], "00:00:01.123")
        self.assertEqual(timestamped_data[0][1], '{"key": "value1"}')
    
    def test_parse_json_data(self):
        """Test the parse_json_data method."""
        # Mock timestamped data
        timestamped_data = [
            ("00:00:01.123", '{"key": "value1"}'),
            ("00:00:02.456", '{"key": "value2"}'),
            ("00:00:03.789", '{"key": "value3"}')
        ]
        
        # Parse the JSON data
        parsed_data = self.processor.parse_json_data(timestamped_data)
        
        # Check the results
        self.assertEqual(len(parsed_data), 3)
        self.assertEqual(parsed_data[0]["timestamp"], "00:00:01.123")
        self.assertEqual(parsed_data[0]["data"]["key"], "value1")
    
    def test_save_to_csv(self):
        """Test the save_to_csv method."""
        # Create a mock DataFrame
        df = pd.DataFrame({
            "driver_number": ["1", "44", "16"],
            "position": [1, 2, 3]
        })
        
        # Save the DataFrame to CSV
        file_path = self.processor.save_to_csv(
            df,
            "test_race",
            "test_session",
            "TimingData",
            "test_positions.csv"
        )
        
        # Check that the file exists
        self.assertTrue(file_path.exists())
        
        # Check the file content
        df_read = pd.read_csv(file_path)
        self.assertEqual(len(df_read), 3)
        self.assertEqual(df_read["driver_number"].tolist(), ["1", "44", "16"])
        
        # Clean up
        file_path.unlink()
        file_path.parent.rmdir()  # Remove the directory if empty


class TestCarDataProcessor(unittest.TestCase):
    """Test the CarDataProcessor class."""
    
    def setUp(self):
        """Set up the test environment."""
        self.processor = CarDataProcessor()
    
    def test_decode_compressed_data(self):
        """Test the decode_compressed_data function with mock data."""
        # We can't easily test with real compressed data in a unit test
        # This test will validate the error handling for an invalid input
        result = decode_compressed_data("invalid_compressed_data")
        self.assertIsNone(result)
    
    def test_extract_car_telemetry(self):
        """Test the extract_car_telemetry method with mock data."""
        # This test requires real sample data, which is not available in this context
        # Instead, test the error handling with invalid data
        mock_data = [("00:00:01.123", "invalid_data")]
        result = self.processor.extract_car_telemetry(mock_data)
        self.assertEqual(len(result), 0)  # Should return an empty list for invalid data
    
    def test_get_raw_file_path(self):
        """Test the get_raw_file_path method."""
        expected_path = self.processor.raw_dir / "test_race" / "test_session" / "CarData.z.jsonStream"
        result = self.processor.get_raw_file_path("test_race", "test_session", "CarData.z")
        self.assertEqual(result, expected_path)


if __name__ == "__main__":
    unittest.main()