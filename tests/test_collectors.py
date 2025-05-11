"""
Unit tests for the collectors module.
"""
import unittest
import asyncio
from pathlib import Path
import sys
import os
import aiohttp

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.collectors.race_data_collector import RaceDataCollector
from src.collectors.historical_collector import HistoricalCollector


class TestRaceDataCollector(unittest.TestCase):
    """Test the RaceDataCollector class."""
    
    def setUp(self):
        """Set up the test environment."""
        self.collector = RaceDataCollector()
        self.test_year = "2024"
        self.test_race = "Miami_Grand_Prix"
        self.test_session = "Race"
    
    async def async_test_get_session_topics(self):
        """Test the get_session_topics method."""
        
        async with aiohttp.ClientSession() as session:
            session_url = f"{self.collector.base_url}/{self.test_year}/{self.test_race}/{self.test_session}"
            topics = await self.collector.get_session_topics(session, session_url)
            self.assertIsInstance(topics, list)
            print(f"Found {len(topics)} topics")    
    def test_get_session_topics(self):
        """Run the async test for get_session_topics."""
        asyncio.run(self.async_test_get_session_topics())
    
    def test_save_raw_data(self):
        """Test the save_raw_data method."""
        test_content = b"Test content"
        file_path = self.collector.save_raw_data(
            test_content,
            "test_race",
            "test_session",
            "test_file.txt"
        )
        self.assertTrue(file_path.exists())
        with open(file_path, "rb") as f:
            content = f.read()
            self.assertEqual(content, test_content)
        file_path.unlink()  # Clean up


if __name__ == "__main__":
    unittest.main()