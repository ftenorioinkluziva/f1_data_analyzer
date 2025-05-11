"""
Collector for retrieving historical F1 race data from the official F1 API.
"""
import asyncio
import aiohttp
import json
from pathlib import Path

import config
from src.collectors.base_collector import BaseCollector
from src.utils.file_utils import ensure_directory


class HistoricalCollector(BaseCollector):
    """
    Collector for retrieving historical F1 data across multiple seasons and races.
    """
    
    async def find_available_years(self):
        """
        Find available years in the F1 data API.
        
        Returns:
            list: A list of tuples containing (year, index content)
        """
        available_years = []
        current_year = 2025  # Start with the current year
        
        print("Looking for available years...")
        
        # Create a session for HTTP requests
        async with aiohttp.ClientSession() as session:
            # Check the last 10 years
            for year in range(current_year, current_year - 10, -1):
                year_url = f"{self.base_url}/{year}"
                exists, content = await self.check_url_exists(session, f"{year_url}/Index.json")
                
                if exists:
                    print(f"✅ Found year: {year}")
                    available_years.append((year, content))
                else:
                    print(f"❌ Year not found: {year}")
        
        return available_years
    
    async def get_year_meetings(self, year, content):
        """
        Extract meeting information from a year's index.
        
        Args:
            year: The year to extract meetings for
            content: The content of the year's index file
            
        Returns:
            list: A list of meeting information dictionaries
        """
        # Process the JSON content
        index_data = self.fix_utf8_bom(content)
        
        if not index_data:
            print(f"Could not process data for year {year}")
            return []
        
        # Save the year's index
        year_dir = self.raw_dir / f"indexes/{year}"
        ensure_directory(year_dir)
        
        with open(year_dir / f"index_{year}.json", "w") as f:
            json.dump(index_data, f, indent=2)
        
        print(f"Index for year {year} saved to {year_dir}/index_{year}.json")
        
        # Extract meeting information
        meetings = []
        
        if "Meetings" in index_data:
            for meeting in index_data["Meetings"]:
                meeting_key = meeting.get("Key")
                meeting_name = meeting.get("Name", "Unknown")
                meeting_official_name = meeting.get("OfficialName", meeting_name)
                meeting_country = meeting.get("Country", "Unknown")
                meeting_location = meeting.get("Location", "Unknown")
                meeting_path = meeting.get("Path", "")
                
                meeting_info = {
                    "key": meeting_key,
                    "name": meeting_name,
                    "official_name": meeting_official_name,
                    "country": meeting_country,
                    "location": meeting_location,
                    "path": meeting_path,
                    "sessions": []
                }
                
                # Extract session information
                if "Sessions" in meeting:
                    for session in meeting["Sessions"]:
                        session_key = session.get("Key")
                        session_name = session.get("Name", "Unknown")
                        session_path = session.get("Path", "")
                        session_date = session.get("StartDate", "Unknown")
                        
                        session_info = {
                            "key": session_key,
                            "name": session_name,
                            "path": session_path,
                            "date": session_date,
                            "available_topics": []
                        }
                        
                        meeting_info["sessions"].append(session_info)
                
                meetings.append(meeting_info)
        
        # Save the meeting information
        with open(year_dir / f"meetings_{year}.json", "w") as f:
            json.dump(meetings, f, indent=2)
        
        print(f"Meeting information saved to {year_dir}/meetings_{year}.json")
        
        return meetings
    
    async def collect_historical_data(self):
        """
        Collect historical F1 data across multiple seasons.
        
        Returns:
            dict: A dictionary mapping years to their meeting information
        """
        # Find available years
        available_years = await self.find_available_years()
        
        if not available_years:
            print("Could not find data for any recent year")
            return {}
        
        # Process each year
        year_data = {}
        
        for year, content in available_years:
            meetings = await self.get_year_meetings(year, content)
            year_data[year] = meetings
        
        return year_data
    
    async def collect_example_topic(self, session, session_path):
        """
        Collect an example topic from a session for demonstration purposes.
        
        Args:
            session: The aiohttp session to use for the request
            session_path: The path of the session
            
        Returns:
            dict: Information about the collected example
        """
        session_url = f"{self.base_url}/{session_path}"
        exists, session_content = await self.check_url_exists(session, f"{session_url}/Index.json")
        
        if not exists or not session_content:
            print(f"Could not access {session_url}/Index.json")
            return {}
        
        # Process the session index
        session_index = self.fix_utf8_bom(session_content)
        
        if not session_index or "Feeds" not in session_index:
            return {}
        
        # Get available topics
        topics = []
        
        for feed_key, feed_info in session_index["Feeds"].items():
            if "StreamPath" in feed_info:
                stream_path = feed_info["StreamPath"]
                if stream_path.endswith(".jsonStream"):
                    topic = stream_path[:-11]  # Remove '.jsonStream'
                    topics.append(topic)
        
        if not topics:
            return {}
        
        print(f"Found {len(topics)} topics for session: {session_path}")
        print(f"Topics: {', '.join(topics[:5])}..." if len(topics) > 5 else f"Topics: {', '.join(topics)}")
        
        # Choose an example topic (prioritize important topics)
        example_topic = None
        
        for topic in config.IMPORTANT_TOPICS:
            if topic in topics:
                example_topic = topic
                break
        
        if not example_topic and topics:
            example_topic = topics[0]
        
        if not example_topic:
            return {}
        
        print(f"Collecting example data for topic: {example_topic}")
        
        # Get the topic data
        topic_url = f"{session_url}/{example_topic}.jsonStream"
        exists, topic_content = await self.check_url_exists(session, topic_url)
        
        if not exists or not topic_content:
            return {}
        
        # Save the raw data to an examples directory
        example_dir = self.raw_dir / "examples"
        ensure_directory(example_dir)
        
        file_path = example_dir / f"{example_topic}_example.jsonStream"
        with open(file_path, "wb") as f:
            f.write(topic_content[:5000] if len(topic_content) > 5000 else topic_content)
        
        print(f"Example data saved to {file_path}")
        
        # Process a few lines to show what the data looks like
        try:
            lines = topic_content.decode('utf-8', errors='replace').split("\r\n")
            processed_data = []
            
            for i, line in enumerate(lines[:10]):  # Process only the first 10 lines
                if not line:
                    continue
                
                # Try to split by comma (assuming format is "time,data")
                parts = line.split(",", 1)
                if len(parts) == 2:
                    time_str, data_str = parts
                    processed_data.append({
                        "time": time_str,
                        "raw_data": data_str[:100] + "..." if len(data_str) > 100 else data_str
                    })
            
            # Save processed data
            processed_file = example_dir / f"{example_topic}_processed.json"
            with open(processed_file, "w") as f:
                json.dump(processed_data, f, indent=2)
            
            print(f"Processed data saved to {processed_file}")
            
            return {
                "topic": example_topic,
                "raw_file": str(file_path),
                "processed_file": str(processed_file),
                "sample_count": len(processed_data)
            }
            
        except Exception as e:
            print(f"Error processing topic lines: {str(e)}")
            return {}