"""
Collector for retrieving F1 race data from the official F1 API.
"""
import asyncio
import aiohttp
from pathlib import Path

import config
from src.collectors.base_collector import BaseCollector


class RaceDataCollector(BaseCollector):
    """
    Collector for retrieving F1 race session data from the official F1 API.
    """
    
    async def get_session_topics(self, session, session_url):
        """
        Get the available topics for a session.
        
        Args:
            session: The aiohttp session to use for the request
            session_url: The URL of the session
            
        Returns:
            list: The available topics for the session
        """
        exists, content = await self.check_url_exists(session, f"{session_url}/Index.json")
        
        if not exists or not content:
            print(f"Could not access {session_url}/Index.json")
            return []
        
        session_index = self.fix_utf8_bom(content)
        
        if not session_index or "Feeds" not in session_index:
            return []
        
        topics = []
        for feed_key, feed_info in session_index["Feeds"].items():
            if "StreamPath" in feed_info:
                stream_path = feed_info["StreamPath"]
                if stream_path.endswith(".jsonStream"):
                    topic = stream_path[:-11]  # Remove '.jsonStream'
                    topics.append(topic)
        
        return topics
    
    async def collect_topic_data(self, session, session_url, topic, race_name, session_name):
        """
        Collect data for a specific topic.
        
        Args:
            session: The aiohttp session to use for the request
            session_url: The URL of the session
            topic: The topic to collect
            race_name: The name of the race
            session_name: The name of the session
            
        Returns:
            Path: The path where the data was saved or None if collection failed
        """
        print(f"Collecting data for topic: {topic}")
        
        # Create the URL for the topic
        topic_url = f"{session_url}/{topic}.jsonStream"
        
        # Check if the topic exists
        exists, content = await self.check_url_exists(session, topic_url)
        
        if not exists or not content:
            print(f"Could not access {topic_url}")
            return None
        
        # Save the raw data
        file_path = self.save_raw_data(
            content, 
            race_name, 
            session_name, 
            f"{topic}.jsonStream"
        )
        
        return file_path
    
    async def collect_session_data(self, year, race_path, session_name, topics=None):
        """
        Collect data for a specific session.
        
        Args:
            year: The year of the race
            race_path: The path/name of the race
            session_name: The name of the session
            topics: The topics to collect (if None, all important topics will be collected)
            
        Returns:
            dict: A dictionary of the collected topics and their file paths
        """
        # Format the session URL
        race_name = race_path
        session_url = f"{self.base_url}/{year}/{race_path}/{session_name}"
        
        print(f"Collecting data for session: {session_url}")
        
        collected_data = {}
        
        # Create a session for HTTP requests
        async with aiohttp.ClientSession() as session:
            # Get the available topics for the session
            available_topics = await self.get_session_topics(session, session_url)
            
            if not available_topics:
                print(f"No topics found for session: {session_url}")
                return collected_data
            
            print(f"Found {len(available_topics)} topics")
            
            # Filter topics if specified
            selected_topics = []
            if topics:
                selected_topics = [t for t in topics if t in available_topics]
                if not selected_topics:
                    print("None of the specified topics were found")
                    # Fall back to important topics
                    selected_topics = [t for t in config.IMPORTANT_TOPICS if t in available_topics]
            else:
                # Use important topics by default
                selected_topics = [t for t in config.IMPORTANT_TOPICS if t in available_topics]
            
            if not selected_topics:
                # If no important topics were found, use the first few available topics
                selected_topics = available_topics[:3]
            
            print(f"Collecting data for topics: {', '.join(selected_topics)}")
            
            # Collect data for each topic in parallel
            tasks = []
            for topic in selected_topics:
                task = self.collect_topic_data(session, session_url, topic, race_name, session_name)
                tasks.append(task)
            
            # Run the tasks concurrently with a limit
            results = await asyncio.gather(*tasks)
            
            # Store the results
            for topic, file_path in zip(selected_topics, results):
                if file_path:
                    collected_data[topic] = file_path
        
        return collected_data