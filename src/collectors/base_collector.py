"""
Base Collector class with common functionality for all data collectors.
"""
import aiohttp
import json
from pathlib import Path

import config
from src.utils.file_utils import ensure_directory


class BaseCollector:
    """
    Base class for all data collectors with shared functionality.
    """
    
    def __init__(self):
        """Initialize the base collector."""
        self.base_url = config.BASE_URL
        self.raw_dir = config.RAW_DATA_DIR
    
    async def check_url_exists(self, session, url):
        """
        Check if a URL exists and return its content if it does.
        
        Args:
            session: The aiohttp session to use for the request
            url: The URL to check
            
        Returns:
            tuple: (exists, content) where exists is a boolean and content is the response content
        """
        try:
            print(f"Tentando acessar: {url}")
            async with session.get(url) as response:
                print(f"Status da resposta: {response.status}")
                if response.status == 200:
                    return True, await response.read()
                print(f"Erro ao acessar {url}: Status {response.status}")
                return False, None
        except Exception as e:
            print(f"Exceção ao acessar {url}: {str(e)}")
            return False, None
    
    def fix_utf8_bom(self, content):
        """
        Fix UTF-8 BOM issues in JSON content.
        
        Args:
            content: The content to fix
            
        Returns:
            dict: The parsed JSON data or None if parsing failed
        """
        try:
            # Decode the content considering the BOM
            text = content.decode('utf-8-sig')
            # Convert to JSON
            data = json.loads(text)
            return data
        except Exception as e:
            print(f"Error processing JSON: {str(e)}")
            return None
    
    def save_raw_data(self, content, race_name, session_name, file_name):
        """
        Save raw data to a file.
        
        Args:
            content: The content to save
            race_name: The name of the race
            session_name: The name of the session
            file_name: The name of the file to save
            
        Returns:
            Path: The path where the file was saved
        """
        # Create directory for the raw data
        raw_dir = self.raw_dir / race_name / session_name
        ensure_directory(raw_dir)
        
        # Save the file
        file_path = raw_dir / file_name
        with open(file_path, "wb") as f:
            f.write(content)
        
        print(f"Raw data saved to {file_path}")
        return file_path