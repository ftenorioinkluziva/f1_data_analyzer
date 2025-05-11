"""
Functions for decoding compressed F1 data formats.
"""
import base64
import zlib
import json


def decode_compressed_data(encoded_data):
    """
    Decode compressed data from Position.z or CarData.z.
    
    Args:
        encoded_data: The encoded data string
        
    Returns:
        dict: The decoded data as a dictionary, or None if decoding failed
    """
    try:
        # Remove quotes from the beginning and end if present
        if encoded_data.startswith('"') and encoded_data.endswith('"'):
            encoded_data = encoded_data[1:-1]
        
        # Decode base64 and decompress zlib
        decoded_data = zlib.decompress(base64.b64decode(encoded_data), -zlib.MAX_WBITS)
        
        # Convert to JSON
        return json.loads(decoded_data)
    except Exception as e:
        print(f"Error decoding compressed data: {str(e)}")
        return None


def decode_json_stream(text):
    """
    Extract timestamped data from a JSON stream.
    
    Args:
        text: The JSON stream text
        
    Returns:
        list: List of tuples containing (timestamp, data)
    """
    import re
    
    # Extract timestamp and data using regex
    pattern = r'(\d{2}:\d{2}:\d{2}\.\d{3})(.*?)(?=\d{2}:\d{2}:\d{2}\.\d{3}|$)'
    matches = re.findall(pattern, text, re.DOTALL)
    
    return matches


def fix_utf8_bom(content):
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