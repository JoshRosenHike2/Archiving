# =========================================
# Import required libraries
# =========================================
import argparse  # For parsing command-line arguments
import os  # For accessing environment variables and file paths
import requests.exceptions  # To handle HTTP request exceptions
from datetime import datetime, timedelta  # For working with dates and time windows
import pandas as pd  # For handling tabular data
from dotenv import load_dotenv  # For loading environment variables from a .env file
from thoughtspot_rest_api_v1 import *  # Import all functions from the ThoughtSpot API wrapper

# =========================================
# Parse command-line arguments
# =========================================
parser = argparse.ArgumentParser(description='Archive ThoughtSpot models based on age and usage.')  # Create a parser object with a description
parser.add_argument('--days', type=int, default=90, help='Minimum model age in days (default: 90)')  # Argument for model age filter
parser.add_argument('--lookback-days', type=int, default=90, help='How far back to look for impressions (default: 90)')  # Argument for impression window
parser.add_argument('--imp-threshold', type=int, default=1, help='Max allowed impressions (default: 1)')  # Argument for impression threshold
parser.add_argument('--env-file', type=str, default='.env', help='Path to .env file')  # Argument for path to .env file
args = parser.parse_args()  # Parse the arguments and store them in a variable

# =========================================
# Load environment variables
# =========================================
load_dotenv(dotenv_path=args.env_file)  # Load environment variables from the given .env file
USERNAME = os.getenv('TS_USERNAME')  # Get the ThoughtSpot username
PASSWORD = os.getenv('TS_PASSWORD')  # Get the ThoughtSpot password
SERVER_URL = os.getenv('TS_SERVER_URL')  # Get the ThoughtSpot server URL
LOGICAL_TABLE_ID = os.getenv('TS_LOGICAL_TABLE_ID')  # Get the GUID for the logical table used in queries
SAMPLE_GUID = os.getenv('TS_SAMPLE_GUID')  # Get the GUID for the hardcoded sample export (Step 9)

# =========================================
# Authenticate with ThoughtSpot
# =========================================
ts = TSRestApiV2(server_url=SERVER_URL)  # Create a ThoughtSpot API client instance
try:
    token = ts.auth_token_full(username=USERNAME, password=PASSWORD, validity_time_in_sec=3600)  # Authenticate and get token
    ts.bearer_token = token['token']  # Set the bearer token for future requests
except requests.exceptions.HTTPError as e:  # If authentication fails...
    print("Authentication failed:", e.response.content)  # Print error message
    exit(1)  # Exit the script

# =========================================
# Step 1: Fetch all models
# =========================================
def get_all_models():
    request = {
        'metadata': [{'type': 'LOGICAL_TABLE'}],  # Request all logical table metadata
        'include_details': True,  # Include detailed metadata
        'record_offset': 0,  # Start from the first record
        'record_size': 100000  # Set a large record size to fetch all
    }
    result = ts.metadata_search(request=request)  # Send the metadata search request
    rows = []  # List to store model info
    for model in result:  # Loop through each returned model
        meta = model.get('metadata_header', {})  # Get metadata header
        rows.append({
            'Model_ID': meta.get('id'),  # Store model GUID
            'Name': meta.get('name'),  # Store model name
            'Author': meta.get('authorDisplayName'),  # Store model author's name
            'Created_ms': meta.get('created')  # Store model creation time in milliseconds
        })
    df = pd.DataFrame(rows)  # Convert to DataFrame
    df['Created_dt'] = pd.to_datetime(df['Created_ms'], unit='ms', errors='coerce')  # Convert ms to datetime
    print(df.head())
    return df  # Return the DataFrame
    

if __name__ == "__main__":
    df = get_all_models()
    print(df[['Model_ID', 'Name', 'Author', 'Created_dt']].to_string(index=False))
