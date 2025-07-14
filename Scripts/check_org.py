# =========================================
# Import required libraries
# =========================================
import argparse  # For parsing command-line arguments
import os  # For accessing environment variables and file paths
import requests  # For making HTTP requests
import requests.exceptions  # To handle HTTP request exceptions
from datetime import datetime, timedelta  # For working with dates and time windows
import pandas as pd  # For handling tabular data
from dotenv import load_dotenv  # For loading environment variables from a .env file
from thoughtspot_rest_api_v1 import *  # Import all functions from the ThoughtSpot API wrapper

# =========================================
# Parse command-line arguments
# =========================================
parser = argparse.ArgumentParser(description='Check ThoughtSpot current user session information.')  # Create a parser object with a description
parser.add_argument('--days', type=int, default=90, help='Minimum model age in days (default: 90)')  # Not used, kept for compatibility
parser.add_argument('--lookback-days', type=int, default=90, help='How far back to look for impressions (default: 90)')  # Not used
parser.add_argument('--imp-threshold', type=int, default=1, help='Max allowed impressions (default: 1)')  # Not used
parser.add_argument('--env-file', type=str, default='.env', help='Path to .env file')  # Argument for path to .env file
args = parser.parse_args()  # Parse the arguments and store them in a variable

# =========================================
# Load environment variables
# =========================================
load_dotenv(dotenv_path=args.env_file)  # Load environment variables from the given .env file
USERNAME = os.getenv('TS_USERNAME')  # Get the ThoughtSpot username
PASSWORD = os.getenv('TS_PASSWORD')  # Get the ThoughtSpot password
SERVER_URL = os.getenv('TS_SERVER_URL')  # Get the ThoughtSpot server URL

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
# Get current user session info
# =========================================
import json  # Add this import at the top with the others

# =========================================
# Get current user session info
# =========================================
def get_user_session_info():
    url = f"{SERVER_URL}/api/rest/2.0/auth/session/user"
    headers = {
        'Authorization': f'Bearer {ts.bearer_token}',
        'Accept': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4XX/5XX responses
        user_info = response.json()
        print("✅ User session info:")
        print(json.dumps(user_info, indent=2))  # Pretty-print raw JSON
    except requests.exceptions.RequestException as e:
        print("❌ Failed to retrieve user session info:", e)


# =========================================
# Main execution
# =========================================
if __name__ == "__main__":
    get_user_session_info()
