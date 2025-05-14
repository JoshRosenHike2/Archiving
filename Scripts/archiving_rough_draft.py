import argparse
import os
import requests.exceptions
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from thoughtspot_rest_api_v1 import TSRestApiV2

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description='Archive ThoughtSpot models based on age and usage.')
parser.add_argument('--days', type=int, default=90, help='Only consider models older than this number of days (default: 90)')
parser.add_argument('--include-dependents', action='store_true', help='Include dependent objects in metadata search')
parser.add_argument('--env-file', type=str, default='.env', help='Path to .env file')
args = parser.parse_args()

# --- Environment Setup ---
load_dotenv(dotenv_path=args.env_file)
USERNAME = os.getenv('TS_USERNAME')
PASSWORD = os.getenv('TS_PASSWORD')
SERVER_URL = os.getenv('TS_SERVER_URL')

# --- Authenticate with ThoughtSpot ---
ts = TSRestApiV2(server_url=SERVER_URL)

try:
    print(f"Authenticating to {SERVER_URL} as {USERNAME}...")
    auth_token_response = ts.auth_token_full(
        username=USERNAME,
        password=PASSWORD,
        validity_time_in_sec=3600
    )
    ts.bearer_token = auth_token_response['token']
    print("Authentication successful.")
except requests.exceptions.HTTPError as e:
    print("Authentication failed.")
    print(e)
    print(e.response.content)
    exit(1)












# --- Action 1 & 2: Fetch and Filter Models ---




def fetch_and_filter_models(ts, days_old: 10, include_dependents: True):

    search_request = {
        'metadata': [{'type': 'LOGICAL_TABLE'}],
        'include_details': True,
        'include_dependent_objects': include_dependents,
        'record_offset': 0,
        'record_size': 100000
    }

    models = ts.metadata_search(request=search_request)
    print(f"Retrieved {len(models)} models.")

    flat_rows = []
    for model in models:
        header = model.get('metadata_header') or {}
        flat_rows.append({
            'GUID': header.get('id'),
            'Name': header.get('name'),
            'Author': header.get('authorDisplayName'),
            'Created': header.get('created')
        })

    df = pd.DataFrame(flat_rows)
    df['Created_dt'] = pd.to_datetime(df['Created'], unit='ms', errors='coerce')


    print("\nRetrieved Models (Preview):")
    print(df[['Name', 'GUID', 'Author', 'Created_dt']].head(100))

    cutoff_date = datetime.now() - timedelta(days=days_old)
    df_filtered = df[df['Created_dt'] < cutoff_date].copy()
    df_filtered['Status'] = 'passed_action_1_&_2'

    print("\nModels That Passed Action 1 & 2:")
    print(df_filtered[['Name', 'GUID', 'Author', 'Created_dt', 'Status']].head(100))

    return df_filtered
    
# --- Execute Action 1 & 2 ---
df_action1_passed = fetch_and_filter_models(
    ts=ts,
    days_old=args.days,
    include_dependents=args.include_dependents
)



## Action 3: Check to see if those models have any real responses in the last X days (Search data API) if they do discard them from the list






## Action 4: For each model GUID Retrieve list of dependents (Might already have this from Action 1)







## Action 5: Check to see if there was any activity on those dependents (Liveboards / Answers) in the last X days If there was discard GUID from List

## Action 6: Check to see if there are alerts set on any dependencies? (Export TML API) If there is Discard GUID from List

## Action 7: For remaining GUIDS, find and store all Ownership and sharing details for each model and dependents 

## Action 8: Export all data to Archive

