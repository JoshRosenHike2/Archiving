## Initial Setup: Import neccesary libraries and set up environment + Authentication to TS

import argparse
import os
import requests.exceptions
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from thoughtspot_rest_api_v1 import TSRestApiV2

# --- argparse ---
parser = argparse.ArgumentParser(description='Archive ThoughtSpot models based on age and usage.')
parser.add_argument('--days', type=int, default=90, help='Only archive models older than this number of days (default: 90)')
parser.add_argument('--include-dependents', action='store_true', help='Include dependent objects in metadata search')
parser.add_argument('--env-file', type=str, default='.env', help='Path to .env file')
args = parser.parse_args()

# --- Load env ---
load_dotenv(dotenv_path=args.env_file)
USERNAME = os.getenv('TS_USERNAME')
PASSWORD = os.getenv('TS_PASSWORD')
SERVER_URL = os.getenv('TS_SERVER_URL')



ts = TSRestApiV2(server_url=SERVER_URL)

try:
    print(f"🔐 Authenticating to ThoughtSpot at {SERVER_URL} as {USERNAME}...")
    auth_token_response = ts.auth_token_full(
        username=USERNAME,
        password=PASSWORD,
        validity_time_in_sec=3600  # 1 hour token
    )
    ts.bearer_token = auth_token_response['token']
    print("✅ Authentication successful.")
except requests.exceptions.HTTPError as e:
    print("❌ Authentication failed:")
    print(e)
    print(e.response.content)
    exit()



## Action 1: Search metadata API for all models & dependencies

def fetch_old_models(ts, older_than_days=90, include_dependents=True):
    """
    Uses /metadata/search to fetch models older than X days.
    Returns all LOGICAL_TABLEs with optional dependents.
    """
    print(f"📅 Fetching models older than {older_than_days} days...")

    # Calculate timestamp cutoff
    cutoff_ts_ms = int((datetime.now() - timedelta(days=older_than_days)).timestamp() * 1000)

    # Build the metadata search request
    search_request = {
    'metadata': [
        { 'type': 'LOGICAL_TABLE' }
    ],
    'filter': {
        'operator': 'LT',
        'operands': [
            {'column': 'created'},
            {'value': cutoff_ts_ms}
        ]
    },
    'include_details': True,  # ← this is the key you're missing
    'include_dependent_objects': include_dependents,
    'record_offset': 0,
    'record_size': 100000
}


    result = ts.metadata_search(request=search_request)

    print(f"✅ Retrieved {len(result)} models.")
    return result




# --- Fetch the models from metadata_search ---
old_models = fetch_old_models(ts, older_than_days=0, include_dependents=True)

# --- Flatten and process the results ---
flat_rows = []
for model in old_models:
    header = model.get('metadata_header') or {}
    flat_rows.append({
        'id': header.get('id'),
        'name': header.get('name'),
        'author': header.get('authorDisplayName'),
        'created': header.get('created')
    })

df_models = pd.DataFrame(flat_rows)
df_models['created_dt'] = pd.to_datetime(df_models['created'], unit='ms')
print(df_models[['name', 'id', 'author', 'created_dt']].head())



## Action 2: Filter list of models to show only those that have been created > X days and add GUID to List 

## Action 3: Check to see if those models have any real responses in the last X days (Search data API) if they do discard them from the list

## Action 4: For each model GUID Retrieve list of dependents (Might already have this from Action 1)

## Action 5: Check to see if there was any activity on those dependents (Liveboards / Answers) in the last X days If there was discard GUID from List

## Action 6: Check to see if there are alerts set on any dependencies? (Export TML API) If there is Discard GUID from List

## Action 7: For remaining GUIDS, find and store all Ownership and sharing details for each model and dependents 

## Action 8: Export all data to Archive

