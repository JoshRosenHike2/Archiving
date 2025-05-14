"""
Archive ThoughtSpot Models Script

This script identifies and filters ThoughtSpot models based on age and usage,
optionally including dependent objects. Configurable via CLI arguments and .env file.
"""

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

# --- Action 1: Fetch and Filter Models ---
def fetch_and_filter_models(ts, days_old: 90, include_dependents: True):
    """
    Retrieve and filter models older than `days_old`.
    """
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
            'id': header.get('id'),
            'name': header.get('name'),
            'author': header.get('authorDisplayName'),
            'created': header.get('created')
        })

    df = pd.DataFrame(flat_rows)
    df['created_dt'] = pd.to_datetime(df['created'], unit='ms', errors='coerce')

    print("\nRetrieved Models (Preview):")
    print(df[['name', 'id', 'author', 'created_dt']].head(100))

    cutoff_date = datetime.now() - timedelta(days=days_old)
    df_filtered = df[df['created_dt'] < cutoff_date].copy()
    df_filtered['status'] = 'passed_action_1'

    print("\nModels That Passed Action 1:")
    print(df_filtered[['name', 'id', 'author', 'created_dt', 'status']].head(100))

    return df_filtered


# --- Execute Action 1 ---
df_action1_passed = fetch_and_filter_models(
    ts=ts,
    days_old=args.days,
    include_dependents=args.include_dependents
)

# --- Future Steps ---
# TODO: Action 2 – Search data usage check
# TODO: Action 3 – Dependent object activity check
# TODO: Action 4 – Alert detection via TML
# TODO: Action 5 – Ownership and sharing metadata
# TODO: Action 6 – Archive export logic
