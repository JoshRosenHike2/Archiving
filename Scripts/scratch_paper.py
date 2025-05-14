import argparse
import os
import requests.exceptions
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from thoughtspot_rest_api_v1 import TSRestApiV2

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description='Archive ThoughtSpot models based on age and usage.')
parser.add_argument('--days', type=int, default=90,
                    help='Only consider models older than this number of days (default: 90)')
parser.add_argument('--include-dependents', action='store_true',
                    help='Include dependent objects in metadata search')
parser.add_argument('--env-file', type=str, default='.env',
                    help='Path to .env file')
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
    auth = ts.auth_token_full(username=USERNAME, password=PASSWORD, validity_time_in_sec=3600)
    ts.bearer_token = auth['token']
    print("Authentication successful.")
except requests.exceptions.HTTPError as e:
    print("Authentication failed.")
    print(e, e.response.content)
    exit(1)

# --- Action 1 & 2: Fetch and Filter Models ---
def fetch_and_filter_models(ts, days_old, include_dependents):
    search_request = {
        'metadata': [{'type': 'LOGICAL_TABLE'}],
        'include_details': True,
        'include_dependent_objects': include_dependents,
        'record_offset': 0,
        'record_size': 100000
    }
    models = ts.metadata_search(request=search_request)
    print(f"Retrieved {len(models)} models.")

    rows = []
    for m in models:
        h = m.get('metadata_header', {}) or {}
        rows.append({
            'GUID': h.get('id'),
            'Name': h.get('name'),
            'Author': h.get('authorDisplayName'),
            'Created': h.get('created')
        })

    df = pd.DataFrame(rows)
    df['Created_dt'] = pd.to_datetime(df['Created'], unit='ms', errors='coerce')

    print("\nRetrieved Models (Preview):")
    print(df[['Name','GUID','Author','Created_dt']].head(100))

    cutoff = datetime.now() - timedelta(days=days_old)
    df_filtered = df[df['Created_dt'] < cutoff].copy()
    df_filtered['Status'] = 'passed_action_1_&_2'

    print("\nModels That Passed Action 1 & 2:")
    print(df_filtered[['Name','GUID','Author','Created_dt','Status']].head(100))
    return df_filtered

df_action1_passed = fetch_and_filter_models(
    ts=ts,
    days_old=args.days,
    include_dependents=args.include_dependents
)

# --- Action 3: (placeholder for usage check) ---
def action3_usage_check(df, days_old):
    # TODO: implement Search Data API usage filter
    return df

df_after_action3 = action3_usage_check(df_action1_passed, args.days)

# --- Action 4: Fetch Dependents ---
def fetch_dependents(ts, model_guid, max_deps=1000):
    """
    Given a model GUID, fetch all dependent objects for that logical table.
    Returns a dict: { dependent_type: [ header_dict, ... ], ... }
    """
    search_request = {
        # Match exactly what the Playground cURL used:
        "dependent_object_version": "V1",
        "include_auto_created_objects": False,
        "include_dependent_objects": True,
        "dependent_objects_record_size": max_deps,
        "include_headers": True,
        "include_details": False,
        "record_offset": 0,
        "record_size": 1,
        "metadata": [
            {
                "type": "LOGICAL_TABLE",
                "identifier": model_guid
            }
        ]
    }

    response = ts.metadata_search(request=search_request)
    if not response:
        return {}

    entry = response[0]
    # pull out the map specifically for this model_guid
    return entry.get("dependent_objects", {}).get(model_guid, {})

# Loop through models and collect dependent GUIDs
dependent_guids_list = []
for _, row in df_after_action3.iterrows():
    guid = row['GUID']
    deps_map = fetch_dependents(ts, guid, max_deps=5000)
    guids = [hdr['id'] for headers in deps_map.values() for hdr in headers]
    dependent_guids_list.append(guids)

df_after_action3['Dependent_GUIDs'] = dependent_guids_list

print("\nModels and their dependent GUIDs:")
print(df_after_action3[['Name','GUID','Dependent_GUIDs']].head(20))

# --- Next Steps ---
# Action 5: Filter out dependents with recent activity  
# Action 6: Check alerts via TML  
# Action 7: Gather ownership/sharing metadata  
# Action 8: Export to archive  
