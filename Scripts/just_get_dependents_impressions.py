import os
import argparse
import pandas as pd
from dotenv import load_dotenv
from thoughtspot_rest_api_v1 import TSRestApiV2

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Fetch dependents and impressions for a single model")
parser.add_argument('--model-guid', required=True, help='GUID of the model (LOGICAL_TABLE) to inspect')
parser.add_argument('--days', type=int, default=90, help='Number of days to look back for impressions')
parser.add_argument('--env-file', type=str, default='.env', help='Path to .env file')
args = parser.parse_args()

# --- Load Environment Variables ---
load_dotenv(args.env_file)
USERNAME = os.getenv('TS_USERNAME')
PASSWORD = os.getenv('TS_PASSWORD')
SERVER_URL = os.getenv('TS_SERVER_URL')
LOGICAL_TABLE_ID = os.getenv('TS_LOGICAL_TABLE_ID')  # This is your usage stats logical table

# --- Authenticate with ThoughtSpot ---
ts = TSRestApiV2(server_url=SERVER_URL)
print(f"Authenticating to {SERVER_URL} as {USERNAME}...")
auth = ts.auth_token_full(username=USERNAME, password=PASSWORD)
ts.bearer_token = auth['token']
print("Authenticated.\n")

# --- Step 1: Fetch Dependent Objects ---
def fetch_dependents(ts, model_guid, max_deps=1000):
    search_request = {
        'dependent_object_version': 'V1',
        'include_auto_created_objects': False,
        'include_dependent_objects': True,
        'dependent_objects_record_size': max_deps,
        'include_headers': True,
        'include_details': False,
        'record_offset': 0,
        'record_size': 1,
        'metadata': [{'type': 'LOGICAL_TABLE', 'identifier': model_guid}]
    }
    response = ts.metadata_search(request=search_request)
    if not response:
        raise ValueError(f"No metadata found for model {model_guid}")

    deps_map = response[0].get('dependent_objects', {}).get(model_guid, {})
    return [hdr for headers in deps_map.values() for hdr in headers]

# --- Step 2: Fetch Impression Counts for Each Dependent ---
def fetch_impressions(ts, dependents, days, logical_table_id, model_guid):
    rows = []
    for obj in dependents:
        dep_guid = obj['id']
        dep_name = obj.get('name', 'Unknown')

        req = {
            'query_string': (
                f"[Answer Book GUID] = '{dep_guid}' "
                f"count [Impressions] [Timestamp].'last {days} days' "
                f"max [Timestamp]"
            ),
            'logical_table_identifier': logical_table_id,
            'data_format': 'COMPACT',
            'record_offset': 0,
            'record_size': 1
        }

        resp = ts.searchdata(request=req)
        contents = resp.get("contents", [])
        
        if contents and contents[0].get("data_rows"):
            column_names = contents[0].get("column_names", [])
            data_rows = contents[0]["data_rows"]
            try:
                imp_index = column_names.index("Number of Impressions")
                imps = data_rows[0][imp_index]
            except (ValueError, IndexError):
                imps = 0
        else:
            imps = 0

        rows.append({
            'Model_GUID': model_guid,
            'Dependent_GUID': dep_guid,
            'Dependent_Name': dep_name,
            f'Impressions_Last_{days}d': imps
        })

    return pd.DataFrame(rows)

# --- Main Run Block ---
dependents = fetch_dependents(ts, args.model_guid)
print(f"\n✅ Found {len(dependents)} dependent(s) for model {args.model_guid}.\n")

if dependents:
    df = fetch_impressions(ts, dependents, args.days, LOGICAL_TABLE_ID, args.model_guid)

    if not df.empty:
        print("📊 Dependent Impression Summary:\n")
        print(df.to_string(index=False))
    else:
        print("⚠️ No impression data returned for any of the dependents.")
else:
    print("ℹ️ This model has no dependent objects.")
