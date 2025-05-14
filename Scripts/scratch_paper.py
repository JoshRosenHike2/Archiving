import argparse
import os
import requests.exceptions

from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from thoughtspot_rest_api_v1 import *

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
USERNAME         = os.getenv('TS_USERNAME')
PASSWORD         = os.getenv('TS_PASSWORD')
SERVER_URL       = os.getenv('TS_SERVER_URL')
LOGICAL_TABLE_ID = os.getenv('TS_LOGICAL_TABLE_ID')

# --- Authenticate with ThoughtSpot ---
ts = TSRestApiV2(server_url=SERVER_URL)
try:
    print(f"Authenticating to {SERVER_URL} as {USERNAME}...")
    auth = ts.auth_token_full(username=USERNAME, password=PASSWORD, validity_time_in_sec=3600)
    ts.bearer_token = auth['token']
    print("Authentication successful.\n")
except requests.exceptions.HTTPError as e:
    print("Authentication failed.")
    print(e, e.response.content)
    exit(1)

# --- Action 1: Fetch ALL Models ---
def fetch_all_models(ts):
    search_request = {
        'metadata':               [{'type': 'LOGICAL_TABLE'}],
        'include_details':        True,
        'include_dependent_objects': False,
        'record_offset':          0,
        'record_size':            100000
    }
    models = ts.metadata_search(request=search_request)
    rows = []
    for m in models:
        h = m.get('metadata_header', {}) or {}
        rows.append({
            'GUID':       h.get('id'),
            'Name':       h.get('name'),
            'Author':     h.get('authorDisplayName'),
            'Created_ms': h.get('created')
        })
    df_all = pd.DataFrame(rows)
    df_all['Created_dt'] = pd.to_datetime(df_all['Created_ms'], unit='ms', errors='coerce')
    return df_all

# --- Action 2: Filter by Age ---
def filter_old_models(df, days_old):
    cutoff = datetime.now() - timedelta(days=days_old)
    df_filtered = df[df['Created_dt'] < cutoff].copy()
    return df_filtered


# Action 3: Check to see if those models have any real responses in the last X days (Search data API) if they do discard them from the list


# --- Action 4: Fetch Dependents for each model ---
def fetch_dependents(ts, model_guid, max_deps=1000):
    search_request = {
        'dependent_object_version':      'V1',
        'include_auto_created_objects':  False,
        'include_dependent_objects':     True,
        'dependent_objects_record_size': max_deps,
        'include_headers':               True,
        'include_details':               False,
        'record_offset':                 0,
        'record_size':                   1,
        'metadata': [
            {'type': 'LOGICAL_TABLE', 'identifier': model_guid}
        ]
    }
    response = ts.metadata_search(request=search_request)
    if not response:
        return []
    entry = response[0]
    deps_map = entry.get('dependent_objects', {}).get(model_guid, {})
    return [hdr['id'] for headers in deps_map.values() for hdr in headers]

# --- Action 5: Exclude Models with Recent Dependency Activity ---
def exclude_active_models(ts, df, days_old, logical_table_id, record_size=1):
    cutoff = datetime.now() - timedelta(days=days_old)
    valid = []
    for _, row in df.iterrows():
        deps = row.get('Dependent_GUIDs', [])
        # If no dependencies, include the model
        if not deps:
            valid.append(row['GUID'])
            continue
        skip = False
        for dep in deps:
            req = {
                'query_string': (
                    f"[Answer Book GUID] = '{dep}' "
                    f"count [Impressions] [Timestamp].'last {days_old} days' "
                    f"max [Timestamp]"
                ),
                'logical_table_identifier': logical_table_id,
                'data_format':               'COMPACT',
                'record_offset':             0,
                'record_size':               record_size
            }
            try:
                resp = ts.search_data(request=req)
                contents = resp[0].get('contents', []) if resp else []
                if contents and contents[0].get('data_rows'):
                    skip = True
                    break
            except Exception:
                skip = True
                break
        if not skip:
            valid.append(row['GUID'])
    return valid

# --- Main Execution ---
if __name__ == '__main__':
    # 1) All imported models
    df_all = fetch_all_models(ts)
    print('1) All fetched models:')
    print(df_all, '\n')

    # 2) Models passing Action 1 & 2
    df_pass12 = filter_old_models(df_all, args.days)
    print(f'2) Models older than {args.days} days:')
    print(df_pass12, '\n')

    # 3) Models + their dependencies
    df_pass12_deps = df_pass12.copy()
    df_pass12_deps['Dependent_GUIDs'] = df_pass12_deps['GUID'].apply(
        lambda g: fetch_dependents(ts, g, max_deps=5000)
    )
    print('3) Models with their dependencies:')
    print(df_pass12_deps, '\n')

    # 4) Models with dependencies AND no recent dependency impressions
    clean_guids = exclude_active_models(
        ts, df_pass12_deps, args.days, LOGICAL_TABLE_ID
    )
    df_final = df_pass12_deps[df_pass12_deps['GUID'].isin(clean_guids)].copy()
    print(f'4) Models older than {args.days} days whose dependencies have NO recent impressions: {len(df_final)}')
    print(df_final)
