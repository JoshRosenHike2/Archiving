import argparse
import os
import requests.exceptions
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
from thoughtspot_rest_api_v1 import *

# --- Parse CLI arguments ---
parser = argparse.ArgumentParser(description='Archive ThoughtSpot models based on age and usage.')
parser.add_argument('--days', type=int, default=90, help='Minimum model age in days to be considered (default: 90)')
parser.add_argument('--lookback-days', type=int, default=90, help='How far back to calculate total impressions (default: 90)')
parser.add_argument('--imp-threshold', type=int, default=1, help='Maximum total impressions allowed across all dependents (default: 1)')
parser.add_argument('--env-file', type=str, default='.env', help='Path to .env file')
args = parser.parse_args()

# --- Load environment variables ---
load_dotenv(dotenv_path=args.env_file)
USERNAME = os.getenv('TS_USERNAME')
PASSWORD = os.getenv('TS_PASSWORD')
SERVER_URL = os.getenv('TS_SERVER_URL')
LOGICAL_TABLE_ID = os.getenv('TS_LOGICAL_TABLE_ID')

# --- Authenticate with ThoughtSpot ---
ts = TSRestApiV2(server_url=SERVER_URL)
try:
    auth = ts.auth_token_full(username=USERNAME, password=PASSWORD, validity_time_in_sec=3600)
    ts.bearer_token = auth['token']
except requests.exceptions.HTTPError as e:
    print("Authentication failed.", e, e.response.content)
    exit(1)

# --- Step 1: Fetch all models ---
def get_all_models():
    request = {
        'metadata': [{'type': 'LOGICAL_TABLE'}],
        'include_details': True,
        'record_offset': 0,
        'record_size': 100000
    }
    result = ts.metadata_search(request=request)
    rows = []
    for model in result:
        meta = model.get('metadata_header', {})
        rows.append({
            'Model_ID': meta.get('id'),
            'Name': meta.get('name'),
            'Author': meta.get('authorDisplayName'),
            'Created_ms': meta.get('created')
        })
    df = pd.DataFrame(rows)
    df['Created_dt'] = pd.to_datetime(df['Created_ms'], unit='ms', errors='coerce')
    return df

# --- Step 2: Filter old models ---
def filter_old_models(df, min_age_days):
    cutoff = datetime.now() - timedelta(days=min_age_days)
    return df[df['Created_dt'] < cutoff].copy()

# --- Step 3: Get dependent GUIDs ---
def get_dependents(model_id):
    request = {
        'metadata': [{'type': 'LOGICAL_TABLE', 'identifier': model_id}],
        'include_dependent_objects': True,
        'include_headers': True,
        'record_offset': 0,
        'record_size': 1
    }
    res = ts.metadata_search(request=request)
    if not res:
        return []
    dependents = res[0].get('dependent_objects', {}).get(model_id, {})
    return [obj['id'] for lst in dependents.values() for obj in lst]

# --- Step 4: Get total impressions for given dependents ---
def get_total_impressions(dependents, days_window):
    total = 0
    for guid in dependents:
        query_string = (
            f"[Answer Book GUID] = '{guid}' "
            f"count [Impressions] [Timestamp].'last {days_window} days' max [Timestamp]"
        )
        request = {
            'query_string': query_string,
            'logical_table_identifier': LOGICAL_TABLE_ID,
            'data_format': 'COMPACT',
            'record_offset': 0,
            'record_size': 1
        }
        try:
            result = ts.searchdata(request=request)
            contents = result.get('contents', [])
            if contents and contents[0].get('data_rows'):
                columns = contents[0].get('column_names', [])
                idx = columns.index('Number of Impressions')
                total += contents[0]['data_rows'][0][idx]
        except Exception as e:
            print(f"Impression fetch failed for {guid}, assuming threshold. Error: {e}")
            total += args.imp_threshold
    return total

# --- Execution ---
print("1) All fetched models:")
all_models = get_all_models()
print(all_models, "\n")

print(f"2) Models older than {args.days} days:")
old_models = filter_old_models(all_models, args.days)
print(old_models, "\n")

print(f"3) Models older than {args.days} days with dependencies:")
models_with_dependencies = []
for _, model in old_models.iterrows():
    model_id = model['Model_ID']
    dependents = get_dependents(model_id)
    models_with_dependencies.append({**model, 'Dependent_GUIDs': dependents})
models_with_dependencies_df = pd.DataFrame(models_with_dependencies)
print(models_with_dependencies_df[['Name', 'Model_ID', 'Dependent_GUIDs']], "\n")

print(f"4) Models older than {args.days} days, with dependencies and total impressions (last {args.lookback_days} days):")
models_with_imps = []
for _, row in models_with_dependencies_df.iterrows():
    dependents = row['Dependent_GUIDs']
    impressions = get_total_impressions(dependents, args.lookback_days)
    models_with_imps.append({**row, 'Total_Impressions': impressions})
models_with_imps_df = pd.DataFrame(models_with_imps)
print(models_with_imps_df[['Name', 'Model_ID', 'Dependent_GUIDs', 'Total_Impressions']], "\n")

print(f"5) Models older than {args.days} days with total impressions < {args.imp_threshold}:")
filtered_models = models_with_imps_df[models_with_imps_df['Total_Impressions'] < args.imp_threshold]
print(filtered_models[['Name', 'Model_ID', 'Dependent_GUIDs', 'Total_Impressions']])
