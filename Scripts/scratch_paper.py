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
parser = argparse.ArgumentParser(description='Archive ThoughtSpot models based on age and usage.')
parser.add_argument('--days', type=int, default=90, help='Minimum model age in days (default: 90)')
parser.add_argument('--lookback-days', type=int, default=90, help='How far back to look for impressions (default: 90)')
parser.add_argument('--imp-threshold', type=int, default=1, help='Max allowed impressions (default: 1)')
parser.add_argument('--env-file', type=str, default='.env', help='Path to .env file')
args = parser.parse_args()  # Parse all the arguments provided at runtime

# =========================================
# Load environment variables
# =========================================
load_dotenv(dotenv_path=args.env_file)  # Load environment variables from .env file
USERNAME = os.getenv('TS_USERNAME')  # ThoughtSpot username
PASSWORD = os.getenv('TS_PASSWORD')  # ThoughtSpot password
SERVER_URL = os.getenv('TS_SERVER_URL').rstrip('/')  # Clean server URL to avoid double slashes
LOGICAL_TABLE_ID = os.getenv('TS_LOGICAL_TABLE_ID')  # Logical table ID for impression lookups
SAMPLE_GUID = os.getenv('TS_SAMPLE_GUID')  # Optional sample GUID for steps 8 and 9

# =========================================
# Authenticate with ThoughtSpot
# =========================================
ts = TSRestApiV2(server_url=SERVER_URL)  # Initialize ThoughtSpot API client
try:
    token = ts.auth_token_full(username=USERNAME, password=PASSWORD, validity_time_in_sec=3600)  # Get auth token
    ts.bearer_token = token['token']  # Apply bearer token to session
except requests.exceptions.HTTPError as e:
    print("Authentication failed:", e.response.content)  # Print failure
    exit(1)

# =========================================
# Step 1: Fetch all models
# =========================================
def get_all_models():
    request = {
        'metadata': [{'type': 'LOGICAL_TABLE'}],
        'include_details': True,
        'record_offset': 0,
        'record_size': 100000
    }
    result = ts.metadata_search(request=request)  # Fetch all logical table metadata
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
    df['Created_dt'] = pd.to_datetime(df['Created_ms'], unit='ms', errors='coerce')  # Convert to datetime
    return df

# =========================================
# Step 2: Filter models older than N days
# =========================================
def filter_old_models(df, min_age_days):
    cutoff = datetime.now() - timedelta(days=min_age_days)  # Calculate cutoff date
    return df[df['Created_dt'] < cutoff].copy()  # Filter DataFrame

# =========================================
# Step 4: Fetch dependent object GUIDs for a model
# =========================================
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
    return [obj['id'] for lst in dependents.values() for obj in lst]  # Flatten

# =========================================
# Step 5: Get total impressions for dependents
# =========================================
def get_total_impressions(dependents, days_window):
    total = 0
    for guid in dependents:
        query_string = (
            f"[Answer Book GUID] = '{guid}' count [Impressions] [Timestamp].'last {days_window} days' max [Timestamp]"
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
                idx = contents[0]['column_names'].index('Number of Impressions')
                total += contents[0]['data_rows'][0][idx]
        except Exception as e:
            print(f"[Impression Fetch Failed] {guid}: {e}")
            total += args.imp_threshold  # Assume max if fetch fails
    return total

# =========================================
# Step 6: Check for alerts in dependent TMLs
# =========================================
def check_alerts_on_dependents(row):
    for guid in row['Dependent_GUIDs']:
        try:
            payload = {"metadata": [{"identifier": guid}], "export_associated": True}
            res = ts.post_request("metadata/tml/export", payload)
            if isinstance(res, list):
                if any(item.get("info", {}).get("filename", "").lower() == "alerts.tml" for item in res):
                    return "Alert Found"
            return "No Alerts Found"
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                print(f"[Error] Failed to inspect {guid}: {e.response.status_code}")
            return "Unknown"
        except Exception as e:
            print(f"[Unexpected Error] {guid}: {e}")
            return "Unknown"
    return "No Alerts Found"

# =========================================
# Step 8: Preview permissions of a sample model
# =========================================
def fetch_sample_permissions(guid):
    try:
        res = ts.post_request("security/metadata/fetch-permissions", {
            "metadata": [{"identifier": guid}]
        })
        print("=========================================")
        print(f"8) Sample Permissions for metadata object GUID '{guid}':")
        print(res, "\n")
    except Exception as e:
        print(f"[Permissions Error] {guid}: {e}")

# =========================================
# Step 9: Export TMLs and show export status
# =========================================
def export_tml_for_sample_guid(guid):
    payload = {"metadata": [{"identifier": guid}]}
    try:
        res = ts.post_request("metadata/tml/export", payload)
        print("=========================================")
        print(f"9) Exporting Sample TML for GUID '{guid}'...\n")
        print(res)
    except Exception as e:
        print(f"[Export Error] {guid}: {e}")

# =========================================
# MAIN EXECUTION FLOW
# =========================================
print("=========================================")
print("1) All fetched models:")
all_models = get_all_models()
print(all_models, "\n")

print("=========================================")
print(f"2) Models older than {args.days} days:")
old_models = filter_old_models(all_models, args.days)
print(old_models, "\n")

print("=========================================")
print(f"3) Models older than {args.days} days with dependencies:")
models_with_dependencies = []
for _, model in old_models.iterrows():
    deps = get_dependents(model['Model_ID'])
    models_with_dependencies.append({**model, 'Dependent_GUIDs': deps})
models_with_dependencies_df = pd.DataFrame(models_with_dependencies)
print(models_with_dependencies_df[['Name', 'Model_ID', 'Dependent_GUIDs']], "\n")

print("=========================================")
print(f"4) With dependencies and total impressions (last {args.lookback_days} days):")
with_impressions = []
for _, row in models_with_dependencies_df.iterrows():
    imps = get_total_impressions(row['Dependent_GUIDs'], args.lookback_days)
    with_impressions.append({**row, 'Total_Impressions': imps})
with_imps_df = pd.DataFrame(with_impressions)
print(with_imps_df[['Name', 'Model_ID', 'Dependent_GUIDs', 'Total_Impressions']], "\n")

print("=========================================")
print(f"5) With total impressions < {args.imp_threshold}:")
filtered = with_imps_df[with_imps_df['Total_Impressions'] < args.imp_threshold].copy()
print(filtered[['Name', 'Model_ID', 'Dependent_GUIDs', 'Total_Impressions']], "\n")

print("=========================================")
print("6) Alert check on dependents:")
filtered['Alert_Status'] = filtered.apply(check_alerts_on_dependents, axis=1)
print(filtered[['Name', 'Model_ID', 'Total_Impressions', 'Alert_Status']], "\n")

print("=========================================")
print("7) Models ready for archiving:")
ready = filtered[filtered['Alert_Status'] == "No Alerts Found"]
print(ready[['Name', 'Model_ID', 'Total_Impressions', 'Alert_Status']], "\n")

if SAMPLE_GUID:
    fetch_sample_permissions(SAMPLE_GUID)
    export_tml_for_sample_guid(SAMPLE_GUID)
else:
    print("[Warning] SAMPLE_GUID not set in environment.")
