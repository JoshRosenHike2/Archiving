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
    return df  # Return the DataFrame

# =========================================
# Step 2: Filter models older than N days
# =========================================
def filter_old_models(df, min_age_days):
    cutoff = datetime.now() - timedelta(days=min_age_days)  # Calculate the cutoff datetime
    return df[df['Created_dt'] < cutoff].copy()  # Filter models created before the cutoff


# Step 3: Check for any real response on survey model
# =========================================

# this would be done using the same searchdata api endpoint you will see later on in this code.


# =========================================
# Step 4: Fetch dependent object GUIDs for a model
# =========================================
def get_dependents(model_id):
    request = {
        'metadata': [{'type': 'LOGICAL_TABLE', 'identifier': model_id}],  # Specify model ID
        'include_dependent_objects': True,  # Include dependent objects
        'include_headers': True,  # Include headers
        'record_offset': 0,
        'record_size': 1
    }
    res = ts.metadata_search(request=request)  # Make the request
    if not res:  # If nothing returned
        return []
    dependents = res[0].get('dependent_objects', {}).get(model_id, {})  # Extract dependents
    return [obj['id'] for lst in dependents.values() for obj in lst]  # Flatten and return GUID list

# =========================================
# Step 5: Get total impressions for dependents
# =========================================
def get_total_impressions(dependents, days_window):
    total = 0  # Initialize total
    for guid in dependents:  # Loop through dependent GUIDs
        query_string = (
            f"[Answer Book GUID] = '{guid}' "
            f"count [Impressions] [Timestamp].'last {days_window} days' max [Timestamp]"
        )  # Build search query
        request = {
            'query_string': query_string,
            'logical_table_identifier': LOGICAL_TABLE_ID,  # Use the impressions table
            'data_format': 'COMPACT',
            'record_offset': 0,
            'record_size': 1
        }
        try:
            result = ts.searchdata(request=request)  # Execute the query
            contents = result.get('contents', [])  # Get result contents
            if contents and contents[0].get('data_rows'):  # If data exists
                idx = contents[0]['column_names'].index('Number of Impressions')  # Get index of impressions column
                total += contents[0]['data_rows'][0][idx]  # Add impressions
        except Exception as e:
            print(f"[Impression Fetch Failed] {guid}: {e}")  # Log failure
            total += args.imp_threshold  # Assume max to avoid deletion risk
    return total  # Return total impressions

# =========================================
# Step 6: Check for alerts in dependent TMLs
# =========================================
def check_alerts_on_dependents(row):
    for guid in row['Dependent_GUIDs']:  # Loop through all dependents
        try:
            payload = {"metadata": [{"identifier": guid}], "export_associated": True}  # Build TML export payload
            res = ts.post_request("/metadata/tml/export", payload)  # Export TML
            if isinstance(res, list):  # Check export result
                if any(item.get("info", {}).get("filename", "").lower() == "alerts.tml" for item in res):
                    return "Alert Found"  # Return if alert exists
            return "No Alerts Found"  # No alerts found
        except requests.exceptions.HTTPError as e:
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
        res = ts.post_request("/security/metadata/fetch-permissions", {
            "metadata": [{"identifier": guid}]
        })  # Fetch permissions for the sample GUID
        print("=========================================")
        print(f"8) Sample Permissions for metadata object GUID '{guid}':")
        print(res, "\n")
    except Exception as e:
        print(f"[Permissions Error] {guid}: {e}")

# =========================================
# Step 9: Export TMLs and show export status
# =========================================
def export_tml_for_sample_guid(guid):
    payload = {"metadata": [{"identifier": guid}]}  # Build export payload for sample
    try:
        res = ts.post_request("/metadata/tml/export", payload)  # Export the sample model
        print("=========================================")
        print(f"9) Exporting Sample TML for GUID '{guid}'...\n")
        print(res)  # Print the TML export result
    except Exception as e:
        print(f"[Export Error] {guid}: {e}")

# =========================================
# MAIN EXECUTION FLOW
# =========================================
print("=========================================")
print("1) All fetched models:")
all_models = get_all_models()  # Call function to get all models
print(all_models, "\n")

print("=========================================")
print(f"2) Models older than {args.days} days:")
old_models = filter_old_models(all_models, args.days)  # Filter by age
print(old_models, "\n")

print("=========================================")
print(f"3) Models older than {args.days} days with dependencies:")
models_with_dependencies = []
for _, model in old_models.iterrows():
    deps = get_dependents(model['Model_ID'])  # Get dependents for each model
    models_with_dependencies.append({**model, 'Dependent_GUIDs': deps})  # Add dependents to row
models_with_dependencies_df = pd.DataFrame(models_with_dependencies)  # Convert to DataFrame
print(models_with_dependencies_df[['Name', 'Model_ID', 'Dependent_GUIDs']], "\n")

print("=========================================")
print(f"4) With dependencies and total impressions (last {args.lookback_days} days):")
with_impressions = []
for _, row in models_with_dependencies_df.iterrows():
    imps = get_total_impressions(row['Dependent_GUIDs'], args.lookback_days)  # Get impression count
    with_impressions.append({**row, 'Total_Impressions': imps})  # Append to list
with_imps_df = pd.DataFrame(with_impressions)  # Convert to DataFrame
print(with_imps_df[['Name', 'Model_ID', 'Dependent_GUIDs', 'Total_Impressions']], "\n")

print("=========================================")
print(f"5) With total impressions < {args.imp_threshold}:")
filtered = with_imps_df[with_imps_df['Total_Impressions'] < args.imp_threshold].copy()  # Apply threshold filter
print(filtered[['Name', 'Model_ID', 'Dependent_GUIDs', 'Total_Impressions']], "\n")

print("=========================================")
print("6) Alert check on dependents:")
filtered['Alert_Status'] = filtered.apply(check_alerts_on_dependents, axis=1)  # Add alert check result
print(filtered[['Name', 'Model_ID', 'Total_Impressions', 'Alert_Status']], "\n")

print("=========================================")
print("7) Models ready for archiving:")
ready = filtered[filtered['Alert_Status'] == "No Alerts Found"]  # Filter final models
print(ready[['Name', 'Model_ID', 'Total_Impressions', 'Alert_Status']], "\n")

# Preview sample permissions for hardcoded GUID
if SAMPLE_GUID:
    fetch_sample_permissions(SAMPLE_GUID)  # Call Step 8

# Export TML for hardcoded GUID
if SAMPLE_GUID:
    export_tml_for_sample_guid(SAMPLE_GUID)  # Call Step 9
else:
    print("[Warning] SAMPLE_GUID not set in environment.")
