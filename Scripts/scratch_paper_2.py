import os  # Provides access to environment variables and OS functions
import argparse  # Used to parse command line arguments
import requests.exceptions  # Handles HTTP exceptions
from datetime import datetime, timedelta  # For working with timestamps and date comparisons
import pandas as pd  # For data manipulation and analysis
from dotenv import load_dotenv  # Loads environment variables from a .env file
from thoughtspot_rest_api_v1 import *  # Imports the ThoughtSpot REST API client

# --- Parse input arguments from the command line ---
parser = argparse.ArgumentParser(description='Archive ThoughtSpot models based on age and usage.')
parser.add_argument('--days', type=int, default=90, help='Minimum model age in days to be considered')
parser.add_argument('--imp-threshold', type=int, default=1, help='Max total impressions to allow')
parser.add_argument('--env-file', type=str, default='.env', help='Path to environment file')
args = parser.parse_args()  # Parses all arguments into the args object

# --- Load credentials and endpoint from environment file ---
load_dotenv(dotenv_path=args.env_file)  # Loads the specified .env file
username = os.getenv('TS_USERNAME')  # Gets ThoughtSpot username
password = os.getenv('TS_PASSWORD')  # Gets ThoughtSpot password
server = os.getenv('TS_SERVER_URL')  # Gets ThoughtSpot server URL
usage_table_id = os.getenv('TS_LOGICAL_TABLE_ID')  # Gets Logical Table ID used for impressions

# --- Authenticate to ThoughtSpot ---
ts = TSRestApiV2(server_url=server)  # Initializes ThoughtSpot API client
try:
    auth = ts.auth_token_full(username=username, password=password, validity_time_in_sec=3600)  # Get auth token
    ts.bearer_token = auth['token']  # Set bearer token for subsequent requests
except requests.exceptions.HTTPError as e:  # Catch any HTTP errors during auth
    print("Authentication failed.", e, e.response.content)  # Print error info
    exit(1)  # Exit program

# --- Step 1: Fetch all logical tables (models) ---
def get_all_models():
    search_request = {
        'metadata': [{'type': 'LOGICAL_TABLE'}],  # Look only for logical tables
        'include_details': True,  # Get full metadata info
        'record_offset': 0,
        'record_size': 100000  # Request a large batch
    }
    result = ts.metadata_search(request=search_request)  # Call the API
    rows = []  # Initialize list for DataFrame rows
    for model in result:
        meta = model.get('metadata_header', {})  # Extract metadata header
        rows.append({  # Build row
            'Model_ID': meta.get('id'),
            'Name': meta.get('name'),
            'Author': meta.get('authorDisplayName'),
            'Created_ms': meta.get('created')
        })
    df = pd.DataFrame(rows)  # Convert to DataFrame
    df['Created_dt'] = pd.to_datetime(df['Created_ms'], unit='ms', errors='coerce')  # Convert timestamp
    return df  # Return DataFrame

# --- Step 2: Filter models by age ---
def filter_old_models(df, min_age_days):
    cutoff = datetime.now() - timedelta(days=min_age_days)  # Calculate cutoff date
    return df[df['Created_dt'] < cutoff].copy()  # Return only old models

# --- Step 3: Fetch all dependents for a given model ---
def get_dependents(model_id):
    search_request = {
        'metadata': [{'type': 'LOGICAL_TABLE', 'identifier': model_id}],  # Filter by model ID
        'include_dependent_objects': True,  # Request dependent objects
        'include_headers': True,
        'record_offset': 0,
        'record_size': 1
    }
    res = ts.metadata_search(request=search_request)  # Make the API call
    if not res:
        return []  # Return empty if no results
    dependents = res[0].get('dependent_objects', {}).get(model_id, {})  # Get dependents by model ID
    return [obj['id'] for lst in dependents.values() for obj in lst]  # Flatten and return GUIDs

# --- Step 4: Calculate total impressions across all dependents ---
def get_total_impressions(dependents, days):
    total = 0  # Start count at 0
    for guid in dependents:
        search_request = {
            'query_string': (
                f"[Answer Book GUID] = '{guid}' "  # Filter by dependent GUID
                f"count [Impressions] [Timestamp].'last {days} days' max [Timestamp]"  # Count over time window
            ),
            'logical_table_identifier': usage_table_id,  # Logical table to run the search on
            'data_format': 'COMPACT',
            'record_offset': 0,
            'record_size': 1
        }
        try:
            result = ts.searchdata(request=search_request)  # Execute query
            contents = result.get('contents', [])
            if contents and contents[0].get('data_rows'):
                columns = contents[0].get('column_names', [])  # Get column headers
                idx = columns.index('Number of Impressions')  # Find index of impressions
                total += contents[0]['data_rows'][0][idx]  # Add impression count
        except:
            total += args.imp_threshold  # Assume high activity if query fails
    return total  # Return sum

# --- Main Execution Flow ---
print("1) All fetched models:")
models_all = get_all_models()  # Step 1
print(models_all, "\n")

print(f"2) Models older than {args.days} days:")
models_old = filter_old_models(models_all, args.days)  # Step 2
print(models_old, "\n")

# Step 3: Append dependents and impressions for each old model
print(f"3) Models older than {args.days} days with dependencies and total impressions:")
model_rows = []
for _, model in models_old.iterrows():
    model_id = model['Model_ID']
    dependents = get_dependents(model_id)
    impressions = get_total_impressions(dependents, args.days)
    model_rows.append({**model, 'Dependent_GUIDs': dependents, 'Total_Impressions': impressions})
models_with_imps = pd.DataFrame(model_rows)
print(models_with_imps[['Name', 'Model_ID', 'Dependent_GUIDs', 'Total_Impressions']], "\n")

# Step 4: Final filter based on impression threshold
print(f"4) Models older than {args.days} days with total impressions < {args.imp_threshold}:")
models_filtered = models_with_imps[models_with_imps['Total_Impressions'] < args.imp_threshold].copy()
print(models_filtered[['Name', 'Model_ID', 'Dependent_GUIDs', 'Total_Impressions']])
