import os
import argparse
import json
import requests
from dotenv import load_dotenv
from thoughtspot_rest_api_v1 import TSRestApiV2

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Check if any dependents of a model have alerts")
parser.add_argument('--model-guid', required=True, help='GUID of the model (LOGICAL_TABLE) to inspect')
parser.add_argument('--env-file', type=str, default='.env', help='Path to .env file')
args = parser.parse_args()

# --- Load Environment Variables ---
load_dotenv(args.env_file)
USERNAME = os.getenv('TS_USERNAME')
PASSWORD = os.getenv('TS_PASSWORD')
SERVER_URL = os.getenv('TS_SERVER_URL')

# --- Authenticate with ThoughtSpot ---
ts = TSRestApiV2(server_url=SERVER_URL)
print(f"\nAuthenticating to {SERVER_URL} as {USERNAME}...")
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

# --- Step 2: Inspect each dependent for alerts ---
def dependent_has_alert(ts, dep_guid):
    export_payload = {
        "metadata": [{"identifier": dep_guid}],
        "export_associated": True,
        "export_fqn": False,
        "edoc_format": "JSON",
        "export_schema_version": "DEFAULT",
        "export_dependent": False,
        "export_connection_as_dependent": False,
        "all_orgs_override": False
    }

    try:
        result = ts.post_request(
            endpoint="/metadata/tml/export",
            request=export_payload
        )

        for item in result:
            filename = item.get("info", {}).get("filename", "").lower()
            if filename == "alerts.tml":
                return True
        return False

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error inspecting dependent {dep_guid}: {e}")
        return False

# --- Main Execution ---
try:
    dependents = fetch_dependents(ts, args.model_guid)
    print(f"\n🔍 Found {len(dependents)} dependents.\n")

    found_alert = False
    for dep in dependents:
        dep_guid = dep.get('id')
        dep_name = dep.get('name', 'Unknown')
        print(f"🔎 Checking dependent: {dep_name} ({dep_guid})")

        if dependent_has_alert(ts, dep_guid):
            print(f"🚨 Alert found on dependency: {dep_name} ({dep_guid})")
            found_alert = True
            break

    if not found_alert:
        print("\n✅ No dependents have alerts.")

except Exception as e:
    print(f"\n❌ Script error: {e}")



