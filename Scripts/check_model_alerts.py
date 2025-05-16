import os
import argparse
import requests
from dotenv import load_dotenv
from thoughtspot_rest_api_v1 import TSRestApiV2

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Check if any dependents of a model have alerts")
parser.add_argument('--model-guid', required=True, help='GUID of the model to inspect')
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

# --- Fetch Dependent Objects ---
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

# --- Check for Alerts in Each Dependent ---
def check_for_alerts(ts, dependents):
    alert_found = False
    for dep in dependents:
        guid = dep['id']
        name = dep.get('name', 'Unknown')
        print(f"🔎 Checking dependent: {name} ({guid})")

        try:
            export_result = ts.metadata_tml_export(
                metadata=[{"identifier": guid}],
                export_associated=True,
                export_fqn=False,
                edoc_format="JSON",
                export_schema_version="DEFAULT",
                export_dependent=False,
                export_connection_as_dependent=False,
                all_orgs_override=False
            )

            for item in export_result:
                info = item.get("info", {})
                filename = info.get("filename", "")
                if filename.lower() == "alerts.tml":
                    print(f"🚨 Alert found on dependent: {name} ({guid})")
                    alert_found = True
                    break

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Error inspecting dependent {guid}: {str(e)}")

    return alert_found

# --- Main ---
try:
    dependents = fetch_dependents(ts, args.model_guid)
    print(f"\n🔍 Found {len(dependents)} dependents.")
    if not dependents:
        print("✅ No dependents found.")
        exit(0)

    has_alerts = check_for_alerts(ts, dependents)

    if has_alerts:
        print("\n❌ At least one dependent has an alert.")
    else:
        print("\n✅ No dependents have alerts.")

except Exception as e:
    print(f"\n❌ Script error: {str(e)}")
