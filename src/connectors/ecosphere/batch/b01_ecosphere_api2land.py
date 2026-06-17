# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import requests
import json
import os
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Setup Widgets and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("schema_name", "land", "Schema Name for Volume")
dbutils.widgets.text("volume_name", "ecosphere_batch", "Volume Name")
dbutils.widgets.text("offset_days", "1", "Offset in days (0=today, 1=yesterday, 2=day before yesterday)")
dbutils.widgets.text("secret_scope", "", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
OFFSET_DAYS = int(dbutils.widgets.get("offset_days"))
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

# All energy_method values supported by /electricity/history/day/
# Each produces a separate API call and a separate JSON file in the landing volume.
# bronze.ecosphere_history stores all 6 variants via the energy_method column.
ELECTRICITY_ENERGY_METHODS = [
    "delivery",        # Grid import (power_import in silver)
    "return_delivery", # Grid export (power_export in silver)
    "production",      # On-site solar PV generation (power_production in silver)
    "consumption",     # Net building consumption (power_net in silver)
    "charge",          # Battery charging power (power_charge in silver)
    "discharge",       # Battery discharging power (power_discharge in silver)
]

BASE_URL = "https://api.ecosphere.nl/public-api"
TOKEN_FILE_PATH = f"/Volumes/{CATALOG_NAME}/operational/ecosphere_token_temp/token_temp.json"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Ensure Token Volume Exists
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.operational.ecosphere_token_temp")
print(f"Volume ready: {CATALOG_NAME}.operational.ecosphere_token_temp")

# COMMAND ----------

# DBTITLE 1,Define Buildings
# Each building has a name (for logging) and UUID (for API calls).
BUILDINGS = [
    {
        "name": "Bedrijfspand (accu (trading), zonnepanelen)",
        "uuid": "a5667b60-0b35-453d-a9ab-da3322b8c5f6",
    },
    {
        "name": "Bedrijfspandd (elektriciteit, water en warmte)",
        "uuid": "0b7db046-1193-444c-a0f7-6380d4f3c875",
    },
    {
        "name": "Bedrijfspand (solar curtailment - EPEX)",
        "uuid": "b485b1cb-5b07-4f2b-9e25-dc29a46dc1d2",
    },
    {
        "name": "Kantoor 3x80a (accu, zonnepanelen, warmtepomp)",
        "uuid": "6ba13993-c8b6-42db-ab28-315ba78cb875",
    },
]

# COMMAND ----------

# DBTITLE 1,Define Endpoints to Ingest
# Each endpoint has:
#   path       - API path relative to /buildings/{uuid}
#   name       - logical name used for folder structure and bronze endpoint_name column
#   params     - query parameters to send with the request
# Note: OFFSET_DAYS and ENERGY_METHOD are resolved after widgets are read above.
def build_endpoints(offset_days, electricity_energy_methods):
    endpoints = []

    # --- Electricity history: one call per energy_method (all land in bronze.ecosphere_history) ---
    for energy_method in electricity_energy_methods:
        endpoints.append({
            "path": "/electricity/history/day/",
            "name": "electricity_history_day",
            "params": {"offset": -offset_days, "energy_method": energy_method},
        })

    endpoints += [
        # --- History endpoints (other energy carriers, same {start_datetime, end_datetime, values[]} shape) ---
        {
            "path": "/heat/history/day/",
            "name": "heat_history_day",
            "params": {"offset": -offset_days},
        },
        {
            "path": "/water/history/day/",
            "name": "water_history_day",
            "params": {"offset": -offset_days},
        },
        {
            "path": "/gas/history/day/",
            "name": "gas_history_day",
            "params": {"offset": -offset_days},
        },
        {
            "path": "/fcr/history/quarter/",
            "name": "fcr_history_quarter",
            "params": {"offset": -offset_days},
        },
        # --- Meterreadings endpoints (cumulative meter totals, array of meter objects) ---
        {
            "path": "/electricity/meterreadings/",
            "name": "electricity_meterreadings",
            "params": {"offset": -offset_days},
        },
        {
            "path": "/heat/meterreadings/",
            "name": "heat_meterreadings",
            "params": {"offset": -offset_days},
        },
        {
            "path": "/water/meterreadings/",
            "name": "water_meterreadings",
            "params": {"offset": -offset_days},
        },
        # --- Point value endpoints (single {datetime, value} object) ---
        {
            "path": "/electricity/capacity/actual/soc/",
            "name": "electricity_soc",
            "params": {"offset": -offset_days},
        },
        {
            "path": "/solar_irradiance/actual/",
            "name": "solar_irradiance",
            "params": {"offset": -offset_days},
        },
        # --- EV socket endpoints (array of {sensor_uuid, datetime, available, in_session}) ---
        {
            "path": "/ev_socket/actual/",
            "name": "ev_socket",
            "params": {"offset": -offset_days},
        },
    ]

    return endpoints

ENDPOINTS = build_endpoints(OFFSET_DAYS, ELECTRICITY_ENERGY_METHODS)

# COMMAND ----------

# DBTITLE 1,Token Management Functions
def save_token_file(token_data):
    """Saves the full token response (access + refresh) to a JSON file on the volume."""
    json_content = json.dumps(token_data)
    dbutils.fs.put(TOKEN_FILE_PATH, json_content, overwrite=True)
    print(f"[System] Tokens saved to {TOKEN_FILE_PATH}")


def load_token_file():
    """Loads the token data from the JSON file on the volume."""
    try:
        content = dbutils.fs.head(TOKEN_FILE_PATH, 10000)
        return json.loads(content)
    except Exception:
        return None


def get_auth_headers():
    """
    1. Check for existing refresh token and try to refresh.
    2. If refresh fails or no token exists, perform full login with credentials.
    """
    api_username = dbutils.secrets.get(scope=SECRET_SCOPE, key="username")
    api_password = dbutils.secrets.get(scope=SECRET_SCOPE, key="password")

    stored_data = load_token_file()

    # --- PATH A: Try Refresh ---
    if stored_data and "refresh" in stored_data:
        print("[Auth] Found cached token. Attempting refresh...")
        try:
            refresh_response = requests.post(
                f"{BASE_URL}/token/refresh/",
                json={"refresh": stored_data["refresh"]},
            )
            if refresh_response.status_code == 200:
                new_tokens = refresh_response.json()
                save_token_file(new_tokens)
                print("[Auth] Session refreshed successfully.")
                return {"Authorization": f"Bearer {new_tokens['access']}"}
            else:
                print(f"[Auth] Refresh failed ({refresh_response.status_code}).")
        except Exception as e:
            print(f"[Auth] Refresh error: {e}")

    # --- PATH B: Full Login (Fallback) ---
    print("[Auth] Performing full credential login...")
    login_response = requests.post(
        f"{BASE_URL}/token/",
        json={"username": api_username, "password": api_password},
    )

    if login_response.status_code == 200:
        tokens = login_response.json()
        save_token_file(tokens)
        print("[Auth] Login successful.")
        return {"Authorization": f"Bearer {tokens['access']}"}
    else:
        raise Exception(f"Login Failed: {login_response.text}")

# COMMAND ----------

# DBTITLE 1,Create Landing Volume (if not exists)
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}
    COMMENT 'Landing volume for raw JSON data from the Ecosphere API (history, meterreadings, point values, EV sockets).'
""")

# COMMAND ----------

# DBTITLE 1,Calculate Target Date
target_date = (datetime.utcnow() - timedelta(days=OFFSET_DAYS)).date()

print(f"Target date to fetch:")
print(f"  - Date: {target_date.strftime('%Y-%m-%d')}")
print(f"  - Offset: {OFFSET_DAYS} (API will use -{OFFSET_DAYS})")
print(f"  - Electricity energy methods: {ELECTRICITY_ENERGY_METHODS}")
print(f"  - Endpoints to process: {len(ENDPOINTS)}")

# COMMAND ----------

# DBTITLE 1,Authenticate
headers = get_auth_headers()
headers["Content-Type"] = "application/json"

# COMMAND ----------

# DBTITLE 1,Validate Buildings Against API
print("\n--- Validating configured buildings against API ---")
r_buildings = requests.get(f"{BASE_URL}/buildings/", headers=headers)

if r_buildings.status_code == 200:
    api_buildings = r_buildings.json()
    api_uuids = {b["uuid"] for b in api_buildings}
    print(f"Buildings available in API: {len(api_buildings)}")
    for b in api_buildings:
        print(f"  - {b.get('name', 'N/A')}: {b['uuid']}")

    configured_uuids = {b["uuid"] for b in BUILDINGS}
    missing = configured_uuids - api_uuids
    if missing:
        print(f"\n  WARNING: Configured UUIDs not found in API: {missing}")
else:
    print(f"  WARNING: Could not validate buildings (HTTP {r_buildings.status_code})")

# COMMAND ----------

# DBTITLE 1,Process All Buildings x All Endpoints
print(f"\n{'='*80}")
print(f"ECOSPHERE INGESTION - MULTI-ENDPOINT BATCH")
print(f"{'='*80}")
print(f"Buildings: {len(BUILDINGS)} | Endpoints: {len(ENDPOINTS)}")
print(f"Target date: {target_date.strftime('%Y-%m-%d')} (offset={OFFSET_DAYS})")
print(f"{'='*80}\n")

results = []
total_files_written = 0
total_buildings_with_data = 0
total_skipped_no_data = 0
total_errors = 0

for b_idx, building in enumerate(BUILDINGS, 1):
    building_name = building["name"]
    building_uuid = building["uuid"]

    print(f"\n[Building {b_idx}/{len(BUILDINGS)}] {building_name} ({building_uuid})")
    print("=" * 80)

    for e_idx, endpoint_cfg in enumerate(ENDPOINTS, 1):
        endpoint_path = endpoint_cfg["path"]
        endpoint_name = endpoint_cfg["name"]
        endpoint_params = endpoint_cfg["params"]

        print(f"\n  [{e_idx}/{len(ENDPOINTS)}] Endpoint: {endpoint_name}")

        result = {
            "building_uuid": building_uuid,
            "building_name": building_name,
            "endpoint_name": endpoint_name,
            "status": "pending",
            "files_written": 0,
            "error": None,
        }

        try:
            url = f"{BASE_URL}/buildings/{building_uuid}{endpoint_path}"
            print(f"    -> GET {url}")
            print(f"    -> Params: {endpoint_params}")

            response = requests.get(url, headers=headers, params=endpoint_params)

            # 204 = no content for this building/endpoint combination — not an error
            if response.status_code == 204:
                print(f"    -> 204 No Content — skipping (no data for this building/endpoint)")
                result["status"] = "no_data"
                total_skipped_no_data += 1
                results.append(result)
                continue

            if response.status_code != 200:
                print(f"    -> ERROR {response.status_code}: {response.text[:300]}")
                result["status"] = "error"
                result["error"] = f"HTTP {response.status_code}: {response.text[:200]}"
                total_errors += 1
                results.append(result)
                continue

            raw_response = response.json()

            # Check for empty response (empty list or missing values)
            if raw_response is None or raw_response == [] or raw_response == {}:
                print(f"    -> Empty response — skipping")
                result["status"] = "no_data"
                total_skipped_no_data += 1
                results.append(result)
                continue

            # Build the JSON envelope that b02 will read
            # raw_response is stored as-is (dict for history, list for meterreadings)
            combined_data = {
                "building_uuid": building_uuid,
                "building_name": building_name,
                "endpoint_name": endpoint_name,
                "energy_method": endpoint_params.get("energy_method", None),
                "date": target_date.strftime("%Y-%m-%d"),
                "raw_response": raw_response,
            }

            # Write to volume: {VOLUME_PATH}/{building_uuid}/{endpoint_name}/
            folder_path = f"{VOLUME_PATH}/{building_uuid}/{endpoint_name}"
            dbutils.fs.mkdirs(folder_path)

            processing_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            energy_method = endpoint_params.get("energy_method", "")
            energy_method_suffix = f"_{energy_method}" if energy_method else ""
            file_name = f"ecosphere_{building_uuid}_{endpoint_name}{energy_method_suffix}_{processing_ts}.json"
            output_path = f"{folder_path}/{file_name}"

            json_content = json.dumps(combined_data, indent=2)
            dbutils.fs.put(output_path, json_content, overwrite=False)

            print(f"    -> OK: Written to {file_name}")
            result["status"] = "success"
            result["files_written"] = 1
            total_files_written += 1
            total_buildings_with_data += 1

        except Exception as e:
            error_msg = str(e)
            print(f"    -> FAILED: {error_msg}")
            result["status"] = "error"
            result["error"] = error_msg
            total_errors += 1

        results.append(result)

# COMMAND ----------

# DBTITLE 1,Summary
print(f"\n{'='*80}")
print(f"INGESTION SUMMARY")
print(f"{'='*80}")
print(f"Total files written : {total_files_written}")
print(f"Skipped (no data)   : {total_skipped_no_data}")
print(f"Errors              : {total_errors}")
print(f"{'='*80}")

# Group by endpoint for a cleaner view
from collections import defaultdict
by_endpoint = defaultdict(list)
for r in results:
    by_endpoint[r["endpoint_name"]].append(r)

for ep_name, ep_results in by_endpoint.items():
    success = [r for r in ep_results if r["status"] == "success"]
    no_data = [r for r in ep_results if r["status"] == "no_data"]
    errors  = [r for r in ep_results if r["status"] == "error"]
    print(f"\n  {ep_name}:")
    print(f"    OK={len(success)}  no_data={len(no_data)}  errors={len(errors)}")
    for r in errors:
        print(f"    [ERROR] {r['building_name']}: {r['error']}")
