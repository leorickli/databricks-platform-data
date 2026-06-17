# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import os
import json
import requests
from datetime import datetime, timedelta

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("volume_name", "sunpeak_inverters_batch", "Volume name for the API files")
dbutils.widgets.text("secret_scope", "", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key="api_key")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD HH:MM:SS) - Leave empty for last 15 minutes")
START_DATE = dbutils.widgets.get("start_date")

dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD HH:MM:SS) - Leave empty for current time")
END_DATE = dbutils.widgets.get("end_date")

BASE_URL = "https://monitoringapi.sunpeak.com"
SITES_LIST_URL = f"{BASE_URL}/sites/list"

# COMMAND ----------

# DBTITLE 1,Create Volume if Not Exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw JSON files from Sunpeak API'
""")

# COMMAND ----------

# DBTITLE 1,Determine Time Range
# If dates not provided, default to last 15 minutes for power data
if not START_DATE or not END_DATE:
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=15)
    START_DATE = start_time.strftime("%Y-%m-%d %H:%M:%S")
    END_DATE = end_time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Using default time range (last 15 minutes):")
else:
    print(f"Using user-provided time range:")

print(f"  Start: {START_DATE}")
print(f"  End: {END_DATE}")

# COMMAND ----------

# DBTITLE 1,Fetch All Site IDs
params = {"api_key": API_KEY}

sites_list_response = requests.get(SITES_LIST_URL, params=params)
sites_list_response.raise_for_status()

sites_data = sites_list_response.json()["sites"]
site_ids = [site["id"] for site in sites_data["site"]]
site_names = {site["id"]: site["name"] for site in sites_data["site"]}

print(f"Found {sites_data['count']} site(s):")
for site_id in site_ids:
    print(f"  - Site ID {site_id}: {site_names[site_id]}")

# COMMAND ----------

# DBTITLE 1,Fetch Data from All Sites
total_files_saved = 0
successful_sites = []
failed_sites = []

for site_id in site_ids:
    try:
        print(f"\n--- Processing Site ID: {site_id} ({site_names[site_id]}) ---")

        # Common parameters for API calls
        api_params = {
            "api_key": API_KEY,
            "startTime": START_DATE,
            "endTime": END_DATE
        }

        # Generate timestamp for filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # FETCH INVERTER TECHNICAL DATA
        print("  Fetching inverter equipment list...")
        equipment_url = f"{BASE_URL}/equipment/{site_id}/list"
        equipment_params = {"api_key": API_KEY}

        try:
            equipment_response = requests.get(equipment_url, params=equipment_params)
            equipment_response.raise_for_status()
            equipment_data = equipment_response.json()

            # Extract inverter serial numbers
            inverters = equipment_data.get("reporters", {}).get("list", [])

            if not inverters:
                print(f"  ⚠ No inverters found for site {site_id}")
            else:
                print(f"  Found {len(inverters)} inverter(s)")

                # Fetch technical data for each inverter
                for inverter in inverters:
                    serial_number = inverter.get("serialNumber")
                    inverter_name = inverter.get("name", "Unknown")

                    if not serial_number:
                        print(f"    ⚠ Skipping inverter without serial number: {inverter_name}")
                        continue

                    print(f"    Fetching data for inverter: {inverter_name} ({serial_number})")

                    inverter_data_url = f"{BASE_URL}/equipment/{site_id}/{serial_number}/data"

                    try:
                        inverter_response = requests.get(inverter_data_url, params=api_params)
                        inverter_response.raise_for_status()
                        inverter_data = inverter_response.json()

                        # Save inverter technical data to volume with serial number in filename
                        inverter_filename = f"sunpeak_inverter_{site_id}_{serial_number}_{timestamp}.json"
                        inverter_file_path = os.path.join(VOLUME_PATH, inverter_filename)

                        with open(inverter_file_path, 'w') as f:
                            json.dump(inverter_data, f, indent=2)

                        print(f"    ✓ Saved inverter data to: {inverter_filename}")
                        total_files_saved += 1

                    except Exception as e:
                        print(f"    ✗ Failed to fetch inverter data for {serial_number}: {e}")

        except Exception as e:
            print(f"  ✗ Failed to fetch equipment list: {e}")

        successful_sites.append(site_id)
        print(f"✓ Completed processing for site {site_id}")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP error fetching data from site {site_id}: {e}")
        if e.response is not None:
            print(f"Response status: {e.response.status_code}, Response body: {e.response.text}")
        failed_sites.append({"site_id": site_id, "reason": str(e)})

    except Exception as e:
        print(f"Error processing site {site_id}: {e}")
        failed_sites.append({"site_id": site_id, "reason": str(e)})
