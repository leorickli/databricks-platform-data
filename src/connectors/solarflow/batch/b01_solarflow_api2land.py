# Databricks notebook source
# DBTITLE 1,Install Required Libraries
%pip install solarflowServer

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
import solarflowServer
import os
import json
import time
from datetime import datetime

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("volume_name", "solarflow_inverters_batch", "Volume name for the API files")
dbutils.widgets.text("secret_scope", "", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")

API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key="api_key")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Create Volume if Not Exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw JSON files from Solarflow API'
""")

# COMMAND ----------

# DBTITLE 1,Initialize Solarflow API with Retry Logic
def initialize_api_with_retry(token, max_retries=5, initial_wait=60):
    """
    Initialize Solarflow API with retry logic for rate limiting errors.

    Args:
        token: API token
        max_retries: Maximum number of retry attempts
        initial_wait: Initial wait time in seconds (doubles with each retry)
    """
    for attempt in range(max_retries):
        try:
            api = solarflowServer.OpenApiV1(token=token)
            print(f"✓ Successfully initialized Solarflow API (attempt {attempt + 1}/{max_retries})")
            return api
        except Exception as e:
            error_str = str(e)

            # Check for rate limiting error
            if "error_frequently_access" in error_str or "10012" in error_str:
                wait_time = initial_wait * (2 ** attempt)  # Exponential backoff

                if attempt < max_retries - 1:
                    print(f"⚠ Rate limit error detected (error_code: 10012 - error_frequently_access)")
                    print(f"  Waiting {wait_time} seconds before retry {attempt + 2}/{max_retries}...")
                    time.sleep(wait_time)
                else:
                    raise Exception(
                        f"Failed to initialize Solarflow API after {max_retries} attempts. "
                        f"Rate limit error persists. Please try again later."
                    )
            else:
                # For other errors, fail immediately
                raise Exception(f"Failed to initialize Solarflow API: {e}")

    raise Exception(f"Failed to initialize Solarflow API after {max_retries} attempts")

try:
    api = initialize_api_with_retry(API_KEY)
except Exception as e:
    raise Exception(f"API initialization failed: {e}")

# COMMAND ----------

# DBTITLE 1,Fetch All Plants/Sites with Retry Logic
def fetch_plant_list_with_retry(api_instance, max_retries=5, initial_wait=60):
    """
    Fetch plant list with retry logic for rate limiting errors.

    Args:
        api_instance: Initialized Solarflow API instance
        max_retries: Maximum number of retry attempts
        initial_wait: Initial wait time in seconds (doubles with each retry)
    """
    for attempt in range(max_retries):
        try:
            plants = api_instance.plant_list()
            print(f"✓ Successfully fetched plant list (attempt {attempt + 1}/{max_retries})")
            return plants
        except Exception as e:
            error_str = str(e)

            # Check for rate limiting error
            if "error_frequently_access" in error_str or "10012" in error_str:
                wait_time = initial_wait * (2 ** attempt)

                if attempt < max_retries - 1:
                    print(f"⚠ Rate limit error detected while fetching plant list")
                    print(f"  Waiting {wait_time} seconds before retry {attempt + 2}/{max_retries}...")
                    time.sleep(wait_time)
                else:
                    raise Exception(
                        f"Failed to fetch plant list after {max_retries} attempts. "
                        f"Rate limit error persists. Please try again later."
                    )
            else:
                # For other errors, provide detailed error information
                print(f"✗ Error fetching plant list: {error_str}")
                print(f"  Error type: {type(e).__name__}")

                # Check if it's a SolarflowV1ApiError with error details
                if hasattr(e, 'error_code') and hasattr(e, 'error_msg'):
                    print(f"  Solarflow API Error Code: {e.error_code}")
                    print(f"  Solarflow API Error Message: {e.error_msg}")

                raise Exception(f"Failed to fetch plant list: {error_str}")

    raise Exception(f"Failed to fetch plant list after {max_retries} attempts")

try:
    plants = fetch_plant_list_with_retry(api)
    print(f"Found {plants['count']} plant(s):")

    for plant in plants['plants']:
        plant_id = plant['plant_id']
        plant_name = plant.get('plant_name', 'Unnamed Plant')
        print(f"  - Plant ID {plant_id}: {plant_name}")

except Exception as e:
    raise Exception(f"Plant list fetch failed: {e}")

# COMMAND ----------

# DBTITLE 1,Fetch Data from All Plants
total_files_saved = 0
successful_plants = []
failed_plants = []

for plant in plants['plants']:
    plant_id = plant['plant_id']
    plant_name = plant.get('plant_name', 'Unnamed Plant')

    try:
        print(f"\n--- Processing Plant: '{plant_name}' (ID: {plant_id}) ---")

        # Get devices for the current plant
        devices = api.device_list(plant_id)
        print(f"  Found {devices['count']} device(s) in this plant")

        for device in devices['devices']:
            device_sn = device['device_sn']
            device_type = device['type']

            print(f"  - Device SN: {device_sn}, Type: {device_type}")

            # Generate timestamp for filenames
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Process type 7 devices (MIN/TLX inverters)
            if device_type == 7:
                print(f"    Processing MIN/TLX inverter: {device_sn}")

                # Fetch energy data with retry logic
                energy_data = None
                for attempt in range(3):  # 3 retries for individual device calls
                    try:
                        print(f"      Fetching energy data for {device_sn} (attempt {attempt + 1}/3)...")
                        energy_data = api.min_energy(device_sn=device_sn)
                        break  # Success, exit retry loop
                    except Exception as e:
                        error_str = str(e)

                        if "error_frequently_access" in error_str or "10012" in error_str:
                            if attempt < 2:
                                wait_time = 60 * (2 ** attempt)
                                print(f"      ⚠ Rate limit error. Waiting {wait_time}s before retry...")
                                time.sleep(wait_time)
                            else:
                                print(f"      ✗ Failed after 3 attempts due to rate limiting: {e}")
                        else:
                            print(f"      ✗ Failed to fetch energy data: {e}")
                            if hasattr(e, 'error_code') and hasattr(e, 'error_msg'):
                                print(f"        API Error Code: {e.error_code}, Message: {e.error_msg}")
                            break  # Non-rate-limit error, don't retry

                # Save energy data if successfully fetched
                if energy_data:
                    try:
                        energy_filename = f"solarflow_{plant_id}_{device_sn}_{timestamp}.json"
                        energy_file_path = os.path.join(VOLUME_PATH, energy_filename)

                        with open(energy_file_path, 'w') as f:
                            json.dump(energy_data, f, indent=2)

                        print(f"      ✓ Saved energy data to: {energy_filename}")
                        total_files_saved += 1
                    except Exception as e:
                        print(f"      ✗ Failed to save energy data: {e}")

            else:
                print(f"    Skipping device type {device_type} (not yet supported)")

        successful_plants.append({"plant_id": plant_id, "plant_name": plant_name})
        print(f"✓ Completed processing for plant {plant_id}")

    except Exception as e:
        print(f"✗ Error processing plant {plant_id}: {e}")
        failed_plants.append({"plant_id": plant_id, "plant_name": plant_name, "reason": str(e)})
