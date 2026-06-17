# Databricks notebook source
# DBTITLE 1,Install Required Libraries
%pip install solarflowServer

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
import solarflowServer
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("table_name", "solarflow_devices", "Metadata Table Name")
TABLE_NAME = dbutils.widgets.get("table_name")

API_KEY = dbutils.secrets.get(scope="globex_solarflow_api_creds", key="api_key")

# COMMAND ----------

# DBTITLE 1,DDL Statement
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.metadata.{TABLE_NAME} (
    id_site STRING COMMENT 'Solarflow plant/site ID',
    site_name STRING COMMENT 'Plant/site name',
    device_sn STRING COMMENT 'Device serial number',
    device_alias STRING COMMENT 'Device alias name',
    device_type STRING COMMENT 'Device type (e.g., Inverter)',
    device_type_code INT COMMENT 'Numeric device type code from API',
    manufacturer STRING COMMENT 'Device manufacturer',
    model STRING COMMENT 'Device model',
    firmware_version STRING COMMENT 'Firmware version',
    communication_version STRING COMMENT 'Communication version',
    inner_version STRING COMMENT 'Inner firmware version',
    datalog_sn STRING COMMENT 'Data logger serial number',
    max_power_w DOUBLE COMMENT 'Maximum power in watts',
    status INT COMMENT 'Device status code',
    status_text STRING COMMENT 'Device status description',
    last_update_time STRING COMMENT 'Last update timestamp from device',
    timezone INT COMMENT 'Timezone offset',
    location STRING COMMENT 'Device installation location',
    other STRING COMMENT 'JSON string with additional device information',
    metadata_updated_at TIMESTAMP COMMENT 'When this metadata was last updated'
)
CLUSTER BY AUTO
COMMENT 'Device metadata for Solarflow installations, refreshed periodically'
""")

# COMMAND ----------

# DBTITLE 1,Initialize Solarflow API
import time

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

# DBTITLE 1,Fetch All Plants/Sites
try:
    plants = api.plant_list()
    print(f"Found {plants['count']} plant(s):")

    for plant in plants['plants']:
        plant_id = plant['plant_id']
        plant_name = plant.get('plant_name', 'Unnamed Plant')
        print(f"  - Plant ID {plant_id}: {plant_name}")

except Exception as e:
    raise Exception(f"Failed to fetch plant list: {e}")

# COMMAND ----------

# DBTITLE 1,Fetch Device Metadata for Each Plant
all_devices = []

for plant in plants['plants']:
    plant_id = plant['plant_id']
    plant_name = plant.get('plant_name', 'Unnamed Plant')

    print(f"\n--- Processing Plant: '{plant_name}' (ID: {plant_id}) ---")

    try:
        # Get devices for the current plant
        devices = api.device_list(plant_id)
        print(f"  Found {devices['count']} device(s) in this plant")

        for device in devices['devices']:
            device_sn = device['device_sn']
            device_type = device['type']

            print(f"  - Device SN: {device_sn}, Type: {device_type}")

            # Process type 7 devices (MIN/TLX inverters)
            if device_type == 7:
                print(f"    Processing MIN/TLX inverter: {device_sn}")

                try:
                    # Get detailed inverter information
                    inverter_data = api.min_detail(device_sn)

                    # Extract core metadata fields
                    device_record = {
                        "id_site": str(plant_id),
                        "site_name": plant_name,
                        "device_sn": inverter_data.get("serialNum"),
                        "device_alias": inverter_data.get("alias"),
                        "device_type": "Inverter",
                        "device_type_code": int(device_type),
                        "manufacturer": inverter_data.get("manufacturer", "").strip(),
                        "model": inverter_data.get("modelText"),
                        "firmware_version": inverter_data.get("fwVersion"),
                        "communication_version": inverter_data.get("communicationVersion"),
                        "inner_version": inverter_data.get("innerVersion"),
                        "datalog_sn": inverter_data.get("dataLogSn"),
                        "max_power_w": float(inverter_data.get("pmax", 0)) if inverter_data.get("pmax") is not None else None,
                        "status": int(inverter_data.get("status")) if inverter_data.get("status") is not None else None,
                        "status_text": inverter_data.get("statusText"),
                        "timezone": int(inverter_data.get("timezone")) if inverter_data.get("timezone") is not None else None,
                        "location": inverter_data.get("location")
                    }

                    # Handle lastUpdateTime (can be dict or string)
                    last_update = inverter_data.get("lastUpdateTime")
                    if isinstance(last_update, dict):
                        device_record["last_update_time"] = last_update.get("lastUpdateTimeText")
                    else:
                        device_record["last_update_time"] = inverter_data.get("lastUpdateTimeText")

                    # Store additional fields as JSON in "other"
                    other_fields = {
                        "modbusVersion": inverter_data.get("modbusVersion"),
                        "mppt": inverter_data.get("mppt"),
                        "countrySelected": inverter_data.get("countrySelected"),
                        "dtc": inverter_data.get("dtc"),
                        "priorityChoose": inverter_data.get("priorityChoose"),
                        "restartTime": inverter_data.get("restartTime"),
                        "startTime": inverter_data.get("startTime"),
                        "batteryType": inverter_data.get("batteryType"),
                        "batParallelNum": inverter_data.get("batParallelNum"),
                        "batSeriesNum": inverter_data.get("batSeriesNum"),
                        "batSysEnergy": inverter_data.get("batSysEnergy"),
                        "tcpServerIp": inverter_data.get("tcpServerIp")
                    }

                    # Remove None values
                    other_fields = {k: v for k, v in other_fields.items() if v is not None}
                    device_record["other"] = json.dumps(other_fields) if other_fields else None

                    all_devices.append(device_record)
                    print(f"    Successfully processed inverter {device_sn}")

                except Exception as e:
                    print(f"    Error processing inverter {device_sn}: {e}")
                    continue

            # TODO: Add support for other device types (type 1, 2, 3, etc.) as needed
            else:
                print(f"    Skipping device type {device_type} (not yet supported)")

    except Exception as e:
        print(f"  Error processing plant {plant_id}: {e}")
        continue

print(f"\n=== Total devices collected: {len(all_devices)} ===")

# COMMAND ----------

# DBTITLE 1,Create DataFrame
schema = StructType([
    StructField("id_site", StringType(), True),
    StructField("site_name", StringType(), True),
    StructField("device_sn", StringType(), True),
    StructField("device_alias", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("device_type_code", IntegerType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("model", StringType(), True),
    StructField("firmware_version", StringType(), True),
    StructField("communication_version", StringType(), True),
    StructField("inner_version", StringType(), True),
    StructField("datalog_sn", StringType(), True),
    StructField("max_power_w", DoubleType(), True),
    StructField("status", IntegerType(), True),
    StructField("status_text", StringType(), True),
    StructField("last_update_time", StringType(), True),
    StructField("timezone", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("other", StringType(), True)
])

devices_df = spark.createDataFrame(all_devices, schema)

# Add metadata timestamp
devices_df = devices_df.withColumn("metadata_updated_at", F.current_timestamp())

# COMMAND ----------

# DBTITLE 1,Write to Metadata Table
metadata_table = f"{CATALOG_NAME}.metadata.{TABLE_NAME}"

# Overwrite table with latest metadata
# Use overwriteSchema to ensure the table schema matches the DataFrame schema
devices_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(metadata_table)

row_count = spark.table(metadata_table).count()
print(f"\n✓ Successfully wrote {row_count} device records to {metadata_table}")

# COMMAND ----------

# DBTITLE 1,Show Device Summary
print("\nSolarflow Device Summary:")
spark.sql(f"""
    SELECT
        id_site,
        site_name,
        device_sn,
        device_alias,
        manufacturer,
        model,
        firmware_version,
        max_power_w,
        status_text
    FROM {metadata_table}
    ORDER BY device_sn
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Show Device Details
print("\nSolarflow Device Technical Details:")
spark.sql(f"""
    SELECT
        device_sn,
        device_alias,
        firmware_version,
        communication_version,
        datalog_sn,
        timezone,
        last_update_time
    FROM {metadata_table}
    ORDER BY device_sn
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Show Additional Information
print("\nAdditional Device Information (from 'other' field):")
spark.sql(f"""
    SELECT
        device_sn,
        get_json_object(other, '$.modbusVersion') as modbus_version,
        get_json_object(other, '$.mppt') as mppt,
        get_json_object(other, '$.batteryType') as battery_type,
        get_json_object(other, '$.batParallelNum') as bat_parallel_num,
        get_json_object(other, '$.batSeriesNum') as bat_series_num,
        get_json_object(other, '$.tcpServerIp') as tcp_server_ip
    FROM {metadata_table}
    ORDER BY device_sn
""").show(truncate=False)
