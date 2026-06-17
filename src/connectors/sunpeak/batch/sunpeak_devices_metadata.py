# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("table_name", "sunpeak_devices", "Metadata Table Name")
TABLE_NAME = dbutils.widgets.get("table_name")

API_KEY = dbutils.secrets.get(scope="globex_sunpeak_api_creds", key="api_key")

BASE_URL = "https://monitoringapi.sunpeak.com"
SITES_LIST_URL = f"{BASE_URL}/sites/list"

# COMMAND ----------

# DBTITLE 1,DDL Statement
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.metadata.{TABLE_NAME} (
    id_site STRING COMMENT 'Sunpeak site ID',
    site_name STRING COMMENT 'Site name',
    device_name STRING COMMENT 'Device name (e.g., Inverter 1)',
    manufacturer STRING COMMENT 'Device manufacturer (e.g., Sunpeak)',
    model STRING COMMENT 'Device model (e.g., SE5000)',
    serial_number STRING COMMENT 'Device serial number',
    kwp_dc DOUBLE COMMENT 'DC capacity in kWp',
    device_type STRING COMMENT 'Type of device (e.g., Inverter)',
    site_status STRING COMMENT 'Site status (Active, Pending, etc.)',
    site_peak_power DOUBLE COMMENT 'Site peak power in kW',
    installation_date STRING COMMENT 'Site installation date',
    pto_date STRING COMMENT 'Permission to operate date',
    site_type STRING COMMENT 'Type of solar installation',
    data_period_start STRING COMMENT 'Start date of available data',
    data_period_end STRING COMMENT 'End date of available data',
    location_info STRING COMMENT 'JSON string with location details (country, city, address, coordinates, etc.)',
    primary_module_info STRING COMMENT 'JSON string with primary module/panel information',
    other STRING COMMENT 'JSON string with additional site information',
    metadata_updated_at TIMESTAMP COMMENT 'When this metadata was last updated'
)
CLUSTER BY AUTO
COMMENT 'Device metadata for Sunpeak installation, refreshed periodically'
""")

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

# DBTITLE 1,Fetch Device Metadata for Each Site
all_devices = []

for site_id in site_ids:
    print(f"\n--- Processing Site ID: {site_id} ({site_names[site_id]}) ---")

    try:
        # Fetch site details
        site_details_url = f"{BASE_URL}/site/{site_id}/details"
        site_details_response = requests.get(site_details_url, params=params)
        site_details_response.raise_for_status()
        site_details = site_details_response.json()["details"]

        print(f"  Status: {site_details['status']}")
        print(f"  Peak Power: {site_details['peakPower']} kW")
        print(f"  Type: {site_details['type']}")

        # Fetch data period
        site_data_period_url = f"{BASE_URL}/site/{site_id}/dataPeriod"
        data_period_response = requests.get(site_data_period_url, params=params)
        data_period_response.raise_for_status()
        data_period = data_period_response.json()["dataPeriod"]

        print(f"  Data Period: {data_period.get('startDate')} to {data_period.get('endDate')}")

        # Fetch equipment list (inverters)
        equipment_list_url = f"{BASE_URL}/equipment/{site_id}/list"
        equipment_response = requests.get(equipment_list_url, params=params)
        equipment_response.raise_for_status()
        equipment_data = equipment_response.json()
        inverters = equipment_data["reporters"]["list"]

        print(f"  Found {equipment_data['reporters']['count']} inverter(s)")

        # Process each inverter
        for inverter in inverters:
            device_record = {
                "id_site": str(site_details["id"]),
                "site_name": site_details["name"],
                "device_name": inverter["name"],
                "manufacturer": inverter["manufacturer"],
                "model": inverter["model"],
                "serial_number": inverter["serialNumber"],
                "kwp_dc": inverter.get("kWpDC"),
                "device_type": "Inverter",
                "site_status": site_details["status"],
                "site_peak_power": site_details["peakPower"],
                "installation_date": site_details["installationDate"],
                "pto_date": site_details.get("ptoDate"),
                "site_type": site_details["type"],
                "data_period_start": data_period.get("startDate"),
                "data_period_end": data_period.get("endDate")
            }

            # Store location information as JSON
            location_info = {
                "country": site_details["location"]["country"],
                "city": site_details["location"]["city"],
                "address": site_details["location"]["address"],
                "address2": site_details["location"]["address2"],
                "zip": site_details["location"]["zip"],
                "timezone": site_details["location"]["timeZone"],
                "country_code": site_details["location"]["countryCode"],
                "latitude": site_details["location"]["latitude"],
                "longitude": site_details["location"]["longitude"]
            }
            device_record["location_info"] = json.dumps(location_info)

            # Store primary module information as JSON
            primary_module = site_details.get("primaryModule", {})
            if primary_module:
                module_info = {
                    "manufacturer_name": primary_module.get("manufacturerName"),
                    "model_name": primary_module.get("modelName"),
                    "maximum_power": primary_module.get("maximumPower"),
                    "temperature_coef": primary_module.get("temperatureCoef")
                }
                device_record["primary_module_info"] = json.dumps(module_info)
            else:
                device_record["primary_module_info"] = None

            # Store other site-level information as JSON
            other_info = {
                "account_id": site_details["accountId"],
                "notes": site_details.get("notes"),
                "alert_quantity": site_details.get("alertQuantity"),
                "highest_impact": site_details.get("highestImpact")
            }
            device_record["other"] = json.dumps(other_info)

            all_devices.append(device_record)

    except Exception as e:
        print(f"  Error processing site {site_id}: {e}")
        continue

print(f"\n=== Total devices collected: {len(all_devices)} ===")

# COMMAND ----------

# DBTITLE 1,Create DataFrame
schema = StructType([
    StructField("id_site", StringType(), True),
    StructField("site_name", StringType(), True),
    StructField("device_name", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("model", StringType(), True),
    StructField("serial_number", StringType(), True),
    StructField("kwp_dc", DoubleType(), True),
    StructField("device_type", StringType(), True),
    StructField("site_status", StringType(), True),
    StructField("site_peak_power", DoubleType(), True),
    StructField("installation_date", StringType(), True),
    StructField("pto_date", StringType(), True),
    StructField("site_type", StringType(), True),
    StructField("data_period_start", StringType(), True),
    StructField("data_period_end", StringType(), True),
    StructField("location_info", StringType(), True),
    StructField("primary_module_info", StringType(), True),
    StructField("other", StringType(), True)
])

devices_df = spark.createDataFrame(all_devices, schema)

# Add metadata timestamp
devices_df = devices_df.withColumn("metadata_updated_at", F.current_timestamp())

# COMMAND ----------

# DBTITLE 1,Write to Metadata Table
metadata_table = f"{CATALOG_NAME}.metadata.{TABLE_NAME}"

# Overwrite table with latest metadata
devices_df.write.mode("overwrite").saveAsTable(metadata_table)

row_count = spark.table(metadata_table).count()
print(f"\n✓ Successfully wrote {row_count} device records to {metadata_table}")

# COMMAND ----------

# DBTITLE 1,Show Device Summary
print("\nSunpeak Device Summary:")
spark.sql(f"""
    SELECT
        id_site,
        site_name,
        device_name,
        manufacturer,
        model,
        serial_number,
        site_peak_power,
        installation_date,
        site_status
    FROM {metadata_table}
    ORDER BY device_name
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Show Location Information
print("\nSite Location Information:")
spark.sql(f"""
    SELECT
        id_site,
        site_name,
        get_json_object(location_info, '$.country') as country,
        get_json_object(location_info, '$.city') as city,
        get_json_object(location_info, '$.address') as address,
        get_json_object(location_info, '$.latitude') as latitude,
        get_json_object(location_info, '$.longitude') as longitude,
        get_json_object(location_info, '$.timezone') as timezone
    FROM {metadata_table}
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Show Data Period
print("\nData Availability Period:")
spark.sql(f"""
    SELECT
        id_site,
        site_name,
        data_period_start,
        data_period_end,
        DATEDIFF(TO_DATE(data_period_end, 'yyyy-MM-dd'), TO_DATE(data_period_start, 'yyyy-MM-dd')) as days_of_data
    FROM {metadata_table}
""").show(truncate=False)
