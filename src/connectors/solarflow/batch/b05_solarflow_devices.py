# Databricks notebook source
# DBTITLE 1,Install Required Libraries
%pip install solarflowServer

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
import os
import json
import solarflowServer
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("volume_name", "solarflow_inverters_batch", "Volume name for the API files")
dbutils.widgets.text("secret_scope", "", "Databricks Secret Scope for API credentials")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key="api_key")

# COMMAND ----------

# DBTITLE 1,Rate Limit Protection - Wait Before API Calls
import time

# Wait 5 minutes before making API calls to avoid rate limiting
# This task runs in parallel with b02_land2bronze, so we need to space out API requests
RATE_LIMIT_DELAY_SECONDS = 300  # 5 minutes
print(f"⏳ Waiting {RATE_LIMIT_DELAY_SECONDS} seconds ({RATE_LIMIT_DELAY_SECONDS/60:.1f} minutes) to avoid Solarflow API rate limits...")
time.sleep(RATE_LIMIT_DELAY_SECONDS)
print("✓ Rate limit delay complete. Proceeding with API calls.")

# COMMAND ----------

# DBTITLE 1,Create Volume if Not Exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw files from Solarflow API'
""")

# COMMAND ----------

# DBTITLE 1,Initialize Solarflow API with Retry Logic
import time

def initialize_api_with_retry(token, max_retries=5, initial_wait=120):
    """Initialize Solarflow API with retry logic for rate limiting errors."""
    for attempt in range(max_retries):
        try:
            api = solarflowServer.OpenApiV1(token=token)
            print(f"✓ Successfully initialized Solarflow API (attempt {attempt + 1}/{max_retries})")
            return api
        except Exception as e:
            error_str = str(e)
            if "error_frequently_access" in error_str or "10012" in error_str:
                wait_time = initial_wait * (2 ** attempt)
                if attempt < max_retries - 1:
                    print(f"⚠ Rate limit error detected (error_code: 10012)")
                    print(f"  Waiting {wait_time} seconds before retry {attempt + 2}/{max_retries}...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed to initialize Solarflow API after {max_retries} attempts.")
            else:
                raise Exception(f"Failed to initialize Solarflow API: {e}")
    raise Exception(f"Failed to initialize Solarflow API after {max_retries} attempts")

def api_call_with_retry(api_function, *args, max_retries=5, initial_wait=120, **kwargs):
    """Wrapper for API calls with retry logic for rate limiting."""
    for attempt in range(max_retries):
        try:
            result = api_function(*args, **kwargs)
            return result
        except Exception as e:
            error_str = str(e)
            if "error_frequently_access" in error_str or "10012" in error_str or "rate limit" in error_str.lower():
                wait_time = initial_wait * (2 ** attempt)
                if attempt < max_retries - 1:
                    print(f"⚠ Rate limit error on API call (error_code: 10012)")
                    print(f"  Waiting {wait_time} seconds before retry {attempt + 2}/{max_retries}...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"API call failed after {max_retries} attempts: {e}")
            else:
                raise Exception(f"API call failed: {e}")
    raise Exception(f"API call failed after {max_retries} attempts")

api = initialize_api_with_retry(API_KEY)

# COMMAND ----------

# DBTITLE 1,Fetch All Plants/Sites
plants = api_call_with_retry(api.plant_list)
print(f"Found {plants['count']} plant(s):")
for plant in plants['plants']:
    print(f"  - Plant ID {plant['plant_id']}: {plant.get('plant_name', 'Unnamed')}")

# COMMAND ----------

# DBTITLE 1,Fetch Device Metadata and Save Raw JSON
all_inverters = []
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

for plant in plants['plants']:
    plant_id = plant['plant_id']
    plant_name = plant.get('plant_name', 'Unnamed Plant')

    print(f"\n--- Processing Plant: '{plant_name}' (ID: {plant_id}) ---")

    try:
        # Get devices for the current plant with retry
        devices = api_call_with_retry(api.device_list, plant_id)
        print(f"  Found {devices['count']} device(s)")

        # Save raw JSON to land layer
        json_filename = f"solarflow_devices_site_{plant_id}_timestamp_{timestamp}.json"
        json_file_path = os.path.join(VOLUME_PATH, json_filename)

        print(f"  Saving raw JSON to: {json_file_path}")
        with open(json_file_path, 'w') as f:
            json.dump(devices, f, indent=2)

        # Process type 7 devices (MIN/TLX inverters)
        for device in devices['devices']:
            device_sn = device['device_sn']
            device_type = device['type']

            if device_type == 7:
                print(f"  Processing MIN/TLX inverter: {device_sn}")

                try:
                    # Get detailed inverter information with retry
                    inverter_data = api_call_with_retry(api.min_detail, device_sn)

                    # Hardcode manufacturer as Solarflow (API returns generic "PV Inverter")
                    manufacturer = "Solarflow"
                    model = inverter_data.get("modelText", "")

                    # Parse model_series and model_sku
                    # Example model: "MIC 60000TL3-X LV" -> series: "MIC", sku: "60000TL3-X LV"
                    model_parts = model.split(' ', 1) if model else ["", ""]
                    model_series = model_parts[0] if model_parts else ""
                    model_sku = model_parts[1] if len(model_parts) > 1 else ""

                    # Get rated power
                    max_power_w = inverter_data.get("pmax")

                    # Determine phases (Solarflow 3-phase models typically have TL3 in name)
                    phases = "3" if "TL3" in model.upper() or "3PH" in model.upper() else "1"

                    # Store vendor-specific fields in 'other' JSON
                    other_fields = {
                        "device_type_code": int(device_type),
                        "communication_version": inverter_data.get("communicationVersion"),
                        "inner_version": inverter_data.get("innerVersion"),
                        "datalog_sn": inverter_data.get("dataLogSn"),
                        "status": inverter_data.get("status"),
                        "status_text": inverter_data.get("statusText"),
                        "last_update_time": inverter_data.get("lastUpdateTimeText"),
                        "timezone": inverter_data.get("timezone"),
                        "location": inverter_data.get("location"),
                        "modbus_version": inverter_data.get("modbusVersion"),
                        "mppt": inverter_data.get("mppt"),
                        "country_selected": inverter_data.get("countrySelected")
                    }

                    # Remove None values
                    other_fields = {k: v for k, v in other_fields.items() if v is not None}

                    # Construct Haystack-compliant foreign keys
                    id_site_fk = f"solarflow-{plant_id}"
                    id_space_fk = f"solarflow-{plant_id}-space-default"

                    inverter_record = {
                        "id_site": id_site_fk,
                        "id_space": id_space_fk,
                        "serial_number": device_sn,
                        "brand": manufacturer,
                        "model_series": model_series,
                        "model_sku": model_sku,
                        "product_name": model,
                        "product_code": None,  # Solarflow doesn't have product codes like Voltcore
                        "firmware_version": inverter_data.get("fwVersion"),
                        "rated_power_w": str(max_power_w) if max_power_w else None,  # Keep as string
                        "phases": phases,
                        "connector": "solarflow_api",
                        "other": json.dumps(other_fields)
                    }

                    all_inverters.append(inverter_record)
                    print(f"    Added inverter: {model}")

                except Exception as e:
                    print(f"    Error processing inverter {device_sn}: {e}")

    except Exception as e:
        print(f"  Error fetching devices for plant {plant_id}: {e}")
        continue

print(f"\n✓ Total inverters collected: {len(all_inverters)}")

# Check if we have any inverters to process
if len(all_inverters) == 0:
    raise ValueError("No inverters collected from Solarflow API. Please check API credentials, network connectivity, and site configuration.")

# COMMAND ----------

# DBTITLE 1,Create DataFrame and Transform to Gold Schema
# Define schema (all strings initially to avoid type inference issues)
schema = StructType([
    StructField("connector", StringType(), True),
    StructField("id_site", StringType(), True),
    StructField("id_space", StringType(), True),
    StructField("serial_number", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("model_series", StringType(), True),
    StructField("model_sku", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_code", StringType(), True),
    StructField("firmware_version", StringType(), True),
    StructField("rated_power_w", StringType(), True),
    StructField("phases", StringType(), True),
    StructField("other", StringType(), True)
])

# Create DataFrame with explicit string schema
inverters_df = spark.createDataFrame(all_inverters, schema)

# Generate surrogate keys
# 1. Equipment spec SK
inverters_df = inverters_df.withColumn(
    "sk_inverters_spec",
    F.md5(F.concat(F.col("brand"), F.col("model_series"), F.col("model_sku")))
)

# 2. Device SK
inverters_df = inverters_df.withColumn(
    "sk_inverter",
    F.md5(F.concat(
        F.col("connector"),
        F.lit("_"),
        F.col("id_site"),
        F.lit("_"),
        F.col("serial_number")
    ))
)

# 3. Site and Space SKs
inverters_df = inverters_df.withColumn(
    "source_site_id",
    F.regexp_extract(F.col("id_site"), r"solarflow-(\d+)", 1)
)

inverters_df = inverters_df.withColumn(
    "sk_site",
    F.md5(F.concat(F.col("connector"), F.lit("_"), F.col("source_site_id")))
)

inverters_df = inverters_df.withColumn(
    "sk_space",
    F.md5(F.concat(F.col("sk_site"), F.lit("_default")))
)

# Cast rated_power_w to DECIMAL
inverters_df = inverters_df.withColumn(
    "rated_power_w",
    F.col("rated_power_w").cast(DecimalType(10, 2))
)

# Cast phases to INT
inverters_df = inverters_df.withColumn("phases", F.col("phases").cast(IntegerType()))

# Add metadata timestamp
inverters_df = inverters_df.withColumn("metadata_updated_at", F.current_timestamp())

# COMMAND ----------

# DBTITLE 1,Create Gold Schema if Not Exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.gold COMMENT 'Gold layer - business-ready dimensional and fact tables'")

# COMMAND ----------

# DBTITLE 1,Write to Generic d_inverters Table
gold_table = f"{CATALOG_NAME}.gold.d_inverters"

# Check if table exists
table_exists = spark.catalog.tableExists(gold_table)

if not table_exists:
    print(f"Creating generic dimension table: {gold_table}")

    spark.sql(f"""
    CREATE TABLE {gold_table} (
        -- Device Surrogate Key (Primary Key)
        sk_inverter STRING COMMENT 'Device surrogate key - MD5(connector + id_site + serial_number)',

        -- Surrogate Key Foreign Keys
        sk_site STRING COMMENT 'Foreign key to dim_site.sk_site',
        sk_space STRING COMMENT 'Foreign key to dim_space.sk_space',
        sk_inverters_spec STRING COMMENT 'Foreign key to d_inverters metadata spec table',

        -- Haystack Natural Keys (Ontology Compliance)
        id_site STRING COMMENT 'Natural key to dim_site.id_site',
        id_space STRING COMMENT 'Natural key to dim_space.id_space',

        -- Device Identification
        serial_number STRING COMMENT 'Device serial number (unique identifier)',

        -- Brand/Manufacturer
        brand STRING COMMENT 'Inverter manufacturer (Voltcore, Sunpeak, Solarflow, etc.)',

        -- Model Information (Generic across all brands)
        model_series STRING COMMENT 'Model series (e.g., MultiPlus-II, SE7600H, MIC)',
        model_sku STRING COMMENT 'Model SKU/variant (e.g., 48/5000/70-50, 7f181dc8-7c)',
        product_name STRING COMMENT 'Full product name',
        product_code STRING COMMENT 'Vendor product code',

        -- Technical Specifications (Common fields)
        rated_power_w DECIMAL(10,2) COMMENT 'Rated AC power output in Watts',
        phases INT COMMENT 'Number of phases (1 for single-phase, 3 for three-phase)',

        -- Version/Firmware
        firmware_version STRING COMMENT 'Current firmware version',

        -- Voltcore-specific fields
        device_instance STRING COMMENT 'Voltcore device instance ID (e.g., 2611, 2623)',

        -- Metadata
        connector STRING COMMENT 'Data source connector (voltcore_api, sunpeak_api, solarflow_api)',
        other STRING COMMENT 'JSON string for vendor-specific fields',
        metadata_updated_at TIMESTAMP COMMENT 'Last metadata update timestamp'
    )
    CLUSTER BY (brand, model_series)
    COMMENT 'd_inverters: Generic dimension table for all inverters across all connectors (Voltcore, Sunpeak, Solarflow). Cross-connector analytics ready.'
    """)

    print(f"✓ Created table: {gold_table}")
else:
    # Migrate existing table: Add new columns if they don't exist
    print(f"Table {gold_table} exists. Checking for schema migration...")

    existing_columns = [field.name for field in spark.table(gold_table).schema.fields]

    # Add new surrogate key columns if they don't exist
    if "sk_inverter" not in existing_columns:
        print("  Adding column: sk_inverter")
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN sk_inverter STRING COMMENT 'Device surrogate key - MD5(connector + id_site + serial_number)'")

    if "sk_site" not in existing_columns:
        print("  Adding column: sk_site")
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN sk_site STRING COMMENT 'Foreign key to dim_site.sk_site'")

    if "sk_space" not in existing_columns:
        print("  Adding column: sk_space")
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN sk_space STRING COMMENT 'Foreign key to dim_space.sk_space'")

    if "sk_inverters_spec" not in existing_columns:
        print("  Adding column: sk_inverters_spec")
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN sk_inverters_spec STRING COMMENT 'Foreign key to d_inverters metadata spec table'")

    if "id_space" not in existing_columns:
        print("  Adding column: id_space")
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN id_space STRING COMMENT 'Natural key to dim_space.id_space'")

    if "device_instance" not in existing_columns:
        print("  Adding column: device_instance")
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN device_instance STRING COMMENT 'Voltcore device instance ID (e.g., 2611, 2623)'")

    print("  ✓ Schema migration complete")

# COMMAND ----------

# DBTITLE 1,Merge Into d_inverters
# Use MERGE to handle updates for existing devices and inserts for new ones
inverters_df.createOrReplaceTempView("solarflow_inverters_staging")

merge_sql = f"""
MERGE INTO {gold_table} AS target
USING solarflow_inverters_staging AS source
ON target.sk_inverter = source.sk_inverter
WHEN MATCHED THEN
  UPDATE SET
    target.sk_site = source.sk_site,
    target.sk_space = source.sk_space,
    target.sk_inverters_spec = source.sk_inverters_spec,
    target.id_site = source.id_site,
    target.id_space = source.id_space,
    target.serial_number = source.serial_number,
    target.product_name = source.product_name,
    target.product_code = source.product_code,
    target.firmware_version = source.firmware_version,
    target.rated_power_w = source.rated_power_w,
    target.phases = source.phases,
    target.other = source.other,
    target.metadata_updated_at = source.metadata_updated_at
WHEN NOT MATCHED THEN
  INSERT (
    sk_inverter,
    sk_site,
    sk_space,
    sk_inverters_spec,
    id_site,
    id_space,
    serial_number,
    brand,
    model_series,
    model_sku,
    product_name,
    product_code,
    rated_power_w,
    phases,
    firmware_version,
    connector,
    other,
    metadata_updated_at
  )
  VALUES (
    source.sk_inverter,
    source.sk_site,
    source.sk_space,
    source.sk_inverters_spec,
    source.id_site,
    source.id_space,
    source.serial_number,
    source.brand,
    source.model_series,
    source.model_sku,
    source.product_name,
    source.product_code,
    source.rated_power_w,
    source.phases,
    source.firmware_version,
    source.connector,
    source.other,
    source.metadata_updated_at
  )
"""

spark.sql(merge_sql)

row_count = spark.table(gold_table).filter(F.col("connector") == "solarflow_api").count()
print(f"\n✓ Successfully merged {len(all_inverters)} Solarflow inverter records into {gold_table}")
print(f"  Total Solarflow inverters in table: {row_count}")

# COMMAND ----------

# DBTITLE 1,Show Summary by Brand
print("\nInverters per brand:")
spark.sql(f"""
    SELECT
        brand,
        connector,
        COUNT(*) as inverter_count,
        COUNT(DISTINCT model_series) as unique_models
    FROM {gold_table}
    GROUP BY brand, connector
    ORDER BY brand, connector
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Show Solarflow Inverter Details
print("\nSolarflow inverter details:")
spark.sql(f"""
    SELECT
        sk_inverter,
        sk_site,
        sk_space,
        id_site,
        id_space,
        serial_number,
        model_series,
        model_sku,
        rated_power_w,
        phases,
        firmware_version
    FROM {gold_table}
    WHERE brand = 'Solarflow'
    ORDER BY id_site
""").show(truncate=False)
