# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import os
import json
import requests
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("volume_name", "sunpeak_inverters_batch", "Volume name for the API files")
dbutils.widgets.text("secret_scope", "", "Databricks Secret Scope for API credentials")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key="api_key")

BASE_URL = "https://monitoringapi.sunpeak.com"
SITES_LIST_URL = f"{BASE_URL}/sites/list"

# COMMAND ----------

# DBTITLE 1,Create Volume if Not Exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw files from Sunpeak API'
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

# DBTITLE 1,Fetch Device Metadata and Save Raw JSON
all_inverters = []
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

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

        # Fetch equipment list (inverters)
        equipment_list_url = f"{BASE_URL}/equipment/{site_id}/list"
        equipment_response = requests.get(equipment_list_url, params=params)
        equipment_response.raise_for_status()
        equipment_data = equipment_response.json()
        inverters = equipment_data["reporters"]["list"]

        print(f"  Found {equipment_data['reporters']['count']} inverter(s)")

        # Save raw JSON to land layer
        json_filename = f"sunpeak_devices_site_{site_id}_timestamp_{timestamp}.json"
        json_file_path = os.path.join(VOLUME_PATH, json_filename)

        combined_data = {
            "site_details": site_details,
            "equipment": equipment_data
        }

        print(f"  Saving raw JSON to: {json_file_path}")
        with open(json_file_path, 'w') as f:
            json.dump(combined_data, f, indent=2)

        # Process each inverter
        for inverter in inverters:
            manufacturer = inverter.get("manufacturer", "Sunpeak")
            model = inverter.get("model", "")
            serial_number = inverter.get("serial_number", "")

            # Parse model_series and model_sku
            # Example: "SE7600H-US" -> series: "SE7600H", sku: "US"
            # For serial as SKU: "7F181DC8-7C"
            if model:
                # Try to split model
                model_parts = model.split('-', 1) if '-' in model else [model, ""]
                model_series = model_parts[0]
                model_sku = model_parts[1] if len(model_parts) > 1 else serial_number
            else:
                model_series = ""
                model_sku = serial_number

            # Get rated power from kWpDC (if available, convert kW to W)
            kwp_dc = inverter.get("kWpDC")
            rated_power_w = str(float(kwp_dc) * 1000) if kwp_dc else None

            # Sunpeak residential inverters are typically single-phase
            # Three-phase models usually have "3PH" or "SE" prefix with specific model numbers
            phases = "3" if "3PH" in model.upper() or model.startswith("SE33") else "1"

            # Store vendor-specific fields in 'other' JSON
            other_fields = {
                "device_name": inverter.get("name"),
                "kwp_dc": kwp_dc,
                "site_status": site_details.get("status"),
                "site_peak_power": site_details.get("peakPower"),
                "installation_date": site_details.get("installationDate"),
                "site_type": site_details.get("type")
            }

            # Remove None values
            other_fields = {k: v for k, v in other_fields.items() if v is not None}

            # Construct Haystack-compliant foreign keys
            id_site_fk = f"sunpeak-{site_id}"
            id_space_fk = f"sunpeak-{site_id}-space-default"

            inverter_record = {
                "id_site": id_site_fk,
                "id_space": id_space_fk,
                "serial_number": serial_number,
                "brand": manufacturer,
                "model_series": model_series,
                "model_sku": model_sku,
                "product_name": model,
                "product_code": None,  # Sunpeak doesn't have product codes like Voltcore
                "firmware_version": None,  # Not available in equipment list API
                "rated_power_w": rated_power_w,
                "phases": phases,
                "connector": "sunpeak_api",
                "other": json.dumps(other_fields)
            }

            all_inverters.append(inverter_record)
            print(f"    Added inverter: {model} ({serial_number})")

    except Exception as e:
        print(f"  Error fetching devices for site {site_id}: {e}")
        continue

print(f"\n✓ Total inverters collected: {len(all_inverters)}")

# Check if we have any inverters to process
if len(all_inverters) == 0:
    raise ValueError("No inverters collected from Sunpeak API. Please check API credentials, network connectivity, and site configuration.")

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
    F.regexp_extract(F.col("id_site"), r"sunpeak-(\d+)", 1)
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
inverters_df.createOrReplaceTempView("sunpeak_inverters_staging")

merge_sql = f"""
MERGE INTO {gold_table} AS target
USING sunpeak_inverters_staging AS source
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

row_count = spark.table(gold_table).filter(F.col("connector") == "sunpeak_api").count()
print(f"\n✓ Successfully merged {len(all_inverters)} Sunpeak inverter records into {gold_table}")
print(f"  Total Sunpeak inverters in table: {row_count}")

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

# DBTITLE 1,Show Sunpeak Inverter Details
print("\nSunpeak inverter details:")
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
        phases
    FROM {gold_table}
    WHERE brand = 'Sunpeak'
    ORDER BY id_site
""").show(truncate=False)
