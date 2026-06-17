# Databricks notebook source
# DBTITLE 1,Imports and Configuration
import os
import json
import requests
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("volume_name", "voltcore_inverters_batch", "Volume name for the API files")
dbutils.widgets.text("secret_scope", "", "Databricks Secret Scope for API credentials")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

USERNAME = dbutils.secrets.get(scope=SECRET_SCOPE, key="username")
PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="password")

LOGIN_URL = "https://vrmapi.voltcoreenergy.com/v2/auth/login"
USER_INSTALLATIONS_URL = "https://vrmapi.voltcoreenergy.com/v2/users/{idUser}/installations"
SYSTEM_OVERVIEW_URL = "https://vrmapi.voltcoreenergy.com/v2/installations/{id_site}/system-overview"

# COMMAND ----------

# DBTITLE 1,Create Volume if Not Exists
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw files from Voltcore API'
""")

# COMMAND ----------

# DBTITLE 1,Login and Get Bearer Token
login_payload = {
    "username": USERNAME,
    "password": PASSWORD,
    "remember_me": False
}

login_response = requests.post(LOGIN_URL, json=login_payload)
login_response.raise_for_status()

token_data = login_response.json()
bearer_token = token_data["token"]
user_id = token_data["idUser"]

headers = {
    "x-authorization": f"Bearer {bearer_token}"
}

print(f"Successfully authenticated as user ID: {user_id}")

# COMMAND ----------

# DBTITLE 1,Get All Site IDs
installations_url = USER_INSTALLATIONS_URL.format(idUser=user_id)
installations_response = requests.get(installations_url, headers=headers)
installations_response.raise_for_status()

installations_data = installations_response.json()
site_ids = [str(record["id_site"]) for record in installations_data["records"]]

print(f"Found {len(site_ids)} installations: {site_ids}")

# COMMAND ----------

# DBTITLE 1,Fetch Device Metadata and Save Raw JSON
all_inverters = []
all_batteries = []
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

for site_id in site_ids:
    print(f"\nFetching devices for site {site_id}...")

    try:
        system_overview_url = SYSTEM_OVERVIEW_URL.format(id_site=site_id)
        response = requests.get(system_overview_url, headers=headers)
        response.raise_for_status()

        system_data = response.json()
        devices = system_data["records"]["devices"]

        print(f"  Found {len(devices)} devices")

        # Save raw JSON to land layer
        json_filename = f"voltcore_devices_site_{site_id}_timestamp_{timestamp}.json"
        json_file_path = os.path.join(VOLUME_PATH, json_filename)

        print(f"  Saving raw JSON to: {json_file_path}")
        with open(json_file_path, 'w') as f:
            json.dump(system_data, f, indent=2)

        # Process VE.Bus System devices (inverters) for d_inverters
        for device in devices:
            device_name = device.get("name", "")

            if device_name == "VE.Bus System":
                product_name = device.get("product_name", "")

                # Parse model_series and model_sku from product_name
                # Example: "MultiPlus-II 48/5000/70-50"
                parts = product_name.split(' ', 1)
                model_series = parts[0] if parts else product_name
                model_sku = parts[1] if len(parts) > 1 else ""

                # Keep SKU as-is (string) - parsing happens in gold layer
                # Format: {voltage}/{power}/{charge_current}/{transfer_switch}
                # Example: "48/5000/70-50"

                # Store phases as string for now
                vid = device.get("vid", {})
                devices_per_phase = vid.get("devicesPerPhase", {})
                l1 = devices_per_phase.get("L1", 0)
                l2 = devices_per_phase.get("L2", 0)
                l3 = devices_per_phase.get("L3", 0)
                phases = "1" if (l1 > 0 and l2 == 0 and l3 == 0) else "3"

                # Extract device instance (used for joining with telemetry data)
                # Use product_code since silver tables are organized by product code (e.g., voltcore_2623_batch)
                # The API's "instance" field is 0 for multiple devices, so it's not unique
                device_instance = str(device.get("product_code", ""))

                # Store vendor-specific fields in 'other' JSON
                other_fields = {
                    "instance": device.get("instance"),
                    "id_device_type": device.get("idDeviceType"),
                    "device_class": device.get("class"),
                    "last_connection": device.get("lastConnection"),
                    "vid": vid,
                    "vmc": device.get("vmc"),
                    "product_id": device.get("productId")
                }

                # Construct Haystack-compliant foreign keys
                id_site_fk = f"voltcore-{site_id}"
                id_space_fk = f"voltcore-{site_id}-space-default"

                inverter_record = {
                    "id_site": id_site_fk,
                    "id_space": id_space_fk,
                    "serial_number": None,  # Voltcore doesn't expose serial in system-overview
                    "brand": "Voltcore",
                    "model_series": model_series,
                    "model_sku": model_sku,
                    "product_name": product_name,
                    "product_code": device.get("product_code"),
                    "firmware_version": device.get("firmware_version"),
                    "phases": phases,  # String: "1" or "3"
                    "device_instance": device_instance,
                    "connector": "voltcore_api",
                    "other": json.dumps(other_fields)
                }

                all_inverters.append(inverter_record)
                print(f"    Added inverter: {product_name}")

            elif "Battery Monitor" in device_name or "Lynx Smart BMS" in device_name or "SmartShunt" in device_name:
                # Process battery monitoring devices for d_batteries
                product_name = device.get("product_name", "")
                product_code = device.get("product_code", "")

                # Parse model_series and model_sku from product_name
                # Examples: "SmartShunt 500A", "Lynx Smart BMS 500A"
                parts = product_name.split(' ', 1) if product_name else ["", ""]
                model_series = parts[0] if parts else ""
                model_sku = parts[1] if len(parts) > 1 else ""

                # Try to extract current rating from model_sku (e.g., "500A" from "SmartShunt 500A")
                # This will be used to populate rated_capacity_ah in gold layer
                rated_capacity_ah = None
                if model_sku and 'A' in model_sku:
                    # Extract number before 'A' (e.g., "500A" -> 500)
                    import re
                    match = re.search(r'(\d+)A', model_sku)
                    if match:
                        rated_capacity_ah = match.group(1)

                # Infer nominal voltage from site's inverter voltage if available
                # This is approximate - we'll extract from inverter SKU in processing
                nominal_voltage_v = None  # Will be populated via JOIN with inverters in gold layer

                # Extract device instance (used for joining with telemetry data)
                # Use product_code since silver tables are organized by product code
                # The API's "instance" field is 0 for multiple devices, so it's not unique
                device_instance = str(device.get("product_code", ""))

                # Store vendor-specific fields in 'other' JSON
                other_fields = {
                    "instance": device.get("instance"),
                    "id_device_type": device.get("idDeviceType"),
                    "device_class": device.get("class"),
                    "last_connection": device.get("lastConnection"),
                    "product_id": device.get("productId"),
                    "device_name": device_name
                }

                # Construct Haystack-compliant foreign keys
                id_site_fk = f"voltcore-{site_id}"
                id_space_fk = f"voltcore-{site_id}-space-default"

                battery_record = {
                    "id_site": id_site_fk,
                    "id_space": id_space_fk,
                    "serial_number": None,  # Voltcore doesn't expose serial in system-overview
                    "brand": "Voltcore",
                    "model_series": model_series,
                    "model_sku": model_sku,
                    "product_name": product_name,
                    "product_code": product_code,
                    "firmware_version": device.get("firmware_version"),
                    "rated_capacity_ah": rated_capacity_ah,  # String for now
                    "nominal_voltage_v": nominal_voltage_v,  # Will be NULL initially
                    "chemistry": None,  # Not available from API
                    "device_instance": device_instance,
                    "connector": "voltcore_api",
                    "other": json.dumps(other_fields)
                }

                all_batteries.append(battery_record)
                print(f"    Added battery monitor: {product_name}")

    except Exception as e:
        print(f"  Error fetching devices for site {site_id}: {e}")
        continue

print(f"\n✓ Total inverters collected: {len(all_inverters)}")
print(f"✓ Total battery monitors collected: {len(all_batteries)}")

# Check if we have any inverters to process
if len(all_inverters) == 0:
    raise ValueError("No inverters collected from Voltcore API. Please check API credentials, network connectivity, and site configuration.")

# COMMAND ----------

# DBTITLE 1,Create DataFrame and Transform to Gold Schema
# Define schema (all strings initially to avoid type inference issues)
schema = StructType([
    StructField("connector", StringType(), True),
    StructField("id_site", StringType(), True),
    StructField("id_space", StringType(), True),
    StructField("serial_number", StringType(), True),
    StructField("product_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("model_series", StringType(), True),
    StructField("model_sku", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("firmware_version", StringType(), True),
    StructField("phases", StringType(), True),
    StructField("device_instance", StringType(), True),
    StructField("other", StringType(), True)
])

# Create DataFrame with explicit string schema
inverters_df = spark.createDataFrame(all_inverters, schema)

# Generate surrogate keys
# 1. Equipment spec SK (for metadata table reference)
inverters_df = inverters_df.withColumn(
    "sk_inverters_spec",
    F.md5(F.concat(F.col("brand"), F.col("model_series"), F.col("model_sku")))
)

# 2. Device SK (primary key for this device instance)
# Use product_code if serial_number is null (Voltcore case)
inverters_df = inverters_df.withColumn(
    "sk_inverter",
    F.md5(F.concat(
        F.col("connector"),
        F.lit("_"),
        F.col("id_site"),
        F.lit("_"),
        F.coalesce(F.col("serial_number"), F.col("product_code"))
    ))
)

# 3. Site and Space SKs (need to lookup from d_sites and d_spaces)
# For now, generate them using the same logic as the metadata notebook
# Extract source_site_id from id_site (e.g., "voltcore-407966" -> "407966")
inverters_df = inverters_df.withColumn(
    "source_site_id",
    F.regexp_extract(F.col("id_site"), r"voltcore-(\d+)", 1)
)

inverters_df = inverters_df.withColumn(
    "sk_site",
    F.md5(F.concat(F.col("connector"), F.lit("_"), F.col("source_site_id")))
)

inverters_df = inverters_df.withColumn(
    "sk_space",
    F.md5(F.concat(F.col("sk_site"), F.lit("_default")))
)

# Parse rated_power_w from model_sku (format: "voltage/power/charge_current/transfer_switch")
# Example: "48/5000/70-50" -> extract "5000"
inverters_df = inverters_df.withColumn(
    "rated_power_w",
    F.when(
        F.col("model_sku").isNotNull(),
        F.split(F.col("model_sku"), "/").getItem(1).cast(DecimalType(10, 2))
    ).otherwise(F.lit(None).cast(DecimalType(10, 2)))
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
        sk_inverter STRING COMMENT 'Device surrogate key - MD5(connector + id_site + serial_number or product_code)',

        -- Surrogate Key Foreign Keys
        sk_site STRING COMMENT 'Foreign key to d_sites.sk_site',
        sk_space STRING COMMENT 'Foreign key to d_spaces.sk_space',
        sk_inverters_spec STRING COMMENT 'Foreign key to d_inverters metadata spec table - MD5(brand + model_series + model_sku)',

        -- Haystack Natural Keys (Ontology Compliance)
        id_site STRING COMMENT 'Natural key to d_sites.id_site (Haystack: siteRef → materializeAs: id_site)',
        id_space STRING COMMENT 'Natural key to d_spaces.id_space (Haystack: spaceRef → materializeAs: id_space)',

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
    CLUSTER BY (connector, sk_inverter)
    COMMENT 'd_inverters: Generic dimension table for all inverters. Uses device SK (sk_inverter) as PK and spec SK (sk_inverters_spec) as FK to metadata table.'
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
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN sk_site STRING COMMENT 'Foreign key to d_sites.sk_site'")

    if "sk_space" not in existing_columns:
        print("  Adding column: sk_space")
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN sk_space STRING COMMENT 'Foreign key to d_spaces.sk_space'")

    if "sk_inverters_spec" not in existing_columns:
        print("  Adding column: sk_inverters_spec")
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN sk_inverters_spec STRING COMMENT 'Foreign key to d_inverters metadata spec table'")

    if "id_space" not in existing_columns:
        print("  Adding column: id_space")
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN id_space STRING COMMENT 'Natural key to d_spaces.id_space'")

    if "device_instance" not in existing_columns:
        print("  Adding column: device_instance")
        spark.sql(f"ALTER TABLE {gold_table} ADD COLUMN device_instance STRING COMMENT 'Voltcore device instance ID (e.g., 2611, 2623)'")

    print("  ✓ Schema migration complete")

# COMMAND ----------

# DBTITLE 1,Merge Into d_inverters
# Use MERGE to handle updates for existing devices and inserts for new ones
inverters_df.createOrReplaceTempView("voltcore_inverters_staging")

merge_sql = f"""
MERGE INTO {gold_table} AS target
USING voltcore_inverters_staging AS source
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
    target.device_instance = source.device_instance,
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
    device_instance,
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
    source.device_instance,
    source.connector,
    source.other,
    source.metadata_updated_at
  )
"""

spark.sql(merge_sql)

row_count = spark.table(gold_table).filter(F.col("connector") == "voltcore_api").count()
print(f"\n✓ Successfully merged {len(all_inverters)} Voltcore inverter records into {gold_table}")
print(f"  Total Voltcore inverters in table: {row_count}")

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

# DBTITLE 1,Show Voltcore Inverter Details
print("\nVoltcore inverter details:")
spark.sql(f"""
    SELECT
        id_site,
        model_series,
        model_sku,
        rated_power_w,
        phases,
        firmware_version,
        sk_inverter
    FROM {gold_table}
    WHERE brand = 'Voltcore'
    ORDER BY id_site
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Process Battery Monitors for d_batteries
if len(all_batteries) > 0:
    print(f"\n{'='*60}")
    print(f"Processing {len(all_batteries)} battery monitor(s)")
    print(f"{'='*60}")

    # Define battery schema (all strings initially)
    battery_schema = StructType([
        StructField("connector", StringType(), True),
        StructField("id_site", StringType(), True),
        StructField("id_space", StringType(), True),
        StructField("serial_number", StringType(), True),
        StructField("product_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("model_series", StringType(), True),
        StructField("model_sku", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("firmware_version", StringType(), True),
        StructField("rated_capacity_ah", StringType(), True),
        StructField("nominal_voltage_v", StringType(), True),
        StructField("chemistry", StringType(), True),
        StructField("device_instance", StringType(), True),
        StructField("other", StringType(), True)
    ])

    # Create DataFrame with explicit string schema
    batteries_df = spark.createDataFrame(all_batteries, battery_schema)

    # Generate surrogate keys
    # 1. Equipment spec SK
    batteries_df = batteries_df.withColumn(
        "sk_batteries_spec",
        F.md5(F.concat(F.col("brand"), F.col("model_series"), F.col("model_sku")))
    )

    # 2. Device SK
    batteries_df = batteries_df.withColumn(
        "sk_battery",
        F.md5(F.concat(
            F.col("connector"),
            F.lit("_"),
            F.col("id_site"),
            F.lit("_"),
            F.coalesce(F.col("serial_number"), F.col("product_code"))
        ))
    )

    # 3. Site and Space SKs
    batteries_df = batteries_df.withColumn(
        "source_site_id",
        F.regexp_extract(F.col("id_site"), r"voltcore-(\d+)", 1)
    )

    batteries_df = batteries_df.withColumn(
        "sk_site",
        F.md5(F.concat(F.col("connector"), F.lit("_"), F.col("source_site_id")))
    )

    batteries_df = batteries_df.withColumn(
        "sk_space",
        F.md5(F.concat(F.col("sk_site"), F.lit("_default")))
    )

    # Cast rated_capacity_ah to DECIMAL
    batteries_df = batteries_df.withColumn(
        "rated_capacity_ah",
        F.col("rated_capacity_ah").cast(DecimalType(10, 2))
    )

    # Cast nominal_voltage_v to DECIMAL
    batteries_df = batteries_df.withColumn(
        "nominal_voltage_v",
        F.col("nominal_voltage_v").cast(DecimalType(10, 2))
    )

    # Add metadata timestamp
    batteries_df = batteries_df.withColumn("metadata_updated_at", F.current_timestamp())

    # COMMAND ----------

    # DBTITLE 1,Create d_batteries Table if Not Exists
    batteries_gold_table = f"{CATALOG_NAME}.gold.d_batteries"

    table_exists = spark.catalog.tableExists(batteries_gold_table)

    if not table_exists:
        print(f"Creating generic battery dimension table: {batteries_gold_table}")

        spark.sql(f"""
        CREATE TABLE {batteries_gold_table} (
            -- Device Surrogate Key (Primary Key)
            sk_battery STRING COMMENT 'Device surrogate key - MD5(connector + id_site + serial_number or product_code)',

            -- Surrogate Key Foreign Keys
            sk_site STRING COMMENT 'Foreign key to d_sites.sk_site',
            sk_space STRING COMMENT 'Foreign key to d_spaces.sk_space',
            sk_batteries_spec STRING COMMENT 'Foreign key to d_batteries metadata spec table - MD5(brand + model_series + model_sku)',

            -- Haystack Natural Keys (Ontology Compliance)
            id_site STRING COMMENT 'Natural key to d_sites.id_site (Haystack: siteRef → materializeAs: id_site)',
            id_space STRING COMMENT 'Natural key to d_spaces.id_space (Haystack: spaceRef → materializeAs: id_space)',

            -- Device Identification
            serial_number STRING COMMENT 'Battery/BMS serial number (if available)',

            -- Brand/Manufacturer
            brand STRING COMMENT 'Battery/BMS manufacturer (Voltcore, etc.)',

            -- Model Information (Generic across all brands)
            model_series STRING COMMENT 'Model series (e.g., SmartShunt, Lynx Smart BMS)',
            model_sku STRING COMMENT 'Model SKU/variant (e.g., 500A, 1000A)',
            product_name STRING COMMENT 'Full product name',
            product_code STRING COMMENT 'Vendor product code',

            -- Technical Specifications (Common battery fields)
            rated_capacity_ah DECIMAL(10,2) COMMENT 'Rated battery capacity in Amp-hours (shunt rating, not actual battery capacity)',
            nominal_voltage_v DECIMAL(10,2) COMMENT 'Nominal system voltage (e.g., 12V, 24V, 48V)',
            chemistry STRING COMMENT 'Battery chemistry type (LiFePO4, Lead-Acid, NMC, etc.) - may be NULL if not available',

            -- Version/Firmware
            firmware_version STRING COMMENT 'Current firmware version',

            -- Voltcore-specific fields
            device_instance STRING COMMENT 'Voltcore device instance ID (e.g., 2611, 2623)',

            -- Metadata
            connector STRING COMMENT 'Data source connector (voltcore_api, etc.)',
            other STRING COMMENT 'JSON string for vendor-specific fields',
            metadata_updated_at TIMESTAMP COMMENT 'Last metadata update timestamp'
        )
        CLUSTER BY (connector, sk_battery)
        COMMENT 'd_batteries: Generic dimension table for battery monitors/BMS. Uses device SK (sk_battery) as PK and spec SK (sk_batteries_spec) as FK to metadata table.'
        """)

        print(f"✓ Created table: {batteries_gold_table}")
    else:
        # Migrate existing table: Add new columns if they don't exist
        print(f"Table {batteries_gold_table} exists. Checking for schema migration...")

        existing_columns = [field.name for field in spark.table(batteries_gold_table).schema.fields]

        # Add new surrogate key columns if they don't exist
        if "sk_battery" not in existing_columns:
            print("  Adding column: sk_battery")
            spark.sql(f"ALTER TABLE {batteries_gold_table} ADD COLUMN sk_battery STRING COMMENT 'Device surrogate key - MD5(connector + id_site + serial_number)'")

        if "sk_site" not in existing_columns:
            print("  Adding column: sk_site")
            spark.sql(f"ALTER TABLE {batteries_gold_table} ADD COLUMN sk_site STRING COMMENT 'Foreign key to d_sites.sk_site'")

        if "sk_space" not in existing_columns:
            print("  Adding column: sk_space")
            spark.sql(f"ALTER TABLE {batteries_gold_table} ADD COLUMN sk_space STRING COMMENT 'Foreign key to d_spaces.sk_space'")

        if "sk_batteries_spec" not in existing_columns:
            print("  Adding column: sk_batteries_spec")
            spark.sql(f"ALTER TABLE {batteries_gold_table} ADD COLUMN sk_batteries_spec STRING COMMENT 'Foreign key to d_batteries metadata spec table'")

        if "id_space" not in existing_columns:
            print("  Adding column: id_space")
            spark.sql(f"ALTER TABLE {batteries_gold_table} ADD COLUMN id_space STRING COMMENT 'Natural key to d_spaces.id_space'")

        if "device_instance" not in existing_columns:
            print("  Adding column: device_instance")
            spark.sql(f"ALTER TABLE {batteries_gold_table} ADD COLUMN device_instance STRING COMMENT 'Voltcore device instance ID (e.g., 2611, 2623)'")

        print("  ✓ Schema migration complete")

    # COMMAND ----------

    # DBTITLE 1,Merge Into d_batteries
    batteries_df.createOrReplaceTempView("voltcore_batteries_staging")

    merge_sql = f"""
    MERGE INTO {batteries_gold_table} AS target
    USING voltcore_batteries_staging AS source
    ON target.sk_battery = source.sk_battery
    WHEN MATCHED THEN
      UPDATE SET
        target.sk_site = source.sk_site,
        target.sk_space = source.sk_space,
        target.sk_batteries_spec = source.sk_batteries_spec,
        target.id_site = source.id_site,
        target.id_space = source.id_space,
        target.serial_number = source.serial_number,
        target.product_name = source.product_name,
        target.product_code = source.product_code,
        target.firmware_version = source.firmware_version,
        target.rated_capacity_ah = source.rated_capacity_ah,
        target.nominal_voltage_v = source.nominal_voltage_v,
        target.chemistry = source.chemistry,
        target.device_instance = source.device_instance,
        target.other = source.other,
        target.metadata_updated_at = source.metadata_updated_at
    WHEN NOT MATCHED THEN
      INSERT (
        sk_battery,
        sk_site,
        sk_space,
        sk_batteries_spec,
        id_site,
        id_space,
        serial_number,
        brand,
        model_series,
        model_sku,
        product_name,
        product_code,
        rated_capacity_ah,
        nominal_voltage_v,
        chemistry,
        firmware_version,
        device_instance,
        connector,
        other,
        metadata_updated_at
      )
      VALUES (
        source.sk_battery,
        source.sk_site,
        source.sk_space,
        source.sk_batteries_spec,
        source.id_site,
        source.id_space,
        source.serial_number,
        source.brand,
        source.model_series,
        source.model_sku,
        source.product_name,
        source.product_code,
        source.rated_capacity_ah,
        source.nominal_voltage_v,
        source.chemistry,
        source.firmware_version,
        source.device_instance,
        source.connector,
        source.other,
        source.metadata_updated_at
      )
    """

    spark.sql(merge_sql)

    row_count = spark.table(batteries_gold_table).filter(F.col("connector") == "voltcore_api").count()
    print(f"\n✓ Successfully merged {len(all_batteries)} Voltcore battery monitor records into {batteries_gold_table}")
    print(f"  Total Voltcore battery monitors in table: {row_count}")
