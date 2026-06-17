# Databricks notebook source
# DBTITLE 1,Generate Connection Configs for All Ecosphere Buildings x Silver Tables
"""
This notebook dynamically generates/updates connection_config entries for all Ecosphere buildings.

Purpose:
- Creates one connection_config row per building × silver table combination
- Enables b10_update_batch_connection_metadata to track status for each combination separately
- Auto-discovers buildings from existing silver data (no manual seed required)
- Automatically handles new buildings as they appear in the pipeline

Architecture:
- Each building × silver table gets a unique connection_id: ecosphere_{short_uuid}_{silver_table_suffix}
- The building_uuid is stored in other_metadata for b10 to filter the silver table
- All connections share the same connector: ecosphere_api
- b10 will filter each silver table by building_uuid to calculate per-building metrics

Workflow:
1. This notebook (b05) runs after b03_ecosphere_bronze2silver
2. Queries all silver tables for distinct building_uuid + building_name combinations
3. Generates one connection_config row per building × silver table
4. Upserts connection_config entries (idempotent via MERGE)
5. b10 runs next with connector='ecosphere_api' and processes all connections
"""

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timezone

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("client_name", "", "Client Name (e.g. ACME, GLOBEX)")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
CLIENT = dbutils.widgets.get("client_name")

# Paths
METADATA_SCHEMA = f"{CATALOG_NAME}.metadata"
CONNECTION_CONFIG_TABLE = f"{METADATA_SCHEMA}.connection_config"

# Constants
CONNECTOR = "ecosphere_api"
BRAND = "Ecosphere"
PROCESSING_TYPE = "Batch"
PROTOCOL = "HTTP"

# Silver table definitions: each entry maps a silver table to its connection metadata.
# - suffix: used in connection_id (ecosphere_{short_uuid}_{suffix})
# - table: fully qualified silver table name
# - data_type: equipment/asset type for the connection_config
# - type: connection type
# - timestamp_col: column used by b10 to calculate freshness
# - expected_interval_minutes: data granularity (15-min for history, 1440 for daily snapshots)
SILVER_TABLES = [
    {
        "suffix": "econnection_history",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_econnection_history",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_econnection_history",
        "data_type": "EConnection",
        "type": "Electricity History",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 15,
    },
    {
        "suffix": "hconnection_history",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_hconnection_history",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_hconnection_history",
        "data_type": "HConnection",
        "type": "Heat History",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 15,
    },
    {
        "suffix": "gconnection_history",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_gconnection_history",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_gconnection_history",
        "data_type": "GConnection",
        "type": "Gas History",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 15,
    },
    {
        "suffix": "waterconnection_history",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_waterconnection_history",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_waterconnection_history",
        "data_type": "WaterConnection",
        "type": "Water History",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 15,
    },
    {
        "suffix": "fcr_history",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_fcr_history",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_fcr_history",
        "data_type": "FCR",
        "type": "FCR History",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 15,
    },
    {
        "suffix": "econnection_meterreadings",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_econnection_meterreadings",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_econnection_meterreadings",
        "data_type": "EConnection",
        "type": "Electricity Meterreadings",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 1440,
    },
    {
        "suffix": "hconnection_meterreadings",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_hconnection_meterreadings",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_hconnection_meterreadings",
        "data_type": "HConnection",
        "type": "Heat Meterreadings",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 1440,
    },
    {
        "suffix": "waterconnection_meterreadings",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_waterconnection_meterreadings",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_waterconnection_meterreadings",
        "data_type": "WaterConnection",
        "type": "Water Meterreadings",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 1440,
    },
    {
        "suffix": "battery",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_battery",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_battery",
        "data_type": "Battery",
        "type": "Battery SoC",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 1440,
    },
    {
        "suffix": "solar_irradiance",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_solar_irradiance",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_solar_irradiance",
        "data_type": "SolarIrradiance",
        "type": "Solar Irradiance",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 1440,
    },
    {
        "suffix": "ev_charging_station",
        "table": f"{CATALOG_NAME}.silver.ecosphere_batch_ev_charging_station",
        "goldTable": f"{CATALOG_NAME}.gold.f_ecosphere_ev_charging_station",
        "data_type": "EVChargingStation",
        "type": "EV Charging Station",
        "timestamp_col": "measurement_timestamp",
        "expected_interval_minutes": 1440,
    },
]

print(f"Catalog: {CATALOG_NAME}")
print(f"Connection Config Table: {CONNECTION_CONFIG_TABLE}")
print(f"Silver tables to scan: {len(SILVER_TABLES)}")

# COMMAND ----------

# MAGIC %run ../../../shared/connection_status_utils

# COMMAND ----------

# DBTITLE 1,Ensure Metadata Table Exists
# Shared DDL — single source of truth in ensure_metadata_tables() (%run'd above).
# The `metadata` schema is Terraform-managed in dataplatformx-infra; never created here.
ensure_metadata_tables(spark, CATALOG_NAME)

print(f"Table {CONNECTION_CONFIG_TABLE} ready")

# COMMAND ----------

# DBTITLE 1,Get Current Job Information
try:
    job_id = spark.conf.get("spark.databricks.job.id", None)
    job_name = spark.conf.get("spark.databricks.jobName", "acme ecosphere batch job")
    print(f"Job ID: {job_id}")
    print(f"Job Name: {job_name}")
except Exception as e:
    print(f"Running interactively, using defaults: {e}")
    job_id = None
    job_name = "acme ecosphere batch job"

# COMMAND ----------

# DBTITLE 1,Auto-Discover Buildings from Silver Tables
# Scan all silver tables for distinct building_uuid + building_name combinations.
# This avoids needing a manual seed CSV — new buildings appear automatically.

all_buildings = {}

for silver_cfg in SILVER_TABLES:
    table_name = silver_cfg["table"]
    try:
        if not spark.catalog.tableExists(table_name):
            print(f"  Table {table_name} does not exist yet — skipping")
            continue

        buildings_df = spark.sql(f"""
            SELECT DISTINCT building_uuid, building_name
            FROM {table_name}
            WHERE building_uuid IS NOT NULL
        """).collect()

        for row in buildings_df:
            if row.building_uuid not in all_buildings:
                all_buildings[row.building_uuid] = row.building_name

        print(f"  {table_name}: {len(buildings_df)} building(s)")
    except Exception as e:
        print(f"  Error scanning {table_name}: {e}")

print(f"\nDiscovered {len(all_buildings)} unique building(s):")
for uuid, name in sorted(all_buildings.items()):
    print(f"  - {uuid[:8]}... : {name}")

# COMMAND ----------

# DBTITLE 1,Build Connection Config Records
import json

now = datetime.now(timezone.utc)
config_records = []

for building_uuid, building_name in sorted(all_buildings.items()):
    short_uuid = building_uuid.split("-")[0]  # e.g., a5667b60

    for silver_cfg in SILVER_TABLES:
        table_name = silver_cfg["table"]

        # Check if this building actually has data in this silver table
        try:
            if not spark.catalog.tableExists(table_name):
                continue

            has_data = spark.sql(f"""
                SELECT 1
                FROM {table_name}
                WHERE building_uuid = '{building_uuid}'
                LIMIT 1
            """).count() > 0

            if not has_data:
                continue
        except Exception:
            continue

        suffix = silver_cfg["suffix"]
        connection_id = f"ecosphere_{short_uuid}_{suffix}"

        # Determine aggregation level label
        interval = silver_cfg["expected_interval_minutes"]
        if interval <= 15:
            aggregation_level = "quarterly"
        elif interval <= 60:
            aggregation_level = "hourly"
        else:
            aggregation_level = "daily"

        # Build other_metadata JSON with all info b10 needs to query.
        # processing_timestamp_col explicitly set to silver_processing_timestamp because
        # ecosphere gold facts propagate the silver-side lineage timestamp (no separate
        # gold-side one), so b10's default 'gold_processing_timestamp' would not be found.
        other_metadata = json.dumps({
            "building_uuid": building_uuid,
            "building_name": building_name,
            "silver_table": table_name,
            "timestamp_col": silver_cfg["timestamp_col"],
            "processing_timestamp_col": "silver_processing_timestamp",
        })

        config_records.append({
            "connection_id": connection_id,
            "data_name": f"{building_name} - {silver_cfg['type']}",
            "type": silver_cfg["type"],
            "data_type": silver_cfg["data_type"],
            "protocol": PROTOCOL,
            "location": None,  # Ecosphere API does not expose building addresses
            "object_id": f"{short_uuid}_{suffix}",
            "client": CLIENT,
            "brand": BRAND,
            "connector": CONNECTOR,
            "processing_type": PROCESSING_TYPE,
            "job_id": job_id,
            "job_name": job_name,
            "gold_table_name": silver_cfg["goldTable"],
            "expected_interval_minutes": interval,
            "aggregation_level": aggregation_level,
            "other_metadata": other_metadata,
            "created_at": now,
            "updated_at": now,
        })

print(f"\nGenerated {len(config_records)} connection config records")
print(f"  Buildings: {len(all_buildings)}")
print(f"  Gold tables referenced: {len(set(r['gold_table_name'] for r in config_records))}")

# COMMAND ----------

# DBTITLE 1,Define Config Schema
# Explicit schema is required because None values in nullable fields (location, object_id, job_id)
# prevent Spark from inferring types automatically.
config_schema = StructType([
    StructField("connection_id", StringType(), False),
    StructField("data_name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("location", StringType(), True),
    StructField("object_id", StringType(), True),
    StructField("client", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("connector", StringType(), True),
    StructField("processing_type", StringType(), True),
    StructField("job_id", StringType(), True),
    StructField("job_name", StringType(), True),
    StructField("gold_table_name", StringType(), True),
    StructField("expected_interval_minutes", IntegerType(), True),
    StructField("aggregation_level", StringType(), True),
    StructField("other_metadata", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])

# COMMAND ----------

# DBTITLE 1,Preview Sample Records
if config_records:
    preview_df = spark.createDataFrame(config_records, schema=config_schema)
    print("\n=== Sample Connection Configs ===\n")
    preview_df.select(
        "connection_id",
        "data_name",
        "data_type",
        "expected_interval_minutes",
        "gold_table_name",
    ).show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Upsert to Connection Config Table
if not config_records:
    print("No connection configs to upsert — no buildings found in silver tables.")
    dbutils.notebook.exit("SUCCESS_NO_DATA")

configs_df = spark.createDataFrame(config_records, schema=config_schema)
configs_df.createOrReplaceTempView("new_configs")

merge_sql = f"""
MERGE INTO {CONNECTION_CONFIG_TABLE} AS target
USING new_configs AS source
ON target.connection_id = source.connection_id
WHEN MATCHED THEN UPDATE SET
    data_name = source.data_name,
    type = source.type,
    data_type = source.data_type,
    protocol = source.protocol,
    location = source.location,
    object_id = source.object_id,
    client = source.client,
    brand = source.brand,
    connector = source.connector,
    processing_type = source.processing_type,
    job_id = source.job_id,
    job_name = source.job_name,
    gold_table_name = source.gold_table_name,
    expected_interval_minutes = source.expected_interval_minutes,
    aggregation_level = source.aggregation_level,
    other_metadata = source.other_metadata,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    connection_id,
    data_name,
    type,
    data_type,
    protocol,
    location,
    object_id,
    client,
    brand,
    connector,
    processing_type,
    job_id,
    job_name,
    gold_table_name,
    expected_interval_minutes,
    aggregation_level,
    other_metadata,
    created_at,
    updated_at
) VALUES (
    source.connection_id,
    source.data_name,
    source.type,
    source.data_type,
    source.protocol,
    source.location,
    source.object_id,
    source.client,
    source.brand,
    source.connector,
    source.processing_type,
    source.job_id,
    source.job_name,
    source.gold_table_name,
    source.expected_interval_minutes,
    source.aggregation_level,
    source.other_metadata,
    source.created_at,
    source.updated_at
)
"""

spark.sql(merge_sql)

print(f"\nSuccessfully upserted {len(config_records)} connection configs to {CONNECTION_CONFIG_TABLE}")

# COMMAND ----------

# DBTITLE 1,Verify Results
result_df = spark.sql(f"""
    SELECT
        connection_id,
        data_name,
        data_type,
        expected_interval_minutes,
        aggregation_level,
        gold_table_name,
        updated_at
    FROM {CONNECTION_CONFIG_TABLE}
    WHERE connector = '{CONNECTOR}'
    ORDER BY connection_id
""")

result_count = result_df.count()
print(f"\n=== Verification ===")
print(f"Total Ecosphere connection configs in table: {result_count}")
print(f"Expected: {len(config_records)}")
print(f"Match: {'YES' if result_count >= len(config_records) else 'MISMATCH'}")

result_df.show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,Display Summary
# Get distribution by data_type
type_distribution = result_df.groupBy("data_type").count().collect()

print("\n" + "=" * 80)
print("ECOSPHERE CONNECTION CONFIG GENERATION - COMPLETE")
print("=" * 80)
print(f"Processed {len(all_buildings)} building(s) x {len(SILVER_TABLES)} silver tables")
print(f"Generated {len(config_records)} connection configs (only where data exists)")
print(f"Upserted to {CONNECTION_CONFIG_TABLE}")
print(f"All configs use connector: {CONNECTOR}")

print(f"\nDistribution by ESDL asset type:")
for row in sorted(type_distribution, key=lambda x: x['count'], reverse=True):
    print(f"  - {row['data_type']}: {row['count']} connection(s)")

print(f"\nBuildings:")
for uuid, name in sorted(all_buildings.items()):
    short_uuid = uuid.split("-")[0]
    building_configs = [r for r in config_records if r["connection_id"].startswith(f"ecosphere_{short_uuid}_")]
    print(f"  - {name} ({short_uuid}...): {len(building_configs)} connection(s)")

print(f"\nNext step:")
print(f"  b10_update_batch_connection_metadata will process all connections with connector='{CONNECTOR}'")
print(f"  Each connection will get a separate row in connection_status table")
print(f"  b10 will use other_metadata.building_uuid to filter each silver table")
print(f"  b10 will use other_metadata.timestamp_col as the timestamp column for freshness")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Return Success
dbutils.notebook.exit("SUCCESS")
