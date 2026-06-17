# Databricks notebook source
# DBTITLE 1,Generate Connection Configs for All Wattflow EANs
"""
This notebook dynamically generates/updates connection_config entries for all Wattflow EANs.

Purpose:
- Creates one connection_config row per EAN (construction site)
- Enables b06_update_connection_metadata to track status for each EAN separately
- Reads metadata from wattflow_sites seed table
- Automatically handles both active and inactive EANs

Architecture:
- Each meter gets a unique connection_id: wattflow_{ean}_{key}
- The key (sleutel) is the true unique meter identifier (dap_id can serve multiple meters)
- All meters share the same connector: wattflow_api
- All meters share the same gold_table_name: {catalog}.gold.f_wattflow_dap_batch
- b06 will filter gold table by EAN + key to calculate per-meter metrics

Workflow:
1. This notebook (b05) runs after b04_silver2gold (dbt)
2. Queries wattflow_sites seed for all EANs (active + inactive)
3. Upserts connection_config entries for each EAN
4. b06 runs next with connector='wattflow_api' and processes all EANs
"""

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from datetime import datetime, timezone
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.window import Window as W

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("client_name", "", "Client Name (e.g. ACME, GLOBEX)")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
CLIENT = dbutils.widgets.get("client_name")

# Paths
METADATA_SCHEMA = f"{CATALOG_NAME}.metadata"
CONNECTION_CONFIG_TABLE = f"{METADATA_SCHEMA}.connection_config"
WATTFLOW_SITES_TABLE = f"{CATALOG_NAME}.gold.d_wattflow_sites"  # Dimension table (seed is dropped by post-hook)
GOLD_TABLE = f"{CATALOG_NAME}.gold.f_wattflow_dap_batch"

# Constants
CONNECTOR = "wattflow_api"
BRAND = "Wattflow"
PROCESSING_TYPE = "Batch"
DATA_TYPE = "Electricity Meter"
TYPE = "Smart meter"
PROTOCOL = "HTTP"

# Data granularity intervals (how often data points are recorded)
# NOTE: This is DIFFERENT from job frequency (job runs daily, but data is recorded every 15/60 min)
# expected_interval_minutes should reflect DATA GRANULARITY for quality monitoring, not job frequency
AGGREGATION_TO_MINUTES = {
    "quarterly": 15,   # 15-minute intervals (96 records/day expected)
    "hourly": 60,      # 1-hour intervals (24 records/day expected)
    "daily": 1440      # daily intervals (1 record/day expected)
}

print(f"Catalog: {CATALOG_NAME}")
print(f"Connection Config Table: {CONNECTION_CONFIG_TABLE}")
print(f"Wattflow Sites Dimension: {WATTFLOW_SITES_TABLE}")
print(f"Gold Table: {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %run ../../../shared/connection_status_utils

# COMMAND ----------

# DBTITLE 1,Ensure Metadata Table Exists
# Shared DDL — single source of truth in ensure_metadata_tables() (%run'd above).
# The `metadata` schema is Terraform-managed in dataplatformx-infra; never created here.
ensure_metadata_tables(spark, CATALOG_NAME)

print(f"✓ Table {CONNECTION_CONFIG_TABLE} ready")

# COMMAND ----------

# DBTITLE 1,Get Current Job Information
try:
    job_id = spark.conf.get("spark.databricks.job.id", None)
    job_name = spark.conf.get("spark.databricks.jobName", "acme wattflow batch job")

    print(f"✓ Job ID: {job_id}")
    print(f"✓ Job Name: {job_name}")
except Exception as e:
    print(f"⚠ Running interactively, using defaults: {e}")
    job_id = None
    job_name = "acme wattflow batch job"

# COMMAND ----------

# DBTITLE 1,Read Wattflow Sites Dimension Table
# Read all meters from d_wattflow_sites dimension table (wattflow_sites seed is dropped by post-hook)
# IMPORTANT: Each unique EAN + key combination represents one physical meter
# Deduplicate by ean + key to ensure no MERGE conflicts

# First, read all data
sites_raw_df = spark.table(WATTFLOW_SITES_TABLE).select(
    "ean",
    "key",  # This is sleutel (renamed in d_wattflow_sites) - unique meter identifier
    "dap_id",  # This is dap_id (renamed in d_wattflow_sites) - DAP device ID (not unique per meter)
    "dis",
    "geo_addr",
    "geo_street",
    "geo_city",
    "geo_postal_code",
    "geo_country",
    "meter_status",  # This is meter_status (renamed in d_wattflow_sites)
    "entity",
    "customer_number"  # This is customer_number (renamed in d_wattflow_sites)
)

# Deduplicate by ean + key (the true natural key)
# If duplicates exist, prefer active meters and most recent dap_id
window_spec = W.partitionBy("ean", "key").orderBy(
    F.when(F.col("meter_status") == "active", 0).otherwise(1),  # Active first
    F.col("dap_id").desc()  # Then by highest dap_id
)

sites_df = sites_raw_df \
    .withColumn("rn", F.row_number().over(window_spec)) \
    .filter(F.col("rn") == 1) \
    .drop("rn")

# Statistics
total_raw_meters = sites_raw_df.count()
total_meters = sites_df.count()
active_count = sites_df.filter(F.col("meter_status") == "active").count()
inactive_count = sites_df.filter(F.col("meter_status") == "inactive").count()
unique_eans = sites_df.select("ean").distinct().count()

if total_raw_meters > total_meters:
    duplicates_removed = total_raw_meters - total_meters
    print(f"⚠ Removed {duplicates_removed} duplicate ean+key combinations from dimension table")
    print(f"  Original rows: {total_raw_meters}, After deduplication: {total_meters}")

print(f"\n✓ Loaded {total_meters} meters from d_wattflow_sites:")
print(f"  - Active: {active_count}")
print(f"  - Inactive: {inactive_count}")
print(f"  - Unique EANs: {unique_eans}")
print(f"  - Average meters per EAN: {total_meters / unique_eans:.1f}")

# COMMAND ----------

# DBTITLE 1,Query Aggregation Levels from Silver Table
# Query the most recent aggregation level for each EAN from silver table
SILVER_TABLE = f"{CATALOG_NAME}.silver.wattflow_dap_batch"

# Check if silver table exists
if spark.catalog.tableExists(SILVER_TABLE):
    from pyspark.sql.window import Window as W

    # Get the most recent aggregation_level for each EAN
    window_spec = W.partitionBy("ean").orderBy(F.col("timestamp").desc())

    aggregation_levels_df = spark.table(SILVER_TABLE) \
        .select("ean", "aggregation_level", "timestamp") \
        .filter(F.col("aggregation_level").isNotNull()) \
        .withColumn("rn", F.row_number().over(window_spec)) \
        .filter(F.col("rn") == 1) \
        .select("ean", "aggregation_level")

    # Convert to dictionary for lookup
    aggregation_by_ean = {row.ean: row.aggregation_level for row in aggregation_levels_df.collect()}

    print(f"\n✓ Loaded aggregation levels for {len(aggregation_by_ean)} EANs from silver table")

    # Show distribution of aggregation levels
    agg_distribution = aggregation_levels_df.groupBy("aggregation_level").count().collect()
    print("\nAggregation level distribution:")
    for row in sorted(agg_distribution, key=lambda x: x['count'], reverse=True):
        print(f"  - {row['aggregation_level']}: {row['count']} EANs")
else:
    print(f"\n⚠ Silver table {SILVER_TABLE} does not exist yet")
    print("  → Will use default interval (quarterly/15min) for all EANs")
    aggregation_by_ean = {}

# COMMAND ----------

# DBTITLE 1,Build Aggregation Lookup DataFrame (Native Spark - No UDFs)
# Convert aggregation lookup dict to a broadcast DataFrame for a native Spark join
# This avoids Python UDFs entirely, keeping all processing in the JVM
if aggregation_by_ean:
    agg_lookup_df = spark.createDataFrame(
        [(ean, agg_level, AGGREGATION_TO_MINUTES.get(agg_level, 15))
         for ean, agg_level in aggregation_by_ean.items()],
        schema=["ean", "_aggregation_level", "_expectedIntervalMinutes"]
    )
else:
    agg_lookup_df = spark.createDataFrame(
        [],
        schema=StructType([
            StructField("ean", StringType()),
            StructField("_aggregation_level", StringType()),
            StructField("_expectedIntervalMinutes", IntegerType())
        ])
    )

print(f"✓ Aggregation lookup DataFrame created with {agg_lookup_df.count()} entries (native Spark, no UDFs)")

# COMMAND ----------

# DBTITLE 1,Build Connection Config Records
# Generate connection config for each meter (EAN + key combination)
# Note: dap_id is NOT unique - one DAP can serve multiple meters (keys) at the same EAN
# The true unique identifier is the combination of EAN + key (sleutel)

# Left join sites with aggregation lookup (broadcast for performance)
sites_with_agg_df = sites_df.join(F.broadcast(agg_lookup_df), on="ean", how="left")

connection_configs = sites_with_agg_df.select(
    # connection_id: unique identifier per METER (EAN + key)
    # Example: wattflow_871687120000023324_54077063
    # This matches the sk_econnection surrogate key in gold: MD5(CONCAT(ean, key))
    F.concat(
        F.lit("wattflow_"),
        F.col("ean"),
        F.lit("_"),
        F.col("key")
    ).alias("connection_id"),

    # data_name: human-readable name (site description + city + key for uniqueness)
    F.concat(
        F.col("dis"),
        F.lit(" - "),
        F.col("geo_city"),
        F.lit(" (Meter: "),
        F.col("key"),
        F.lit(")")
    ).alias("data_name"),

    # Type and data_type
    F.lit(TYPE).alias("type"),
    F.lit(DATA_TYPE).alias("data_type"),
    F.lit(PROTOCOL).alias("protocol"),

    # Location: full address
    F.concat(
        F.col("geo_addr"),
        F.lit(", "),
        F.col("geo_postal_code"),
        F.lit(" "),
        F.col("geo_city"),
        F.lit(", "),
        F.col("geo_country")
    ).alias("location"),

    # object_id: entity name (business unit)
    F.col("entity").alias("object_id"),

    # Client & Brand
    F.lit(CLIENT).alias("client"),
    F.lit(BRAND).alias("brand"),
    F.lit(CONNECTOR).alias("connector"),
    F.lit(PROCESSING_TYPE).alias("processing_type"),

    # Pipeline configuration
    F.lit(job_id).alias("job_id"),
    F.lit(job_name).alias("job_name"),
    F.lit(GOLD_TABLE).alias("gold_table_name"),
    F.coalesce(F.col("_expectedIntervalMinutes"), F.lit(15)).alias("expected_interval_minutes"),
    F.coalesce(F.col("_aggregation_level"), F.lit("quarterly")).alias("aggregation_level"),

    # Additional metadata as JSON (data keys snake_case; platform contract keys
    # `timestamp_col` / `processing_timestamp_col` stay camelCase — b10/b11 read by those names).
    # processing_timestamp_col explicitly set to the renamed v2 column so we don't fall
    # through to b10's default `goldProcessingTimestamp` which no longer exists.
    F.to_json(
        F.struct(
            F.col("ean").alias("ean"),
            F.col("key").alias("key"),
            F.col("dap_id").alias("dap_id"),
            F.col("customer_number").alias("customer_number"),
            F.col("meter_status").alias("meter_status"),
            F.lit("timestamp").alias("timestamp_col"),
            F.lit("gold_processing_timestamp").alias("processing_timestamp_col")
        )
    ).alias("other_metadata"),

    # Audit timestamps
    F.lit(datetime.now(timezone.utc)).alias("created_at"),
    F.lit(datetime.now(timezone.utc)).alias("updated_at")
)

print(f"✓ Generated {connection_configs.count()} connection config records (one per meter)")

# COMMAND ----------

# DBTITLE 1,Preview Sample Records
print("\n=== Sample Connection Configs ===\n")
connection_configs.select(
    "connection_id",
    "data_name",
    "location",
    "object_id",
    "aggregation_level",
    "expected_interval_minutes",
    "gold_table_name"
).show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Upsert to Connection Config Table
# Use MERGE to upsert (insert new, update existing)
# This ensures idempotency - running multiple times is safe

connection_configs.createOrReplaceTempView("new_configs")

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

print(f"\n✓ Successfully upserted {total_meters} connection configs to {CONNECTION_CONFIG_TABLE}")

# COMMAND ----------

# DBTITLE 1,Verify Results
# Query back to confirm
result_df = spark.sql(f"""
    SELECT
        connection_id,
        data_name,
        location,
        object_id,
        connector,
        aggregation_level,
        expected_interval_minutes,
        gold_table_name,
        updated_at
    FROM {CONNECTION_CONFIG_TABLE}
    WHERE connector = '{CONNECTOR}'
    ORDER BY aggregation_level, expected_interval_minutes, connection_id
""")

result_count = result_df.count()
print(f"\n=== Verification ===")
print(f"Total Wattflow connection configs in table: {result_count}")
print(f"Expected: {total_meters}")
print(f"Match: {'✓ YES' if result_count == total_meters else '✗ NO - MISMATCH!'}")

# COMMAND ----------

# DBTITLE 1,Display Summary
# Get interval distribution
interval_distribution = result_df.groupBy("aggregation_level", "expected_interval_minutes").count().collect()

print("\n" + "="*80)
print("WATTFLOW CONNECTION CONFIG GENERATION - COMPLETE")
print("="*80)
print(f"✓ Processed {total_meters} meters from {unique_eans} unique EANs")
print(f"  - Active meters: {active_count}")
print(f"  - Inactive meters: {inactive_count}")
print(f"✓ Upserted to {CONNECTION_CONFIG_TABLE}")
print(f"✓ All configs use connector: {CONNECTOR}")
print(f"✓ All configs point to gold table: {GOLD_TABLE}")
print(f"\n✓ Interval distribution (data granularity from silver table):")
for row in sorted(interval_distribution, key=lambda x: x['expected_interval_minutes']):
    agg_level = row['aggregation_level'] or 'unknown'
    interval = row['expected_interval_minutes']
    count = row['count']

    # Calculate expected records per day for each granularity
    if interval > 0:
        expected_records_per_day = (24 * 60) // interval
        print(f"  - {agg_level}: {interval} min intervals → {count} meter(s) ({expected_records_per_day} records/day expected)")
    else:
        print(f"  - {agg_level}: {interval} minutes → {count} meter(s)")

print("\nNext step:")
print(f"  → b10_update_batch_connection_metadata will process all meters with connector='{CONNECTOR}'")
print(f"  → Each meter (EAN + key) will get a separate row in connection_status table")
print(f"  → Status (ACTIVE/DELAYED/OFFLINE) will be calculated dynamically per meter")
print(f"  → Completeness will be calculated using DATA GRANULARITY intervals:")
print(f"     • Quarterly (15min): expected_records_24h = 96")
print(f"     • Hourly (60min): expected_records_24h = 24")
print(f"  → Note: key (sleutel) is the unique meter identifier, not dap_id")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Return Success
dbutils.notebook.exit("SUCCESS")
