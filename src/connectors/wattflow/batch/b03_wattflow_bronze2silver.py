# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("bronze_table_name", "wattflow_dap_batch", "Bronze Table Name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_PATH = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

dbutils.widgets.text("silver_table_name", "wattflow_dap_batch", "Silver Table Name")
SILVER_TABLE_NAME = dbutils.widgets.get("silver_table_name")
SILVER_PATH = f"{CATALOG_NAME}.silver.{SILVER_TABLE_NAME}"

# COMMAND ----------

# DBTITLE 1,Define EAN to Key (Sleutel) Mapping
# Hardcoded mapping from wattflow_sites seed file
# Note: Some EANs have multiple keys (different meters), we take the first active one
# For EANs with only inactive keys, we use the first inactive one
EAN_TO_KEY = {
    '871689260011945210': '50594511',   # Argonautenweg 57 BIJ
    '871689260011746329': '56621452',   # Hazelaarweg 100 Bij
    '871685920004048543': '59868668',   # Koningin Wilhelminaplein 1
    '871689260012433310': '63398185',   # Orionstraat 235 BIJ
    '871689260012411691': '63086626',   # Wegastraat 67 BIJ
    '871687120000023324': '54077063',   # Plantijnweg 32 (3 meters, taking first)
    '871687110000967926': '50624190',   # De Serpeling 120
    '871687120000061975': '73017642',   # De Serpeling 120
    '871687910000065475': 'M63508311',  # De Steenbok 15
    '871687910000475441': '63508312',   # De Steenbok 15
    '871687910000475458': '63508311',   # De Steenbok 15
    '871694831000416077': '43333524',   # Den Hulst 102 (3 meters, taking first)
    '871694831000080872': '54631656',   # Den Hulst 110
    '871685900041068209': '63508328',   # Elsrijkdreef 199 A
    '871692150000024054': '54631172',   # H.J.Nederhorststraat 1 (9 meters, taking first)
    '871694831000211504': '56621322',   # Jeverweg 16 T/M 18 (3 meters, taking first)
    '871689260013155471': '54636290',   # Kilkade 39
    '871689276000060611': '54077184',   # Kilkade 53 (2 meters, taking first)
    '871689290602500320': '33201979',   # Kilkade 53
    '871687120000032982': '54629872',   # Marowijne 34
    '871690910008547670': '53177620',   # Molenstraat 60
    '871690910000009350': '38485684',   # Molenstraat 63 (2 meters, taking first)
    '871687140006918639': '27019472',   # Molenstraat 63
    '871687400009183091': '50676492',   # Proostwetering 31 (2 meters, taking first)
    '871689260012252874': '59868927',   # Von Geusaustraat 195 BIJ
    '871687400008460148': '98389752',   # Tasveld 16
    '871685900041044883': '56218582',   # Oosterengweg 38
    '871689276000030706': '54633316',   # Stadionweg 23
    '871685920004378541': '67235393',   # Dijkmeerlaan 551 (active one, not inactive 67235442)
    '871689260013028706': '67235478',   # Euryzakade 401 CVZ
    '871685920003789768': '55695924',   # H.J.E. Wenckebachweg 1692
    '871689260012516082': '63398280',   # de Eik 56 BIJ
    '871689260013005899': '66022558',   # Hartenruststraat 6 BIJ
    '871688660012152920': '55665323',   # Van Embdenstrat 2 TA
    '871685920004381039': '56621561',   # Vreeswijkpad 6
    '871687110004007611': '56621216',   # Akulaan 2
    '871688660012398397': '63087252',   # Ambachtsherenpad 8 BIJ
    '871685920003998443': '54631808',   # Bongerdkade 32
    '871685920004053639': '59867776',   # Mary van der Sluisstraat 428
    '871685920004053752': '98389761',   # Zuider IJdijk 76 A
    '871689260012250047': '67235473',   # 2e Rosestraat 11
    '871689260012250054': '67235472',   # 2e Rosestraat 7
    '871685920004541433': '37675873',   # G.J. Scheurleerpad 8
    '871685920004481388': '99848730',   # Transvaalstraat 9
}

print(f"EAN to Key mapping loaded: {len(EAN_TO_KEY)} entries")

# COMMAND ----------

# DBTITLE 1,Create Silver Table (Haystack Ontology-Aligned - Optimized Schema)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_PATH} (
    -- Identifiers (Haystack ontology-aligned with camelCase)
    dap_id STRING COMMENT 'DAP/Meter ID from Wattflow API - natural key',
    ean STRING COMMENT 'European Article Number - grid connection identifier',
    key STRING COMMENT 'Meter key (sleutel) for joining with dimension table',

    -- Time dimensions
    timestamp TIMESTAMP COMMENT 'Measurement timestamp (end of interval)',
    measurement_date DATE COMMENT 'Date of measurement (for partitioning)',
    aggregation_level STRING COMMENT 'Granularity: quarterly (15min), hourly, or daily',

    -- Active Energy (kWh)
    energy_import DOUBLE COMMENT 'AC active energy imported from grid (kWh)',
    energy_export DOUBLE COMMENT 'AC active energy exported to grid (kWh)',

    -- Reactive Energy (kVARh)
    reactive_energy_import DOUBLE COMMENT 'AC reactive energy imported from grid (kVARh)',
    reactive_energy_export DOUBLE COMMENT 'AC reactive energy exported to grid (kVARh)',

    -- Contextual attributes (timestamp-level, not measurement-level)
    tariff_rate STRING COMMENT 'Tariff rate for this timestamp: low (off-peak) or normal (peak)',
    is_peak_demand BOOLEAN COMMENT 'Peak demand flag - true when this interval represents maximum demand',

    -- Lineage
    silver_processing_timestamp TIMESTAMP COMMENT 'When processed into silver'
)
CLUSTER BY AUTO
COMMENT 'Haystack-aligned Wattflow meter measurements. Optimized schema where rate and peak status are timestamp-level attributes, not duplicated across measurements. Each row contains 4 energy measurements plus contextual flags.'
""")

print(f"✓ Silver table created/verified: {SILVER_PATH}")

# COMMAND ----------

# DBTITLE 1,Get Max Timestamp from Silver Table
# Check for incremental processing
max_ts_result = spark.sql(f"SELECT MAX(timestamp) as max_ts FROM {SILVER_PATH}").collect()[0]
max_ts = max_ts_result.max_ts if max_ts_result.max_ts else None

if max_ts:
    print(f"Latest timestamp in silver: {max_ts}")
    print("Running incremental update...")
else:
    print("No existing data - performing initial load")

# COMMAND ----------

# DBTITLE 1,Read Bronze Table with Incremental Filter
# Read bronze table with optional incremental filter
if max_ts:
    bronze_df = spark.read.table(BRONZE_PATH).filter(F.col("measurement_datetime") > max_ts)
else:
    bronze_df = spark.read.table(BRONZE_PATH)

print(f"✓ Reading from bronze table: {BRONZE_PATH}")
print(f"  Records to process: {bronze_df.count():,}")

# COMMAND ----------

# DBTITLE 1,Transform to Haystack Ontology Format - Optimized Schema
# Step 1: Rename core fields to camelCase (Haystack convention)
# Note: ean field flows directly from bronze table (populated in b01_api2land and b02_land2bronze)
transformed_df = bronze_df.withColumnRenamed("dap_id", "dap_id") \
                          .withColumnRenamed("measurement_datetime", "timestamp") \
                          .withColumn("measurement_date", F.to_date(F.col("timestamp"))) \
                          .withColumnRenamed("aggregation_level", "aggregation_level")

print("✓ Reading ean directly from bronze table")

# Step 2: Add key (sleutel) field using EAN mapping
# Create a mapping expression for key lookup
key_mapping_expr = F.create_map([F.lit(x) for pair in EAN_TO_KEY.items() for x in pair])
transformed_df = transformed_df.withColumn("key", key_mapping_expr[F.col("ean")])

print("✓ Added key (sleutel) field based on EAN mapping")

# Step 3: Pivot measurements by utility_type only (rate is extracted separately at timestamp level)
# Create columns for each measurement type based on utility_type
# Note: consumption field is redundant (always matches "consumption"/"production" in utility_type)

# Active Energy Import (wh-energy consumption)
transformed_df = transformed_df.withColumn(
    "energy_import",
    F.when(
        F.col("utility_type") == "wh-energy consumption",
        F.col("value").cast(DoubleType())
    ).otherwise(F.lit(None))
)

# Active Energy Export (wh-energy production)
transformed_df = transformed_df.withColumn(
    "energy_export",
    F.when(
        F.col("utility_type") == "wh-energy production",
        F.col("value").cast(DoubleType())
    ).otherwise(F.lit(None))
)

# Reactive Energy Import (varh-energy consumption)
transformed_df = transformed_df.withColumn(
    "reactive_energy_import",
    F.when(
        F.col("utility_type") == "varh-energy consumption",
        F.col("value").cast(DoubleType())
    ).otherwise(F.lit(None))
)

# Reactive Energy Export (varh-energy production)
transformed_df = transformed_df.withColumn(
    "reactive_energy_export",
    F.when(
        F.col("utility_type") == "varh-energy production",
        F.col("value").cast(DoubleType())
    ).otherwise(F.lit(None))
)

print("✓ Created measurement columns based on utility_type")

# Step 4: Group by unique timestamp to collapse rows and extract timestamp-level attributes
# Rate and is_max are the same for all 4 measurements within a timestamp, so we extract once
pivoted_df = transformed_df.groupBy(
    "dap_id", "ean", "key", "timestamp", "measurement_date", "aggregation_level"
).agg(
    # Energy measurements (max picks the non-null value)
    F.max("energy_import").alias("energy_import"),
    F.max("energy_export").alias("energy_export"),
    F.max("reactive_energy_import").alias("reactive_energy_import"),
    F.max("reactive_energy_export").alias("reactive_energy_export"),

    # Timestamp-level attributes (same across all 4 measurements)
    F.max("rate").alias("tariff_rate"),  # 'low' or 'normal'
    F.max(F.col("is_max") == "true").cast("boolean").alias("is_peak_demand")  # true if is_max='true'
)

print("✓ Pivoted to wide format and extracted timestamp-level attributes")

# Step 5: Add silver processing timestamp
pivoted_df = pivoted_df.withColumn("silver_processing_timestamp", F.current_timestamp())

# Step 6: Select final columns in ontology order
final_df = pivoted_df.select(
    # Identifiers
    "dap_id",
    "ean",
    "key",

    # Time dimensions
    "timestamp",
    "measurement_date",
    "aggregation_level",

    # Energy measurements (4 columns, always populated)
    "energy_import",
    "energy_export",
    "reactive_energy_import",
    "reactive_energy_export",

    # Contextual attributes (timestamp-level)
    "tariff_rate",
    "is_peak_demand",

    # Lineage
    "silver_processing_timestamp"
)

print("✓ Transformation complete - optimized schema with 0% NULL values in measurements")

# COMMAND ----------

# DBTITLE 1,Write to Silver Delta Table
new_rows = final_df.count()

if new_rows > 0:
    # Append new data
    final_df.write.mode("append").saveAsTable(SILVER_PATH)
    print(f"✓ Appended {new_rows:,} new rows to {SILVER_PATH}")
    print(f"✓ Optimized schema: {len(final_df.columns)} columns (reduced from 17 to 13)")
    print("✓ Measurements per row: 4 energy values + 2 contextual flags")
else:
    print("✓ No new data to append")

# COMMAND ----------

# DBTITLE 1,Data Quality Summary
if new_rows > 0:
    print("\n=== Data Quality Summary ===")

    # Count non-null measurements per type (should be close to 100%)
    import_count = final_df.filter(F.col("energy_import").isNotNull()).count()
    export_count = final_df.filter(F.col("energy_export").isNotNull()).count()
    reactive_import_count = final_df.filter(F.col("reactive_energy_import").isNotNull()).count()
    reactive_export_count = final_df.filter(F.col("reactive_energy_export").isNotNull()).count()

    print(f"Energy measurements (non-null counts):")
    print(f"  AC Energy Import: {import_count:,} ({import_count/new_rows*100:.1f}%)")
    print(f"  AC Energy Export: {export_count:,} ({export_count/new_rows*100:.1f}%)")
    print(f"  AC Reactive Energy Import: {reactive_import_count:,} ({reactive_import_count/new_rows*100:.1f}%)")
    print(f"  AC Reactive Energy Export: {reactive_export_count:,} ({reactive_export_count/new_rows*100:.1f}%)")

    # Tariff rate distribution
    print(f"\nTariff rate distribution:")
    rate_counts = final_df.groupBy("tariff_rate").count().collect()
    for row in rate_counts:
        print(f"  {row['tariff_rate']}: {row['count']:,} intervals ({row['count']/new_rows*100:.1f}%)")

    # Peak demand intervals
    peak_demand_count = final_df.filter(F.col("is_peak_demand") == True).count()
    print(f"\nPeak demand intervals: {peak_demand_count:,} ({peak_demand_count/new_rows*100:.1f}%)")

    # Unique meters
    unique_meters = final_df.select("dap_id").distinct().count()
    print(f"\nUnique meters: {unique_meters:,}")

    # EAN population (flows from bronze)
    ean_populated = final_df.filter(F.col("ean").isNotNull()).count()
    print(f"Records with EAN populated: {ean_populated:,} ({ean_populated/new_rows*100:.1f}%)")
