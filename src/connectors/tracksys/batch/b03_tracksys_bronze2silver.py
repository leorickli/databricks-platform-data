# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql import functions as F

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("bronze_table_name", "tracksys_batch", "Bronze unified table name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_PATH = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

dbutils.widgets.text("silver_table_name", "tracksys_batch", "Silver table name")
SILVER_TABLE_NAME = dbutils.widgets.get("silver_table_name")
SILVER_PATH = f"{CATALOG_NAME}.silver.{SILVER_TABLE_NAME}"

dbutils.widgets.text("report_table_name", "imei_signal_mappings_batch", "IMEI signal mappings table name")
REPORT_TABLE_NAME = dbutils.widgets.get("report_table_name")
REPORT_PATH = f"{CATALOG_NAME}.operational.{REPORT_TABLE_NAME}"

# Signal mappings - priority order (first found wins)
SIGNAL_MAPPINGS = {
    'soc': ['soc', 'bms_soc', 'libal_soc', 'bms_soc_1', 'libal_soc_1'],
    'voltage': ['voltage', 'core_voltage_level', 'bms_pack_voltage', 'libal_packcurrent', 'bms_packvoltage_1', 'libal_packcurrent_1', 'outputvoltage', 'output_v', 'voltage_level'],
    'current': ['current', 'core_current_level', 'bms_pack_current', 'libal_packcurrent', 'bms_pack_current_1', 'libal_packcurrent_1', 'outputcurrent', 'output_a', 'current_level']
}

# COMMAND ----------

# DBTITLE 1,Discover IMEIs from Unified Bronze Table
# Get distinct IMEIs from the unified bronze table
imei_df = spark.sql(f"""
    SELECT DISTINCT imei
    FROM {BRONZE_PATH}
    WHERE imei IS NOT NULL
    ORDER BY imei
""")

imei_list = [row.imei for row in imei_df.collect()]
print(f"Found {len(imei_list)} IMEIs in unified bronze table")

# COMMAND ----------

# DBTITLE 1,Detect Signals for Each IMEI
def detect_signals_for_imei(imei):
    """
    Detect which signals are available for a given IMEI in the unified table
    Returns dict of metric -> signal_name mappings
    """
    # Get distinct signal names for this IMEI from unified table
    available_signals = spark.sql(f"""
        SELECT DISTINCT signal_name
        FROM {BRONZE_PATH}
        WHERE imei = '{imei}' 
          AND signal_name IS NOT NULL
    """).collect()
    
    available_set = {row.signal_name for row in available_signals}
    
    # Find matches for each metric type
    detected = {}
    for metric, possible_signals in SIGNAL_MAPPINGS.items():
        for signal in possible_signals:
            if signal in available_set:
                detected[metric] = signal
                break
    
    return detected

# Detect signals for each IMEI
print("Detecting signal mappings for each IMEI...")
imei_signal_map = {}
for idx, imei in enumerate(imei_list):
    signals = detect_signals_for_imei(imei)
    imei_signal_map[imei] = signals
    
    # Print progress every 10 IMEIs
    if (idx + 1) % 10 == 0:
        print(f"  Processed {idx + 1}/{len(imei_list)} IMEIs...")

print(f"\n✓ Signal detection complete for {len(imei_list)} IMEIs")

# Show summary of signal availability
has_soc = sum(1 for s in imei_signal_map.values() if 'soc' in s)
has_voltage = sum(1 for s in imei_signal_map.values() if 'voltage' in s)
has_current = sum(1 for s in imei_signal_map.values() if 'current' in s)

print(f"\nSignal availability summary:")
print(f"  SOC signals found: {has_soc}/{len(imei_list)} IMEIs")
print(f"  Voltage signals found: {has_voltage}/{len(imei_list)} IMEIs")
print(f"  Current signals found: {has_current}/{len(imei_list)} IMEIs")

# COMMAND ----------

# DBTITLE 1,Build Dynamic Union Query for Unified Table
def build_imei_query(imei, signals):
    """Build SQL query for a single IMEI with dynamic signal mapping from unified table"""

    # Build the CASE statements for each metric
    select_parts = [
        f"CAST('{imei}' AS BIGINT) AS loggerImei",
        "timestamp"
    ]

    where_signals = []

    if signals.get('soc'):
        select_parts.append(f"""MAX(CASE WHEN signal_name = '{signals['soc']}'
                                THEN CAST(signal_value AS INT)
                                END) AS SoC""")
        where_signals.append(f"'{signals['soc']}'")
    else:
        select_parts.append("CAST(NULL AS INT) AS SoC")

    if signals.get('voltage'):
        select_parts.append(f"""MAX(CASE WHEN signal_name = '{signals['voltage']}'
                                THEN CAST(signal_value AS DOUBLE)
                                END) AS voltage""")
        where_signals.append(f"'{signals['voltage']}'")
    else:
        select_parts.append("CAST(NULL AS DOUBLE) AS voltage")

    if signals.get('current'):
        select_parts.append(f"""MAX(CASE WHEN signal_name = '{signals['current']}'
                                THEN CAST(signal_value AS DOUBLE)
                                END) AS current""")
        where_signals.append(f"'{signals['current']}'")
    else:
        select_parts.append("CAST(NULL AS DOUBLE) AS current")
    
    # Build the query - now reading from unified table with IMEI filter
    if where_signals:
        where_clause = f"""WHERE imei = '{imei}' 
                          AND signal_name IN ({', '.join(where_signals)})"""
    else:
        where_clause = f"WHERE imei = '{imei}' AND 1=0"  # No signals found
    
    query = f"""
    SELECT {', '.join(select_parts)}
    FROM {BRONZE_PATH}
    {where_clause}
    GROUP BY timestamp
    """
    
    return query

# Build the full union query
print("\nBuilding transformation query...")
union_queries = []
for imei, signals in imei_signal_map.items():
    if signals:  # Only include IMEIs with at least one signal
        union_queries.append(build_imei_query(imei, signals))

if union_queries:
    full_query = " UNION ALL ".join(union_queries)
    print(f"✓ Query built for {len(union_queries)} IMEIs with valid signals")
else:
    print("WARNING: No IMEIs with valid signals found!")
    full_query = """SELECT
                    CAST(NULL AS BIGINT) AS loggerImei,
                    CAST(NULL AS TIMESTAMP) AS timestamp,
                    CAST(NULL AS INT) AS SoC,
                    CAST(NULL AS DOUBLE) AS voltage,
                    CAST(NULL AS DOUBLE) AS current
                    WHERE 1=0"""

# COMMAND ----------

# DBTITLE 1,Alternative Optimized Query (Single Pass)
# This is a more efficient alternative that processes all IMEIs in a single query
# Uncomment to use this approach instead

optimized_query = f"""
WITH signal_pivot AS (
    SELECT
        CAST(imei AS BIGINT) AS loggerImei,
        timestamp,
        MAX(CASE
            WHEN signal_name IN ('soc', 'bms_soc', 'bms_soc_1', 'libal_soc_1')
            THEN CAST(signal_value AS INT)
        END) AS SoC,
        MAX(CASE
            WHEN signal_name IN ('voltage', 'bms_pack_voltage', 'outputvoltage', 'output_v', 'voltage_level')
            THEN CAST(signal_value AS DOUBLE)
        END) AS voltage,
        MAX(CASE
            WHEN signal_name IN ('current', 'bms_pack_current', 'libal_packcurrent_1', 'output_a')
            THEN CAST(signal_value AS DOUBLE)
        END) AS current
    FROM {BRONZE_PATH}
    WHERE signal_name IN (
        'soc', 'bms_soc', 'bms_soc_1', 'libal_soc_1',
        'voltage', 'bms_pack_voltage', 'outputvoltage', 'output_v', 'voltage_level',
        'current', 'bms_pack_current', 'libal_packcurrent_1', 'output_a'
    )
    GROUP BY imei, timestamp
)
SELECT * FROM signal_pivot
WHERE SoC IS NOT NULL OR voltage IS NOT NULL OR current IS NOT NULL
"""

# To use the optimized query, replace full_query with optimized_query
# full_query = optimized_query

# COMMAND ----------

# DBTITLE 1,Create or Update Silver Table
# Check if table exists
table_exists = spark.catalog.tableExists(SILVER_PATH)

if not table_exists:
    print(f"Creating new table: {SILVER_PATH}")

    spark.sql(f"""
    CREATE TABLE {SILVER_PATH} (
        loggerImei BIGINT COMMENT 'International Mobile Equipment Identity of the data logger device (originalIdInSource on the ESDL Battery asset).',
        timestamp TIMESTAMP COMMENT 'Signal measurement time',
        SoC INT COMMENT 'State of charge percentage (ESDL Battery.SoC, %)',
        voltage DOUBLE COMMENT 'Battery voltage in volts (ESDL Battery.V, V — DC Bus)',
        current DOUBLE COMMENT 'Battery current in amperes (ESDL Battery.A, A — DC Bus)',
        activePower DOUBLE COMMENT 'Battery active power in watts, derived as voltage * current (ESDL Battery.W, W)'
    ) CLUSTER BY AUTO
    COMMENT 'Standardized time-series telemetry data aggregated from all vehicles, with ESDL Battery-aligned naming across different hardware configurations'
    """)

    # Insert data
    print("Inserting initial data...")
    spark.sql(f"""
    INSERT INTO {SILVER_PATH}
    WITH telemetry_data AS ({full_query})
    SELECT
        loggerImei,
        timestamp,
        SoC,
        voltage,
        current,
        voltage * current AS activePower
    FROM telemetry_data
    WHERE timestamp IS NOT NULL
    """)
    
    row_count = spark.table(SILVER_PATH).count()
    print(f"✓ Created table with {row_count:,} rows")
    
else:
    print("Table exists. Running incremental update...")
    
    # Get max timestamp from existing table
    max_ts_row = spark.sql(f"SELECT MAX(timestamp) as max_ts FROM {SILVER_PATH}").collect()[0]
    max_ts = max_ts_row.max_ts
    
    if max_ts:
        print(f"Latest timestamp in silver: {max_ts}")
    else:
        print("Silver table is empty, loading all data...")
    
    # Create temp view with new data
    incremental_query = f"""
    WITH telemetry_data AS ({full_query})
    SELECT
        loggerImei,
        timestamp,
        SoC,
        voltage,
        current,
        voltage * current AS activePower
    FROM telemetry_data
    WHERE timestamp IS NOT NULL
    {'AND timestamp > CAST("' + str(max_ts) + '" AS TIMESTAMP)' if max_ts else ''}
    """
    
    new_data_df = spark.sql(incremental_query)
    new_rows = new_data_df.count()
    
    if new_rows > 0:
        # Append new data
        new_data_df.write.mode("append").saveAsTable(SILVER_PATH)
        print(f"✓ Appended {new_rows:,} new rows")
    else:
        print("No new data to append")

# COMMAND ----------

# DBTITLE 1,Create Signal Detection Report
# Create report data
print("\nGenerating signal detection report...")
report_data = []
for imei, signals in imei_signal_map.items():
    row_data = {'imei': imei}
    
    # Dynamically add signal columns based on SIGNAL_MAPPINGS
    for metric in SIGNAL_MAPPINGS.keys():
        row_data[f'{metric}_signal'] = signals.get(metric, 'NOT FOUND')
    
    report_data.append(row_data)

# Create DataFrame with proper column order
fixed_columns = ['imei']
signal_columns = [f'{metric}_signal' for metric in SIGNAL_MAPPINGS.keys()]
all_columns = fixed_columns + signal_columns

report_df = spark.createDataFrame(report_data).select(*all_columns)

# Add timestamp
report_df_final = report_df.withColumn('detection_timestamp', F.current_timestamp())

# Create table with description first
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REPORT_PATH} (
    imei STRING COMMENT 'International Mobile Equipment Identity of the data logger device',
    soc_signal STRING COMMENT 'State of charge signal name',
    voltage_signal STRING COMMENT 'Voltage signal name', 
    current_signal STRING COMMENT 'Current signal name',
    detection_timestamp TIMESTAMP COMMENT 'Analysis timestamp'
) COMMENT 'Operational audit table tracking signal name mappings discovered for each IMEI during batch processing, used for monitoring data quality and troubleshooting.'
""")

# Write data to existing table
report_df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(REPORT_PATH)

print(f"✓ Created {REPORT_PATH} with {len(report_data)} rows")

# COMMAND ----------

# DBTITLE 1,Display Summary Statistics
# Show summary of the silver table
print("Silver Table Summary:")
print("=" * 50)

summary_stats = spark.sql(f"""
    SELECT
        COUNT(DISTINCT loggerImei) as unique_imeis,
        COUNT(*) as total_rows,
        MIN(timestamp) as earliest_timestamp,
        MAX(timestamp) as latest_timestamp,
        COUNT(DISTINCT DATE(timestamp)) as unique_days,
        SUM(CASE WHEN SoC IS NOT NULL THEN 1 ELSE 0 END) as soc_records,
        SUM(CASE WHEN voltage IS NOT NULL THEN 1 ELSE 0 END) as voltage_records,
        SUM(CASE WHEN current IS NOT NULL THEN 1 ELSE 0 END) as current_records
    FROM {SILVER_PATH}
""").collect()[0]

print(f"Unique IMEIs: {summary_stats['unique_imeis']:,}")
print(f"Total rows: {summary_stats['total_rows']:,}")
print(f"Date range: {summary_stats['earliest_timestamp']} to {summary_stats['latest_timestamp']}")
print(f"Unique days: {summary_stats['unique_days']}")
print(f"\nSignal coverage:")
print(f"  SOC records: {summary_stats['soc_records']:,}")
print(f"  Voltage records: {summary_stats['voltage_records']:,}")
print(f"  Current records: {summary_stats['current_records']:,}")

# Display the report table
print("\n" + "=" * 50)
print("Signal Detection Report (first 20 IMEIs):")
display(spark.table(REPORT_PATH).orderBy('imei').limit(20))
