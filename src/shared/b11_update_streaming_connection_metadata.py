# Databricks notebook source
# DBTITLE 1,Update Streaming Connection Metadata (Multi-Connection Support)
"""
This notebook updates the connection status metadata for STREAMING pipelines.

Similar to b06 (batch), but designed to run continuously as a parallel task
within streaming jobs, updating metadata every 15 minutes.

Key Differences from b06 (Batch):
- Runs continuously with internal 15-minute loop (not job-based schedule)
- Queries streaming silver tables that are continuously updated
- No job completion state (streaming jobs run 24/7)
- Focuses on data freshness from silverProcessingTimestamp

Usage:
1. Single connection (specific device):
   - connection_id: "kafka_inverters_device_123"

2. Multiple connections (connector-based):
   - connector: "kafka_inverters"
   - Will update all devices for that connector

Process:
1. Reads connection config(s) from metadata.connection_config
2. Every 15 minutes:
   - Queries each silver table for latest data timestamp
   - Calculates connection status (ACTIVE, DELAYED, OFFLINE)
   - Calculates hourly quality metrics (hourly_metrics ARRAY<STRUCT>):
     Per-hour: data_points, expected_records, completeness_pct, duplicate_timestamps,
     null_values, avg_delay_minutes, max_delay_minutes, uptime_pct
   - Appends historical record to metadata.connection_status table

Status Logic:
- OFFLINE: No data for > 7 days
- DELAYED: Data is 2x+ expected interval but < 7 days old
- ACTIVE: Data is within 2x expected interval

This runs as a continuous task within streaming pipeline jobs.
"""

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import datetime, timezone, timedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, IntegerType, DecimalType, DoubleType, ArrayType
import traceback
import time

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.text("catalog_name", "globex_dev", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

# Option 1: Single connection mode
dbutils.widgets.text("connection_id", "", "Single Connection ID (leave empty to use connector)")
CONNECTION_ID = dbutils.widgets.get("connection_id")

# Option 2: Multi-connection mode (for connectors with multiple devices)
dbutils.widgets.text("connector", "", "Connector name (e.g., kafka_inverters, tracksys_stream)")
CONNECTOR = dbutils.widgets.get("connector")

# Validate parameters
if not CONNECTION_ID and not CONNECTOR:
    raise ValueError("Must provide either connection_id OR connector parameter")

if CONNECTION_ID and CONNECTOR:
    print(f"⚠ Both connection_id and connector provided. Using connection_id: {CONNECTION_ID}")
    CONNECTOR = None

# COMMAND ----------

# DBTITLE 1,Create Metadata Schema if Not Exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.metadata")
print(f"✓ Schema {CATALOG_NAME}.metadata ready")

# COMMAND ----------

# DBTITLE 1,Create Metadata Tables
config_table = f"{CATALOG_NAME}.metadata.connection_config"
status_table = f"{CATALOG_NAME}.metadata.connection_status"

# Create connection_config table (same as b06)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {config_table} (
    -- Primary Key
    connection_id STRING NOT NULL COMMENT 'Unique connection identifier (e.g., kafka_inverters_device_123)',

    -- Display Information
    data_name STRING COMMENT 'Human-readable connection name',
    type STRING COMMENT 'Connection type (e.g., IoT sensor, Smart meter)',
    data_type STRING COMMENT 'Equipment type (Battery, Inverter, Charging Station, Electricity Meter)',
    protocol STRING COMMENT 'Communication protocol (HTTP, MQTT, Kafka)',

    -- Location & Object References
    location STRING COMMENT 'Physical location of the device',
    object_id STRING COMMENT 'Business object reference (e.g., ACME Bunnik ABCD)',

    -- Client & Brand Information
    client STRING COMMENT 'Client name (ACME, GLOBEX)',
    brand STRING COMMENT 'Device manufacturer (Sunpeak, Voltcore, Solarflow, TRACKSYS, Wattflow, SMA)',
    connector STRING COMMENT 'Data source connector (kafka_inverters, tracksys_stream, etc.)',
    processing_type STRING COMMENT 'Batch, Real-Time, or Event-Driven',

    -- Pipeline Configuration
    job_id STRING COMMENT 'Databricks job ID for this pipeline',
    job_name STRING COMMENT 'Databricks job name',
    silver_table_name STRING COMMENT 'Fully qualified silver table name for streaming (catalog.schema.table)',
    expected_interval_minutes INT COMMENT 'Expected data refresh interval in minutes',

    -- Status Threshold Configuration
    delayed_threshold_days INT COMMENT 'Days before connection marked DELAYED (default: 2 for batch, 0 for real-time)',
    offline_threshold_days INT COMMENT 'Days before connection marked OFFLINE (default: 4)',

    -- Flexible Metadata
    other_metadata STRING COMMENT 'JSON string for additional metadata',

    -- Audit
    created_at TIMESTAMP COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP COMMENT 'Last update timestamp',

    CONSTRAINT connection_config_pk PRIMARY KEY (connection_id)
)
COMMENT 'Static configuration for data connections - supports both batch and streaming pipelines'
""")

# Create connection_status table (lean schema: core fields + hourly_metrics array)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {status_table} (
    -- Connection Identifier
    connection_id STRING NOT NULL COMMENT 'Foreign key to connection_config.connection_id',

    -- Static Fields (copied from config for easy access)
    data_name STRING COMMENT 'Human-readable connection name',
    type STRING COMMENT 'Connection type',
    data_type STRING COMMENT 'Equipment type',
    protocol STRING COMMENT 'Communication protocol',
    location STRING COMMENT 'Physical location',
    object_id STRING COMMENT 'Business object reference',
    brand STRING COMMENT 'Device manufacturer',
    processing_type STRING COMMENT 'Batch, Real-Time, or Event-Driven',

    -- Dynamic Status Fields
    status STRING COMMENT 'Connection status: ACTIVE, DELAYED, OFFLINE',
    last_sync_timestamp TIMESTAMP COMMENT 'Timestamp of last successful data sync',

    -- Job Execution Metadata (for streaming, this is the scheduled check)
    job_run_id STRING COMMENT 'Databricks job run ID for this check',
    job_state STRING COMMENT 'Job run state (SUCCESS, FAILED, UNKNOWN)',

    -- Hourly Granular Metrics (24 hours)
    hourly_metrics ARRAY<STRUCT<
        hour: TIMESTAMP,
        data_points: INT,
        expected_records: INT,
        completeness_pct: DOUBLE,
        duplicate_timestamps: INT,
        null_values: INT,
        avg_delay_minutes: DOUBLE,
        max_delay_minutes: DOUBLE,
        uptime_pct: DOUBLE
    >> COMMENT 'Hourly metrics for the previous 24 hours',

    -- Audit (Snapshot Timestamp)
    checked_at TIMESTAMP NOT NULL COMMENT 'When this status check was performed'
)
CLUSTER BY AUTO
COMMENT 'Historical connection status tracking - appends a new record every 15 minutes for streaming pipelines'
""")

print(f"✓ Metadata tables ready:")
print(f"  - {config_table}")
print(f"  - {status_table}")

# COMMAND ----------

# DBTITLE 1,Get Connection Configs
# Check if config has data
configs_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {config_table}").collect()[0]['cnt']
if configs_count == 0:
    raise ValueError(f"Configuration table {config_table} exists but has no data. Please insert connection configurations first.")

# Read config(s) based on mode
if CONNECTION_ID:
    # Single connection mode
    configs_df = spark.sql(f"""
        SELECT *
        FROM {config_table}
        WHERE connection_id = '{CONNECTION_ID}'
    """)
    mode = "single"
    print(f"📍 Single connection mode: {CONNECTION_ID}")
else:
    # Multi-connection mode
    configs_df = spark.sql(f"""
        SELECT *
        FROM {config_table}
        WHERE connector = '{CONNECTOR}'
    """)
    mode = "multi"
    print(f"📍 Multi-connection mode: {CONNECTOR}")

configs = configs_df.collect()

if len(configs) == 0:
    error_msg = f"No configuration found for "
    error_msg += f"connection_id '{CONNECTION_ID}'" if CONNECTION_ID else f"connector '{CONNECTOR}'"
    raise ValueError(f"{error_msg}. Please add it to {config_table}.")

print(f"✓ Loaded {len(configs)} connection(s)")
for config in configs:
    print(f"  - {config['connection_id']}: {config['data_name']}")

# COMMAND ----------

# DBTITLE 1,Get Current Job Run Information
try:
    # Get job context from Spark configuration
    job_id_val = spark.conf.get("spark.databricks.job.id", None)
    run_id_val = spark.conf.get("spark.databricks.job.runId", None)

    job_run_id = f"{job_id_val}_{run_id_val}" if job_id_val and run_id_val else "unknown"
    job_id = job_id_val

    # For streaming, we're just doing periodic checks (not marking job as SUCCESS/FAILED)
    job_state = "RUNNING"

    print(f"✓ Job run ID: {job_run_id}")
    print(f"  Job ID: {job_id}")
    print(f"  Job state: {job_state}")

except Exception as e:
    print(f"⚠ Could not retrieve job context (might be running interactively): {e}")
    job_run_id = "interactive_run"
    job_id = None
    job_state = "UNKNOWN"

# COMMAND ----------

# DBTITLE 1,Define Metadata Update Function
def update_connection_metadata():
    """Update connection metadata - called every 15 minutes"""
    status_records = []
    errors = []

    for config in configs:
        connection_id = config['connection_id']
        silver_table = config['silver_table_name']
        expected_interval = config['expected_interval_minutes']

        print(f"\n{'='*60}")
        print(f"Processing: {connection_id} ({config['data_name']})")
        print(f"{'='*60}")

        try:
            # Check if silver table exists
            if not spark.catalog.tableExists(silver_table):
                print(f"⚠ Silver table {silver_table} does not exist")
                status = "OFFLINE"
                latest_record_timestamp = None
                hourly_metrics = []
            else:
                # Get latest record timestamp
                latest_result = spark.sql(f"""
                    SELECT MAX(silverProcessingTimestamp) as latest_record_timestamp
                    FROM {silver_table}
                """).collect()[0]
                latest_record_timestamp = latest_result['latest_record_timestamp']

                print(f"✓ Silver table exists")
                print(f"  Latest record: {latest_record_timestamp}")

                # Calculate 24h window for hourly aggregation
                now = datetime.now(timezone.utc)
                yesterday_start = (now - timedelta(days=1)).replace(minute=0, second=0, microsecond=0)
                yesterday_start_sql = yesterday_start.strftime('%Y-%m-%d %H:%M:%S')
                now_sql = now.strftime('%Y-%m-%d %H:%M:%S')

                # Get table columns to check for delay and null-check fields
                table_columns = [field.name for field in spark.table(silver_table).schema.fields]
                has_ingestion_ts = 'ingestionTimestamp' in table_columns

                # Build delay columns for the hourly query
                if has_ingestion_ts:
                    delay_select = """,
                        AVG((UNIX_TIMESTAMP(ingestionTimestamp) - UNIX_TIMESTAMP(timestamp)) / 60) as avg_delay_min,
                        MAX((UNIX_TIMESTAMP(ingestionTimestamp) - UNIX_TIMESTAMP(timestamp)) / 60) as max_delay_min"""
                else:
                    delay_select = """,
                        NULL as avg_delay_min,
                        NULL as max_delay_min"""

                # Build null check for critical fields
                critical_fields = ['timestamp', 'W', 'WH', 'Hz', 'PhVphA', 'TmpCab']
                existing_critical_fields = [f for f in critical_fields if f in table_columns]
                if existing_critical_fields:
                    null_conditions = " OR ".join([f"{field} IS NULL" for field in existing_critical_fields])
                    null_select = f""",
                        SUM(CASE WHEN ({null_conditions}) THEN 1 ELSE 0 END) as null_count"""
                else:
                    null_select = """,
                        0 as null_count"""

                # Single query to get all hourly metrics for the last 24 hours
                hourly_metrics_query = f"""
                    SELECT
                        DATE_TRUNC('hour', silverProcessingTimestamp) as hour,
                        COUNT(*) as data_points,
                        COUNT(DISTINCT timestamp) as unique_timestamps
                        {delay_select}
                        {null_select}
                    FROM {silver_table}
                    WHERE silverProcessingTimestamp >= '{yesterday_start_sql}'
                      AND silverProcessingTimestamp < '{now_sql}'
                    GROUP BY DATE_TRUNC('hour', silverProcessingTimestamp)
                    ORDER BY hour ASC
                """

                hourly_results = spark.sql(hourly_metrics_query).collect()
                print(f"✓ Retrieved hourly metrics ({len(hourly_results)} hours)")

                # Build hourly metrics array
                hourly_metrics = []
                expected_records_per_hour = int(60 // expected_interval)

                for row in hourly_results:
                    hour_start = row['hour']
                    data_points = row['data_points']
                    unique_timestamps = row['unique_timestamps']

                    # Completeness for this hour
                    completeness_pct_hour = round((data_points / expected_records_per_hour) * 100, 2) if expected_records_per_hour > 0 else 0
                    completeness_pct_hour = min(100.0, completeness_pct_hour)

                    # Uptime: binary (data received = up)
                    uptime_pct_hour = 100.0 if data_points > 0 else 0.0

                    # Duplicates
                    duplicate_count = data_points - unique_timestamps

                    # Delay metrics
                    avg_delay = round(float(row['avg_delay_min']), 2) if row['avg_delay_min'] is not None else None
                    max_delay = round(float(row['max_delay_min']), 2) if row['max_delay_min'] is not None else None

                    # Null values
                    null_values = int(row['null_count'] or 0)

                    hourly_metrics.append((
                        hour_start,                    # hour: TIMESTAMP
                        int(data_points),              # data_points: INT
                        expected_records_per_hour,     # expected_records: INT
                        float(completeness_pct_hour),  # completeness_pct: DOUBLE
                        int(duplicate_count),          # duplicate_timestamps: INT
                        null_values,                   # null_values: INT
                        avg_delay,                     # avg_delay_minutes: DOUBLE
                        max_delay,                     # max_delay_minutes: DOUBLE
                        float(uptime_pct_hour)         # uptime_pct: DOUBLE
                    ))

                # Calculate overall completeness from hourly metrics
                if hourly_metrics:
                    overall_completeness = sum(h[3] for h in hourly_metrics) / len(hourly_metrics)
                else:
                    overall_completeness = 0

                print(f"✓ Hourly metrics calculated:")
                print(f"  Hours processed: {len(hourly_metrics)}")
                print(f"  Expected records per hour: {expected_records_per_hour}")
                print(f"  Overall completeness: {overall_completeness:.2f}%")

            # Calculate minutes since last record
            current_time = datetime.now(timezone.utc)

            if latest_record_timestamp:
                if latest_record_timestamp.tzinfo is None:
                    latest_record_timestamp = latest_record_timestamp.replace(tzinfo=timezone.utc)
                minutes_since_last_record = (current_time - latest_record_timestamp).total_seconds() / 60
            else:
                minutes_since_last_record = None

            # Determine status based on thresholds
            try:
                delayed_threshold_days = config['delayed_threshold_days'] if config['delayed_threshold_days'] is not None else 0
            except (KeyError, TypeError):
                delayed_threshold_days = 0  # Default: 0 days for real-time (immediate)

            try:
                offline_threshold_days = config['offline_threshold_days'] if config['offline_threshold_days'] is not None else 1
            except (KeyError, TypeError):
                offline_threshold_days = 1  # Default: 1 day for real-time

            delayed_threshold_minutes = delayed_threshold_days * 24 * 60
            offline_threshold_minutes = offline_threshold_days * 24 * 60

            if minutes_since_last_record is None:
                status = "OFFLINE"
            elif minutes_since_last_record > offline_threshold_minutes:
                status = "OFFLINE"
            elif minutes_since_last_record > delayed_threshold_minutes:
                status = "DELAYED"
            elif hourly_metrics and overall_completeness < 80:
                status = "DELAYED"
            else:
                status = "ACTIVE"

            print(f"\n✓ Connection status: {status}")
            if minutes_since_last_record:
                print(f"  Minutes since last record: {minutes_since_last_record:.1f}")
            print(f"  Threshold for DELAYED: {delayed_threshold_minutes} minutes ({delayed_threshold_days} days)")
            print(f"  Threshold for OFFLINE: {offline_threshold_minutes} minutes ({offline_threshold_days} days)")

            # Create status record with hourly_metrics
            status_record = {
                "connection_id": connection_id,
                "data_name": config['data_name'],
                "type": config['type'],
                "data_type": config['data_type'],
                "protocol": config['protocol'],
                "location": config['location'],
                "object_id": config['object_id'],
                "brand": config['brand'],
                "processing_type": config['processing_type'],
                "status": status,
                "last_sync_timestamp": latest_record_timestamp,
                "job_run_id": job_run_id,
                "job_state": job_state,
                "hourly_metrics": hourly_metrics if hourly_metrics else None,
                "checked_at": datetime.now(timezone.utc)
            }

            status_records.append(status_record)
            print(f"✓ Status record prepared for {connection_id}")

        except Exception as e:
            error_msg = f"Error processing {connection_id}: {str(e)}"
            print(f"✗ {error_msg}")
            print(traceback.format_exc())
            errors.append(error_msg)

            # Create OFFLINE record on error
            status_record = {
                "connection_id": connection_id,
                "data_name": config['data_name'],
                "type": config['type'],
                "data_type": config['data_type'],
                "protocol": config['protocol'],
                "location": config['location'],
                "object_id": config['object_id'],
                "brand": config['brand'],
                "processing_type": config['processing_type'],
                "status": "OFFLINE",
                "last_sync_timestamp": None,
                "job_run_id": job_run_id,
                "job_state": "ERROR",
                "hourly_metrics": None,
                "checked_at": datetime.now(timezone.utc)
            }
            status_records.append(status_record)

    # Append to table
    if status_records:
        status_df = spark.createDataFrame(status_records, schema=status_schema)
        status_df.write.mode("append").saveAsTable(status_table)
        print(f"\n✓ Successfully appended {len(status_records)} connection status record(s) to {status_table}")

    return status_records, errors

# COMMAND ----------

# DBTITLE 1,Run Continuous Metadata Update Loop
print(f"\n{'='*60}")
print("Starting continuous metadata update loop (15-minute interval)")
print(f"{'='*60}\n")

# Define the schema once (outside the loop)
hourly_metrics_struct = StructType([
    StructField("hour", TimestampType(), True),
    StructField("data_points", IntegerType(), True),
    StructField("expected_records", IntegerType(), True),
    StructField("completeness_pct", DoubleType(), True),
    StructField("duplicate_timestamps", IntegerType(), True),
    StructField("null_values", IntegerType(), True),
    StructField("avg_delay_minutes", DoubleType(), True),
    StructField("max_delay_minutes", DoubleType(), True),
    StructField("uptime_pct", DoubleType(), True)
])

status_schema = StructType([
    StructField("connection_id", StringType(), False),
    StructField("data_name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("location", StringType(), True),
    StructField("object_id", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("processing_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("last_sync_timestamp", TimestampType(), True),
    StructField("job_run_id", StringType(), True),
    StructField("job_state", StringType(), True),
    StructField("hourly_metrics", ArrayType(hourly_metrics_struct), True),
    StructField("checked_at", TimestampType(), False)
])

# Continuous loop - runs every 15 minutes
while True:
    try:
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running metadata update...")

        status_records, errors = update_connection_metadata()

        # Calculate summary
        statuses = [r['status'] for r in status_records]
        if "OFFLINE" in statuses:
            overall_status = "OFFLINE"
        elif "DELAYED" in statuses:
            overall_status = "DELAYED"
        else:
            overall_status = "ACTIVE"

        print(f"\n{'='*60}")
        print(f"Overall Status: {overall_status}")
        print(f"  Total connections: {len(status_records)}")
        print(f"  ACTIVE: {statuses.count('ACTIVE')}")
        print(f"  DELAYED: {statuses.count('DELAYED')}")
        print(f"  OFFLINE: {statuses.count('OFFLINE')}")
        if errors:
            print(f"  Errors: {len(errors)}")
        print(f"{'='*60}")

        # Sleep for 15 minutes
        print(f"\n💤 Sleeping for 15 minutes until next update...")
        time.sleep(15 * 60)

    except Exception as e:
        print(f"\n❌ Error in metadata update loop: {str(e)}")
        print(traceback.format_exc())
        print("⏸ Sleeping for 15 minutes before retrying...")
