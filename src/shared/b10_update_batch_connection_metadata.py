# Databricks notebook source
# DBTITLE 1,Update Connection Metadata with Quality Metrics (Multi-Connection Support)
"""
This notebook updates the connection status metadata and calculates quality metrics after a pipeline completes.

It supports both single and multiple connections per pipeline:
- Single connection mode: Pass connection_id parameter (e.g., a single smartnode asset)
- Multi-connection mode: Pass connector parameter (e.g., ampcore_api for all AMPCORE devices)

Usage:
1. Single connection (smartnode):
   - connection_id: "smartnode_<account>_<asset>"

2. Multiple connections (ampcore):
   - connector: "ampcore_api"
   - Will update all AMPCORE devices registered in metadata.connection_config

Process:
1. Reads connection config(s) from metadata.connection_config
2. Gets current job run information from Databricks context
3. Queries each gold table for latest data timestamp and row counts
4. Calculates connection status (ACTIVE, DELAYED, OFFLINE)
5. Calculates hourly quality metrics (hourly_metrics ARRAY<STRUCT>):
   - Per-hour: data_points, expected_records, completeness_pct, duplicate_timestamps,
     null_values, avg_delay_minutes, max_delay_minutes, uptime_pct
6. Appends historical record to metadata.connection_status table

Status Logic:
- OFFLINE: No data for > 7 days OR job failed
- DELAYED: Data is 2x+ expected interval but < 7 days old
- ACTIVE: Data is within 2x expected interval

This runs as the final task (b06) in batch pipelines. 
"""

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, IntegerType, DecimalType, DoubleType, ArrayType
import traceback
import json

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.text("catalog_name", "acme_dev", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

# Option 1: Single connection mode (for a single smartnode asset)
dbutils.widgets.text("connection_id", "", "Single Connection ID (leave empty to use connector)")
CONNECTION_ID = dbutils.widgets.get("connection_id")

# Option 2: Multi-connection mode (for ampcore, smartnode)
dbutils.widgets.text("connector", "", "Connector name (e.g., ampcore_api, smartnode_api)")
CONNECTOR = dbutils.widgets.get("connector")

# Validate parameters
if not CONNECTION_ID and not CONNECTOR:
    raise ValueError("Must provide either connection_id OR connector parameter")

if CONNECTION_ID and CONNECTOR:
    print(f"⚠ Both connection_id and connector provided. Using connection_id: {CONNECTION_ID}")
    CONNECTOR = None

# COMMAND ----------

# MAGIC %run ./connection_status_utils

# COMMAND ----------

# DBTITLE 1,Create Metadata Tables
config_table = f"{CATALOG_NAME}.metadata.connection_config"
status_table = f"{CATALOG_NAME}.metadata.connection_status"

# connection_config + connection_status DDL lives in one place — the shared
# ensure_metadata_tables() helper (%run'd above). Idempotent, safe every run.
ensure_metadata_tables(spark, CATALOG_NAME)

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
    # dbutils context works on both classic and serverless (Spark Connect).
    # spark.conf.get("spark.databricks.job.id") is blocked on serverless.
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    job_id_val = ctx.tags().get("jobId").getOrElse(None)
    run_id_val = ctx.tags().get("jobRunId").getOrElse(None)

    job_run_id = f"{job_id_val}_{run_id_val}" if job_id_val and run_id_val else "unknown"
    job_id = job_id_val
    job_state = "SUCCESS"

    print(f"✓ Job run ID: {job_run_id}")
    print(f"  Job ID: {job_id}")
    print(f"  Job state: {job_state}")

except Exception as e:
    print(f"⚠ Could not retrieve job context (might be running interactively): {e}")
    job_run_id = "interactive_run"
    job_id = None
    job_state = "UNKNOWN"

# COMMAND ----------

# DBTITLE 1,Process Each Connection
status_records = []
errors = []

for config in configs:
    connection_id = config['connection_id']
    gold_table = config['gold_table_name']
    expected_interval = config['expected_interval_minutes']

    print(f"\n{'='*60}")
    print(f"Processing: {connection_id} ({config['data_name']})")
    print(f"{'='*60}")

    # Extract filter clause and timestamp column from other_metadata
    # Supports multiple connectors:
    #   - Strategy A: filter by ean + key
    #   - Strategy B: filter by building_uuid (custom timestamp column)
    #   - Others: no filter, uses 'timestamp' column (default)
    where_clause = ""
    ts_col = "timestamp"  # Default timestamp column
    processing_ts_col = "gold_processing_timestamp"  # Default processing timestamp column
    other_metadata = config['other_metadata'] if 'other_metadata' in config.__fields__ else None
    if other_metadata:
        try:
            metadata_dict = json.loads(other_metadata)

            # Read custom timestamp column if specified (when the gold table uses a non-default name)
            ts_col = metadata_dict.get('timestamp_col', ts_col)

            # Read custom processing timestamp column if specified
            processing_ts_col = metadata_dict.get('processing_timestamp_col', processing_ts_col)

            # Strategy A: filter by EAN + key
            ean = metadata_dict.get('ean')
            key = metadata_dict.get('key')
            if ean and key:
                where_clause = f"WHERE ean = '{ean}' AND key = '{key}'"
                print(f"  → Filtering by EAN: {ean}, key: {key}")
            elif ean:
                where_clause = f"WHERE ean = '{ean}'"
                print(f"  → Filtering by EAN: {ean}")

            # Strategy B: filter by building_uuid
            building_uuid = metadata_dict.get('building_uuid')
            if building_uuid and not ean:
                where_clause = f"WHERE building_uuid = '{building_uuid}'"
                print(f"  → Filtering by building_uuid: {building_uuid}")

            # Generic: honor a pre-built filter_clause for connectors like AMPCORE.
            # Takes precedence only when neither ean nor building_uuid matched above.
            filter_clause_raw = metadata_dict.get('filter_clause')
            if filter_clause_raw and not ean and not building_uuid:
                where_clause = f"WHERE {filter_clause_raw}"
                print(f"  → Filtering by filter_clause: {filter_clause_raw}")

        except Exception as e:
            print(f"  ⚠ Could not parse other_metadata JSON: {e}")

    print(f"  → Timestamp column: {ts_col}")

    # Check if processing timestamp column exists in the gold table (schema evolution safe)
    has_processing_ts = False

    # Calculate yesterday's date range using Amsterdam time so that daily meter
    # readings timestamped at local midnight (= 22:00 or 23:00 UTC depending on
    # DST) are captured correctly instead of falling outside the UTC day boundary.
    amsterdam = ZoneInfo("Europe/Amsterdam")
    now_ams = datetime.now(amsterdam)
    yesterday_start = (now_ams - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    today_start = now_ams.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start_sql = yesterday_start.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    today_start_sql = today_start.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    # Check if we already processed this connection today (idempotency check)
    # Use explicit date string to avoid timezone issues with CURRENT_DATE()
    today_date = datetime.now(timezone.utc).date()
    today_date_str = today_date.strftime('%Y-%m-%d')

    check_query = f"""
    SELECT COUNT(*) as count
    FROM {status_table}
    WHERE connection_id = '{connection_id}'
      AND DATE(checked_at) = DATE('{today_date_str}')
    """

    try:
        existing_count = spark.sql(check_query).collect()[0]['count']
        if existing_count > 0:
            print(f"⚠ Skipping {connection_id} - already processed today ({today_date_str})")
            continue
    except Exception as e:
        print(f"  ⚠ Could not check for existing record: {e}")
        # Continue anyway - first run or table doesn't exist yet

    try:
        # Check if gold table exists
        if not spark.catalog.tableExists(gold_table):
            print(f"⚠ Gold table {gold_table} does not exist")
            status = "OFFLINE"
            latest_record_timestamp = None
            hourly_metrics = []
            data_gap_count = None
            data_gaps = []
        else:
            # Get latest record timestamp
            latest_record_query = f"""
                SELECT MAX({ts_col}) as latest_record_timestamp
                FROM {gold_table}
                {where_clause}
            """
            latest_record_result = spark.sql(latest_record_query).collect()[0]
            latest_record_timestamp = latest_record_result['latest_record_timestamp']

            print(f"✓ Gold table exists")
            print(f"  Latest record: {latest_record_timestamp}")

            # Single query to get all hourly metrics for yesterday
            # Apply connection filter if present (EAN+key or building_uuid, per connection_config)
            ean_filter = f"AND {where_clause.replace('WHERE', '')}" if where_clause else ""

            # Get table columns to check for value fields and processing timestamp availability
            table_columns = [field.name for field in spark.table(gold_table).schema.fields]
            has_processing_ts = processing_ts_col in table_columns
            if has_processing_ts:
                print(f"  → Processing timestamp column '{processing_ts_col}' found — delay metrics enabled")
                delay_select = f"""
                    ,AVG((UNIX_TIMESTAMP({processing_ts_col}) - UNIX_TIMESTAMP({ts_col})) / 60.0) as avg_delay_minutes
                    ,MAX((UNIX_TIMESTAMP({processing_ts_col}) - UNIX_TIMESTAMP({ts_col})) / 60.0) as max_delay_minutes
                """
            else:
                print(f"  ⚠ Processing timestamp column '{processing_ts_col}' not found — delay metrics unavailable")
                delay_select = ""

            # Build hourly metrics array from aggregated results
            hourly_metrics = []

            is_daily_granularity = expected_interval >= 1440

            if is_daily_granularity:
                # Daily connections (e.g. meter readings): group by day instead of hour.
                # expected_records_per_hour would be int(60 // 1440) = 0, making hourly
                # completeness always 0%. Instead we check: did 1 record arrive yesterday?
                print(f"  → Daily granularity detected (expectedInterval={expected_interval}min) — using daily completeness check")

                daily_metrics_query = f"""
                    SELECT
                        DATE_TRUNC('day', {ts_col}) as hour,
                        COUNT(*) as data_points,
                        COUNT(DISTINCT {ts_col}) as unique_timestamps
                        {delay_select}
                    FROM {gold_table}
                    WHERE {ts_col} >= '{yesterday_start_sql}' AND {ts_col} < '{today_start_sql}'
                    {ean_filter}
                    GROUP BY DATE_TRUNC('day', {ts_col})
                    ORDER BY hour ASC
                """

                daily_results = spark.sql(daily_metrics_query).collect()
                print(f"✓ Retrieved daily metrics for yesterday ({len(daily_results)} day(s))")

                expected_records_per_period = 1  # 1 record expected per day

                if daily_results:
                    row = daily_results[0]
                    data_points = row['data_points']
                    unique_timestamps = row['unique_timestamps']
                    completeness_pct = min(100.0, round((data_points / expected_records_per_period) * 100, 2))
                    uptime_pct = 100.0 if data_points > 0 else 0.0
                    duplicate_count = data_points - unique_timestamps
                    null_values = 0
                    avg_delay = float(row['avg_delay_minutes']) if has_processing_ts and row['avg_delay_minutes'] is not None else None
                    max_delay = float(row['max_delay_minutes']) if has_processing_ts and row['max_delay_minutes'] is not None else None

                    hourly_metrics.append((
                        yesterday_start,             # hour: TIMESTAMP (day boundary)
                        int(data_points),            # data_points: INT
                        expected_records_per_period, # expected_records: INT
                        float(completeness_pct),     # completeness_pct: DOUBLE
                        int(duplicate_count),        # duplicate_timestamps: INT
                        null_values,                 # null_values: INT
                        avg_delay,                   # avg_delay_minutes: DOUBLE
                        max_delay,                   # max_delay_minutes: DOUBLE
                        float(uptime_pct),           # uptime_pct: DOUBLE
                        int(duplicate_count + null_values)  # total_validation_errors: INT
                    ))

                overall_completeness = float(hourly_metrics[0][3]) if hourly_metrics else 0.0

                print("\n✓ Daily metrics calculated:")
                print(f"  Expected records per day: {expected_records_per_period}")
                print(f"  Overall completeness: {overall_completeness:.2f}%")

            else:
                hourly_metrics_query = f"""
                    SELECT
                        DATE_TRUNC('hour', {ts_col}) as hour,
                        COUNT(*) as data_points,
                        COUNT(DISTINCT {ts_col}) as unique_timestamps
                        {delay_select}
                    FROM {gold_table}
                    WHERE {ts_col} >= '{yesterday_start_sql}' AND {ts_col} < '{today_start_sql}'
                    {ean_filter}
                    GROUP BY DATE_TRUNC('hour', {ts_col})
                    ORDER BY hour ASC
                """

                hourly_results = spark.sql(hourly_metrics_query).collect()
                print(f"✓ Retrieved hourly metrics for yesterday ({len(hourly_results)} hours)")

                expected_records_per_hour = int(60 // expected_interval)

                for row in hourly_results:
                    hour_start = row['hour']
                    data_points = row['data_points']
                    unique_timestamps = row['unique_timestamps']

                    # Calculate completeness for this hour (data volume metric)
                    completeness_pct_hour = round((data_points / expected_records_per_hour) * 100, 2) if expected_records_per_hour > 0 else 0
                    completeness_pct_hour = min(100.0, completeness_pct_hour)  # Cap at 100%

                    # Calculate uptime for this hour (connection availability metric)
                    # Uptime = binary indicator of whether gateway/logger is connected
                    # 100% = gateway is alive and transmitting data
                    # 0% = gateway is offline or disconnected
                    if data_points > 0:
                        uptime_pct_hour = 100.0  # Connection is up
                    else:
                        uptime_pct_hour = 0.0    # Connection is down

                    # Detect duplicates
                    duplicate_count = data_points - unique_timestamps
                    null_values = 0  # Placeholder — not tracked at gold layer currently

                    # Total validation errors = all data quality issues combined
                    total_validation_errors = duplicate_count + null_values

                    # Ingestion delay (only available when gold_processing_timestamp exists in gold table)
                    avg_delay = float(row['avg_delay_minutes']) if has_processing_ts and row['avg_delay_minutes'] is not None else None
                    max_delay = float(row['max_delay_minutes']) if has_processing_ts and row['max_delay_minutes'] is not None else None

                    # Create native tuple for Spark struct (not dict)
                    hourly_metrics.append((
                        hour_start,              # hour: TIMESTAMP
                        int(data_points),        # data_points: INT
                        expected_records_per_hour,  # expected_records: INT
                        float(completeness_pct_hour),  # completeness_pct: DOUBLE (0-100%, proportional)
                        int(duplicate_count),    # duplicate_timestamps: INT
                        null_values,             # null_values: INT
                        avg_delay,               # avg_delay_minutes: DOUBLE
                        max_delay,               # max_delay_minutes: DOUBLE
                        float(uptime_pct_hour),  # uptime_pct: DOUBLE (100% or 0%, binary)
                        int(total_validation_errors)  # total_validation_errors: INT
                    ))

                # Calculate overall completeness from hourly metrics
                # Note: hourly_metrics is now a list of tuples, completeness_pct is at index 3
                if hourly_metrics:
                    overall_completeness = sum(h[3] for h in hourly_metrics) / len(hourly_metrics)
                else:
                    overall_completeness = 0

                print("\n✓ Hourly metrics calculated:")
                print(f"  Hours processed: {len(hourly_metrics)}")
                print(f"  Expected records per hour: {expected_records_per_hour}")
                print(f"  Overall completeness: {overall_completeness:.2f}%")

            # Detect data gaps — contiguous runs of missing expected records — for
            # the frontend timeline (subtask 3.4). Orthogonal to completeness_pct:
            # completeness = how much is missing; data_gap_count = how it's spread.
            # Daily-granularity connections return (0, []) inside the helper.
            data_gap_count, data_gaps = calculate_data_gaps(
                spark,
                gold_table,
                ts_col,
                where_clause,
                expected_interval,
                yesterday_start_sql,
                today_start_sql,
            )
            print(f"\n✓ Data gaps detected: {data_gap_count}")
            for g in data_gaps:
                print(f"  • {g[0]} → {g[1]} ({g[2]} min, ~{g[3]} missing records)")

        # Calculate minutes since last record (outside table check)
        current_time = datetime.now(timezone.utc)

        if latest_record_timestamp:
            # Ensure latest_record_timestamp is timezone-aware
            if latest_record_timestamp.tzinfo is None:
                latest_record_timestamp = latest_record_timestamp.replace(tzinfo=timezone.utc)

            minutes_since_last_record = (current_time - latest_record_timestamp).total_seconds() / 60
        else:
            minutes_since_last_record = None

        # Determine status based on overall completeness and last record time
        # Read configurable thresholds from config (with defaults)
        # Note: config is a PySpark Row object, not a dict, so we use try-except for field access
        try:
            delayed_threshold_days = config['delayed_threshold_days'] if config['delayed_threshold_days'] is not None else 2
        except (KeyError, TypeError):
            delayed_threshold_days = 2  # Default: 2 days for batch

        try:
            offline_threshold_days = config['offline_threshold_days'] if config['offline_threshold_days'] is not None else 4
        except (KeyError, TypeError):
            offline_threshold_days = 4  # Default: 4 days

        # Convert to minutes
        delayed_threshold_minutes = delayed_threshold_days * 24 * 60
        offline_threshold_minutes = offline_threshold_days * 24 * 60

        if job_state == "FAILED":
            status = "OFFLINE"
        elif minutes_since_last_record is None:
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
        print(f"  Minutes since last record: {minutes_since_last_record:.1f}" if minutes_since_last_record else "  Minutes since last record: N/A")
        print(f"  Threshold for DELAYED: {delayed_threshold_minutes} minutes ({delayed_threshold_minutes / 60:.1f} hours, {delayed_threshold_days} days)")
        print(f"  Threshold for OFFLINE: {offline_threshold_minutes} minutes ({offline_threshold_minutes / 60 / 24:.1f} days)")

        # Pre-compute 24h summary fields from hourly_metrics for fast frontend queries
        if hourly_metrics:
            data_points_24h = sum(h[1] for h in hourly_metrics)
            completeness_pct = round(sum(h[3] for h in hourly_metrics) / len(hourly_metrics), 1)
            uptime_pct_24h = round(sum(h[8] for h in hourly_metrics) / len(hourly_metrics), 1)
            valid_delays = [h[6] for h in hourly_metrics if h[6] is not None]
            avg_delay_minutes = round(sum(valid_delays) / len(valid_delays), 1) if valid_delays else None
            total_validation_errors = sum(h[9] for h in hourly_metrics)
            duplicate_timestamps_24h = sum(h[4] for h in hourly_metrics)
            null_values_24h = sum(h[5] for h in hourly_metrics)
        else:
            data_points_24h = None
            completeness_pct = None
            uptime_pct_24h = None
            avg_delay_minutes = None
            total_validation_errors = None
            duplicate_timestamps_24h = None
            null_values_24h = None

        # Create status record with hourly_metrics
        status_record = {
            "connection_id": connection_id,

            # Static fields (from config)
            "data_name": config['data_name'],
            "type": config['type'],
            "data_type": config['data_type'],
            "protocol": config['protocol'],
            "location": config['location'],
            "object_id": config['object_id'],
            "brand": config['brand'],
            "processing_type": config['processing_type'],

            # Dynamic status
            "status": status,
            "last_sync_timestamp": latest_record_timestamp,

            # Job metadata
            "job_run_id": job_run_id,
            "job_state": job_state,

            # Hourly metrics as native array
            "hourly_metrics": hourly_metrics if hourly_metrics else None,

            # Pre-computed 24h summary
            "data_points_24h": data_points_24h,
            "completeness_pct": completeness_pct,
            "uptime_pct_24h": uptime_pct_24h,
            "avg_delay_minutes": avg_delay_minutes,
            "total_validation_errors": total_validation_errors,
            "duplicate_timestamps_24h": duplicate_timestamps_24h,
            "null_values_24h": null_values_24h,

            # Data gaps (subtask 3.4) — count + per-gap boundaries for the timeline
            "data_gap_count": data_gap_count,
            "data_gaps": data_gaps if data_gaps else None,

            # Audit (snapshot timestamp)
            "checked_at": datetime.now(timezone.utc)
        }

        status_records.append(status_record)
        print(f"✓ Status record prepared for {connection_id}")

    except Exception as e:
        error_msg = f"Error processing {connection_id}: {str(e)}"
        print(f"✗ {error_msg}")
        print(traceback.format_exc())
        errors.append(error_msg)

        # Still create a record with OFFLINE status
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
            "data_points_24h": None,
            "completeness_pct": None,
            "uptime_pct_24h": None,
            "avg_delay_minutes": None,
            "total_validation_errors": None,
            "duplicate_timestamps_24h": None,
            "null_values_24h": None,
            "data_gap_count": None,
            "data_gaps": None,
            "checked_at": datetime.now(timezone.utc)
        }
        status_records.append(status_record)

print(f"\n{'='*60}")
print(f"✓ Processed {len(status_records)} connection(s)")
if errors:
    print(f"⚠ Encountered {len(errors)} error(s)")

# COMMAND ----------

# DBTITLE 1,Append Status Records to connection_status Table (Historical Tracking)
# Define hourly metrics struct schema
hourly_metrics_struct = StructType([
    StructField("hour", TimestampType(), True),
    StructField("data_points", IntegerType(), True),
    StructField("expected_records", IntegerType(), True),
    StructField("completeness_pct", DoubleType(), True),
    StructField("duplicate_timestamps", IntegerType(), True),
    StructField("null_values", IntegerType(), True),
    StructField("avg_delay_minutes", DoubleType(), True),
    StructField("max_delay_minutes", DoubleType(), True),
    StructField("uptime_pct", DoubleType(), True),
    StructField("total_validation_errors", IntegerType(), True)
])

# Per-gap struct for data_gaps ARRAY<STRUCT> (subtask 3.4 timeline segments)
data_gaps_struct = StructType([
    StructField("gap_start", TimestampType(), True),
    StructField("gap_end", TimestampType(), True),
    StructField("gap_duration_minutes", IntegerType(), True),
    StructField("missing_records", IntegerType(), True)
])

# Create DataFrame with explicit schema to avoid type inference issues
status_schema = StructType([
    # Connection identifiers
    StructField("connection_id", StringType(), False),

    # Static fields
    StructField("data_name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("location", StringType(), True),
    StructField("object_id", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("processing_type", StringType(), True),

    # Dynamic status
    StructField("status", StringType(), True),
    StructField("last_sync_timestamp", TimestampType(), True),

    # Job metadata
    StructField("job_run_id", StringType(), True),
    StructField("job_state", StringType(), True),

    # Hourly metrics as native ARRAY<STRUCT>
    StructField("hourly_metrics", ArrayType(hourly_metrics_struct), True),

    # Pre-computed 24h summary
    StructField("data_points_24h", IntegerType(), True),
    StructField("completeness_pct", DoubleType(), True),
    StructField("uptime_pct_24h", DoubleType(), True),
    StructField("avg_delay_minutes", DoubleType(), True),
    StructField("total_validation_errors", IntegerType(), True),
    StructField("duplicate_timestamps_24h", IntegerType(), True),
    StructField("null_values_24h", IntegerType(), True),

    # Data gaps (subtask 3.4)
    StructField("data_gap_count", IntegerType(), True),
    StructField("data_gaps", ArrayType(data_gaps_struct), True),

    # Audit timestamp
    StructField("checked_at", TimestampType(), False)
])

status_df = spark.createDataFrame(status_records, schema=status_schema)

# Append records to the table (historical tracking)
# mergeSchema=true allows adding new fields to the hourly_metrics ARRAY<STRUCT> via schema evolution
status_df.write.mode("append").option("mergeSchema", "true").saveAsTable(status_table)

print(f"\n✓ Successfully appended {len(status_records)} connection status record(s) to {status_table}")
