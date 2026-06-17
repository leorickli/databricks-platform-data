# Databricks notebook source
# DBTITLE 1,Shared Connection Status Utilities
"""
Shared utility functions for connection status processing.

These functions are extracted from the general b10_update_batch_connection_metadata notebook
and are used by connector-specific b10 notebooks via %run.

Usage in connector-specific notebooks:
    # MAGIC %run ../../../shared/connection_status_utils

All functions require `spark` to be passed explicitly so they work correctly
when %run'd from any notebook context.
"""

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, DoubleType, ArrayType
)

# COMMAND ----------

# DBTITLE 1,Metadata Table DDL

def ensure_metadata_tables(spark, catalog):
    """
    Creates connection_config and connection_status tables if they don't exist.
    Idempotent — safe to call on every pipeline run.

    Single source of truth for the batch metadata DDL: %run'd by the connector
    *_connection_config notebooks (so connection_config exists before their MERGE)
    and by b10_update_batch_connection_metadata. Streaming (b11) keeps its own
    schema — see that notebook.

    The `metadata` schema is Terraform-managed in lmx-infra; this
    helper never creates it.
    """
    config_table = f"{catalog}.metadata.connection_config"
    status_table = f"{catalog}.metadata.connection_status"

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {config_table} (
        connection_id STRING NOT NULL COMMENT 'Unique connection identifier (e.g., ampcore_<sensor>_history)',
        data_name STRING COMMENT 'Human-readable connection name',
        type STRING COMMENT 'Connection type (e.g., Electricity History, Battery SoC)',
        data_type STRING COMMENT 'Equipment type (Battery, Inverter, Charging Station, Electricity Meter)',
        protocol STRING COMMENT 'Communication protocol (HTTP, MQTT, Kafka)',
        location STRING COMMENT 'Physical location of the device',
        object_id STRING COMMENT 'Business object reference (e.g., ACME Bunnik ABCD)',
        client STRING COMMENT 'Client name (e.g., ACME)',
        brand STRING COMMENT 'Device manufacturer',
        connector STRING COMMENT 'Data source connector (ampcore_api, smartnode_api, etc.)',
        processing_type STRING COMMENT 'Batch, Real-Time, or Event-Driven',
        job_id STRING COMMENT 'Databricks job ID for this pipeline',
        job_name STRING COMMENT 'Databricks job name',
        gold_table_name STRING COMMENT 'Fully qualified gold table name (catalog.schema.table)',
        expected_interval_minutes INT COMMENT 'Expected data refresh interval in minutes',
        aggregation_level STRING COMMENT 'Data granularity: quarterly (15min), hourly, or daily',
        delayed_threshold_days INT COMMENT 'Days before connection marked DELAYED (default: 2 for batch)',
        offline_threshold_days INT COMMENT 'Days before connection marked OFFLINE (default: 4)',
        other_metadata STRING COMMENT 'JSON string for connector-specific metadata (building_uuid, ean, key, etc.)',
        created_at TIMESTAMP COMMENT 'Record creation timestamp',
        updated_at TIMESTAMP COMMENT 'Last update timestamp',
        CONSTRAINT connection_config_pk PRIMARY KEY (connection_id)
    )
    COMMENT 'Static configuration for data connections - defines expected behavior and metadata'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {status_table} (
        connection_id STRING NOT NULL COMMENT 'Foreign key to connection_config.connection_id',
        data_name STRING COMMENT 'Human-readable connection name',
        type STRING COMMENT 'Connection type',
        data_type STRING COMMENT 'Equipment type',
        protocol STRING COMMENT 'Communication protocol',
        location STRING COMMENT 'Physical location',
        object_id STRING COMMENT 'Business object reference',
        brand STRING COMMENT 'Device manufacturer',
        processing_type STRING COMMENT 'Batch, Real-Time, or Event-Driven',
        status STRING COMMENT 'Connection status: ACTIVE, DELAYED, OFFLINE',
        last_sync_timestamp TIMESTAMP COMMENT 'Timestamp of last successful data sync',
        job_run_id STRING COMMENT 'Databricks job run ID for this check',
        job_state STRING COMMENT 'Job run state (SUCCESS, FAILED, UNKNOWN)',
        hourly_metrics ARRAY<STRUCT<
            hour: TIMESTAMP,
            data_points: INT,
            expected_records: INT,
            completeness_pct: DOUBLE,
            duplicate_timestamps: INT,
            null_values: INT,
            avg_delay_minutes: DOUBLE,
            max_delay_minutes: DOUBLE,
            uptime_pct: DOUBLE,
            total_validation_errors: INT
        >> COMMENT 'Hourly metrics for the previous 24 hours',
        data_points_24h INT COMMENT 'Total data points in the last 24h',
        completeness_pct DOUBLE COMMENT 'Average completeness percentage for last 24h',
        uptime_pct_24h DOUBLE COMMENT 'Average uptime percentage for last 24h',
        avg_delay_minutes DOUBLE COMMENT 'Average ingestion delay in minutes for last 24h',
        total_validation_errors INT COMMENT 'Total validation errors in last 24h',
        duplicate_timestamps_24h INT COMMENT 'Total duplicate timestamps in last 24h',
        null_values_24h INT COMMENT 'Total null values in last 24h',
        data_gap_count INT COMMENT 'Number of distinct data gaps (contiguous runs of missing expected records) in last 24h — measures how missing data is distributed, complementing completeness_pct which measures how much is missing',
        data_gaps ARRAY<STRUCT<
            gap_start: TIMESTAMP,
            gap_end: TIMESTAMP,
            gap_duration_minutes: INT,
            missing_records: INT
        >> COMMENT 'One entry per detected gap; powers the frontend Connection Health Timeline gap segments. missing_data_pct is intentionally not stored — derive it as (100 - completeness_pct) on read',
        checked_at TIMESTAMP NOT NULL COMMENT 'When this status check was performed'
    )
    CLUSTER BY AUTO
    COMMENT 'Historical connection status tracking with quality metrics - appends a new record after each pipeline run'
    """)

    print(f"✓ Metadata tables ready:")
    print(f"  - {config_table}")
    print(f"  - {status_table}")

# COMMAND ----------

# DBTITLE 1,Job Run Info

def get_job_run_info(spark):
    """
    Retrieves Databricks job run context from Spark configuration.
    Returns (job_run_id, job_state) tuple.
    """
    try:
        job_id_val = spark.conf.get("spark.databricks.job.id", None)
        run_id_val = spark.conf.get("spark.databricks.job.runId", None)
        job_run_id = f"{job_id_val}_{run_id_val}" if job_id_val and run_id_val else "unknown"
        job_state = "SUCCESS"
        print(f"✓ Job run ID: {job_run_id}")
        print(f"  Job state: {job_state}")
        return job_run_id, job_state
    except Exception as e:
        print(f"⚠ Could not retrieve job context (running interactively?): {e}")
        return "interactive_run", "UNKNOWN"

# COMMAND ----------

# DBTITLE 1,Time Window

def calculate_time_window():
    """
    Calculates yesterday's date range in Amsterdam timezone.
    Returns (yesterday_start, today_start, yesterday_start_sql, today_start_sql).

    Uses Amsterdam timezone so daily meter readings timestamped at local midnight
    (22:00 or 23:00 UTC depending on DST) are captured correctly.
    """
    amsterdam = ZoneInfo("Europe/Amsterdam")
    now_ams = datetime.now(amsterdam)
    yesterday_start = (now_ams - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    today_start = now_ams.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start_sql = yesterday_start.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    today_start_sql = today_start.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    return yesterday_start, today_start, yesterday_start_sql, today_start_sql

# COMMAND ----------

# DBTITLE 1,Idempotency Check

def check_already_processed(spark, status_table, connection_id):
    """
    Checks if a connection has already been processed today.
    Returns True if a record exists for today, False otherwise.
    """
    today_date_str = datetime.now(timezone.utc).date().strftime('%Y-%m-%d')
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
            return True
        return False
    except Exception as e:
        print(f"  ⚠ Could not check for existing record: {e}")
        return False

# COMMAND ----------

# DBTITLE 1,Latest Record Timestamp

def get_latest_record_timestamp(spark, gold_table, ts_col, where_clause):
    """
    Queries the gold table for the most recent record timestamp.
    Returns the timestamp or None if the table is empty / doesn't exist.
    """
    query = f"""
        SELECT MAX({ts_col}) as latest_record_timestamp
        FROM {gold_table}
        {where_clause}
    """
    result = spark.sql(query).collect()[0]
    return result['latest_record_timestamp']

# COMMAND ----------

# DBTITLE 1,Hourly Metrics Calculation

def calculate_hourly_metrics(
    spark,
    gold_table,
    ts_col,
    where_clause,
    expected_interval,
    processing_ts_col,
    yesterday_start_sql,
    today_start_sql,
    yesterday_start=None
):
    """
    Calculates hourly (or daily) quality metrics for a gold table.

    Handles both granularities:
    - Daily (expected_interval_minutes >= 1440): groups by day, expects 1 record
    - Hourly (< 1440): groups by hour, calculates completeness per hour

    Returns (hourly_metrics, overall_completeness) where hourly_metrics is a
    list of tuples matching the hourly_metrics ARRAY<STRUCT> schema.
    """
    ean_filter = f"AND {where_clause.replace('WHERE', '').strip()}" if where_clause else ""

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

    hourly_metrics = []
    is_daily_granularity = expected_interval >= 1440

    if is_daily_granularity:
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

        expected_records_per_period = 1

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

            # Use the provided yesterday_start datetime or fall back to parsing from SQL string
            yesterday_start_dt = yesterday_start if yesterday_start else datetime.strptime(yesterday_start_sql, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

            hourly_metrics.append((
                yesterday_start_dt,
                int(data_points),
                expected_records_per_period,
                float(completeness_pct),
                int(duplicate_count),
                null_values,
                avg_delay,
                max_delay,
                float(uptime_pct),
                int(duplicate_count + null_values)
            ))

        overall_completeness = float(hourly_metrics[0][3]) if hourly_metrics else 0.0
        print(f"\n✓ Daily metrics calculated:")
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

            completeness_pct_hour = round((data_points / expected_records_per_hour) * 100, 2) if expected_records_per_hour > 0 else 0
            completeness_pct_hour = min(100.0, completeness_pct_hour)

            uptime_pct_hour = 100.0 if data_points > 0 else 0.0
            duplicate_count = data_points - unique_timestamps
            null_values = 0
            total_validation_errors = duplicate_count + null_values

            avg_delay = float(row['avg_delay_minutes']) if has_processing_ts and row['avg_delay_minutes'] is not None else None
            max_delay = float(row['max_delay_minutes']) if has_processing_ts and row['max_delay_minutes'] is not None else None

            hourly_metrics.append((
                hour_start,
                int(data_points),
                expected_records_per_hour,
                float(completeness_pct_hour),
                int(duplicate_count),
                null_values,
                avg_delay,
                max_delay,
                float(uptime_pct_hour),
                int(total_validation_errors)
            ))

        overall_completeness = sum(h[3] for h in hourly_metrics) / len(hourly_metrics) if hourly_metrics else 0

        print(f"\n✓ Hourly metrics calculated:")
        print(f"  Hours processed: {len(hourly_metrics)}")
        print(f"  Expected records per hour: {expected_records_per_hour}")
        print(f"  Overall completeness: {overall_completeness:.2f}%")

    return hourly_metrics, overall_completeness

# COMMAND ----------

# DBTITLE 1,Data Gap Detection

def calculate_data_gaps(
    spark,
    gold_table,
    ts_col,
    where_clause,
    expected_interval,
    window_start_sql,
    window_end_sql,
    gap_threshold_factor=1.5,
):
    """
    Detects data gaps — contiguous runs of *missing expected records* — within a
    time window, by inspecting the spacing between consecutive observed records.

    This is the metric behind subtask 3.4's timeline gap segments. It is
    orthogonal to completeness_pct:
      - completeness_pct measures *how much* data is missing (magnitude).
      - data_gap_count / data_gaps measure *how it is distributed* (one long
        outage vs. many scattered single drops). 8 missing records can be one
        gap (a 2h outage) or eight gaps (a flaky link) — same completeness,
        very different operational meaning.

    A gap is flagged whenever the spacing between two consecutive records exceeds
    `expected_interval * gap_threshold_factor` minutes (i.e. at least one full
    expected record is missing, with tolerance for jitter / late arrivals). The
    window boundaries are included as anchors, so a connection that goes quiet
    at the start or end of the window — or for the whole window — is also caught;
    this gives the frontend timeline full left-to-right coverage.

    Gap-between-records detection only makes sense for sub-daily connections; for
    daily granularity (expected_interval >= 1440) it returns (0, []) and the
    timeline falls back to per-day completeness.

    Returns (data_gap_count, gaps) where gaps is a list of tuples matching the
    data_gaps ARRAY<STRUCT> schema:
        (gap_start: TIMESTAMP, gap_end: TIMESTAMP,
         gap_duration_minutes: INT, missing_records: INT)
    """
    if expected_interval is None or expected_interval <= 0 or expected_interval >= 1440:
        return 0, []

    ean_filter = f"AND {where_clause.replace('WHERE', '').strip()}" if where_clause else ""

    # Anchor on the window boundaries so leading / trailing / full-window silence
    # is detected, not just gaps strictly between two observed records.
    gaps_query = f"""
        WITH records AS (
            SELECT DISTINCT {ts_col} AS ts
            FROM {gold_table}
            WHERE {ts_col} >= '{window_start_sql}' AND {ts_col} < '{window_end_sql}'
            {ean_filter}
        ),
        anchored AS (
            SELECT ts FROM records
            UNION
            SELECT TIMESTAMP('{window_start_sql}') AS ts
            UNION
            SELECT TIMESTAMP('{window_end_sql}') AS ts
        ),
        spaced AS (
            SELECT ts, LAG(ts) OVER (ORDER BY ts) AS prev_ts
            FROM anchored
        )
        SELECT
            prev_ts AS gap_start,
            ts      AS gap_end,
            (UNIX_TIMESTAMP(ts) - UNIX_TIMESTAMP(prev_ts)) / 60.0 AS gap_minutes
        FROM spaced
        WHERE prev_ts IS NOT NULL
          AND (UNIX_TIMESTAMP(ts) - UNIX_TIMESTAMP(prev_ts)) / 60.0 > {expected_interval} * {gap_threshold_factor}
        ORDER BY gap_start ASC
    """

    rows = spark.sql(gaps_query).collect()

    gaps = []
    for row in rows:
        gap_minutes = float(row['gap_minutes'])
        # Expected records that should have landed inside the gap but didn't.
        missing_records = max(0, int(round(gap_minutes / expected_interval)) - 1)
        gaps.append((
            row['gap_start'],
            row['gap_end'],
            int(round(gap_minutes)),
            missing_records,
        ))

    return len(gaps), gaps

# COMMAND ----------

# DBTITLE 1,Status Determination

def determine_status(
    minutes_since_last_record,
    overall_completeness,
    hourly_metrics,
    job_state,
    delayed_threshold_days,
    offline_threshold_days
):
    """
    Determines connection status (ACTIVE, DELAYED, OFFLINE) based on data freshness and quality.

    Priority order:
    1. OFFLINE: job failed, no data, or data older than offline_threshold_days
    2. DELAYED: data is stale (> delayed_threshold_days) or completeness < 80%
    3. ACTIVE: data is fresh and complete
    """
    delayed_threshold_minutes = delayed_threshold_days * 24 * 60
    offline_threshold_minutes = offline_threshold_days * 24 * 60

    if job_state == "FAILED":
        return "OFFLINE"
    elif minutes_since_last_record is None:
        return "OFFLINE"
    elif minutes_since_last_record > offline_threshold_minutes:
        return "OFFLINE"
    elif minutes_since_last_record > delayed_threshold_minutes:
        return "DELAYED"
    elif hourly_metrics and overall_completeness < 80:
        return "DELAYED"
    else:
        return "ACTIVE"

# COMMAND ----------

# DBTITLE 1,24h Summary

def compute_24h_summary(hourly_metrics):
    """
    Pre-computes 24h summary fields from the hourly_metrics list for fast frontend queries.
    Returns a dict with data_points_24h, completeness_pct, uptime_pct_24h, avg_delay_minutes,
    total_validation_errors, duplicate_timestamps_24h, null_values_24h.
    All values are None if hourly_metrics is empty.
    """
    if not hourly_metrics:
        return {
            "data_points_24h": None,
            "completeness_pct": None,
            "uptime_pct_24h": None,
            "avg_delay_minutes": None,
            "total_validation_errors": None,
            "duplicate_timestamps_24h": None,
            "null_values_24h": None,
        }

    valid_delays = [h[6] for h in hourly_metrics if h[6] is not None]
    return {
        "data_points_24h": sum(h[1] for h in hourly_metrics),
        "completeness_pct": round(sum(h[3] for h in hourly_metrics) / len(hourly_metrics), 1),
        "uptime_pct_24h": round(sum(h[8] for h in hourly_metrics) / len(hourly_metrics), 1),
        "avg_delay_minutes": round(sum(valid_delays) / len(valid_delays), 1) if valid_delays else None,
        "total_validation_errors": sum(h[9] for h in hourly_metrics),
        "duplicate_timestamps_24h": sum(h[4] for h in hourly_metrics),
        "null_values_24h": sum(h[5] for h in hourly_metrics),
    }

# COMMAND ----------

# DBTITLE 1,Status Record Builders

def build_status_record(connection_id, config, status, latest_record_timestamp, hourly_metrics, summary, job_run_id, job_state):
    """
    Assembles a status record dict ready for DataFrame creation.
    """
    return {
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
        "data_points_24h": summary["data_points_24h"],
        "completeness_pct": summary["completeness_pct"],
        "uptime_pct_24h": summary["uptime_pct_24h"],
        "avg_delay_minutes": summary["avg_delay_minutes"],
        "total_validation_errors": summary["total_validation_errors"],
        "duplicate_timestamps_24h": summary["duplicate_timestamps_24h"],
        "null_values_24h": summary["null_values_24h"],
        "checked_at": datetime.now(timezone.utc),
    }


def build_error_status_record(connection_id, config, job_run_id):
    """
    Assembles a fallback OFFLINE status record when processing fails unexpectedly.
    """
    return {
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
        "checked_at": datetime.now(timezone.utc),
    }

# COMMAND ----------

# DBTITLE 1,DataFrame Schemas

def get_hourly_metrics_struct():
    """Returns the StructType for the hourly_metrics ARRAY<STRUCT> field."""
    return StructType([
        StructField("hour", TimestampType(), True),
        StructField("data_points", IntegerType(), True),
        StructField("expected_records", IntegerType(), True),
        StructField("completeness_pct", DoubleType(), True),
        StructField("duplicate_timestamps", IntegerType(), True),
        StructField("null_values", IntegerType(), True),
        StructField("avg_delay_minutes", DoubleType(), True),
        StructField("max_delay_minutes", DoubleType(), True),
        StructField("uptime_pct", DoubleType(), True),
        StructField("total_validation_errors", IntegerType(), True),
    ])


def get_status_schema():
    """Returns the StructType for the connection_status DataFrame."""
    return StructType([
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
        StructField("hourly_metrics", ArrayType(get_hourly_metrics_struct()), True),
        StructField("data_points_24h", IntegerType(), True),
        StructField("completeness_pct", DoubleType(), True),
        StructField("uptime_pct_24h", DoubleType(), True),
        StructField("avg_delay_minutes", DoubleType(), True),
        StructField("total_validation_errors", IntegerType(), True),
        StructField("duplicate_timestamps_24h", IntegerType(), True),
        StructField("null_values_24h", IntegerType(), True),
        StructField("checked_at", TimestampType(), False),
    ])

# COMMAND ----------

# DBTITLE 1,Write Status Records

def write_status_records(spark, status_records, status_table):
    """
    Creates a DataFrame from status_records and appends it to the connection_status table.
    Uses mergeSchema=true to allow safe addition of new fields to hourly_metrics ARRAY<STRUCT>.
    """
    status_df = spark.createDataFrame(status_records, schema=get_status_schema())
    status_df.write.mode("append").option("mergeSchema", "true").saveAsTable(status_table)
    print(f"\n✓ Successfully appended {len(status_records)} connection status record(s) to {status_table}")
