# Databricks notebook source
# DBTITLE 1,Pipeline Failure Status Handler
"""
This notebook creates connection status records with "DELAYED" status when the main pipeline fails.

It is designed to run with `run_if: AT_LEAST_ONE_FAILED` condition, meaning it only executes
when upstream tasks (b01_api2land through b05) have failed.

Purpose:
- Ensures the frontend always has fresh connection status data
- Creates "DELAYED" status for all ACTIVE connections when API ingestion fails
- Distinguishes between active meters (get DELAYED) and inactive meters (skipped)
- Provides visibility into pipeline health even during outages

Parameters:
    - catalog_name: Client catalog name (e.g., 'acme_dev')
    - connector: Connector name (e.g., 'wattflow_api')

Key Logic:
- Only processes meters where meterStatus = 'active' (from other_metadata JSON)
- Inactive meters are intentionally skipped to avoid misleading status updates
- Creates one DELAYED record per active connection per day (idempotent)
"""

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import datetime, timezone, date
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, DoubleType, ArrayType
)
import traceback

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.text("catalog_name", "acme_dev", "Catalog Name")
dbutils.widgets.text("connector", "wattflow_api", "Connector name")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
CONNECTOR = dbutils.widgets.get("connector")

config_table = f"{CATALOG_NAME}.metadata.connection_config"
status_table = f"{CATALOG_NAME}.metadata.connection_status"

print(f"{'='*60}")
print(f"PIPELINE FAILURE STATUS HANDLER")
print(f"{'='*60}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Connector: {CONNECTOR}")
print(f"This task runs because upstream pipeline tasks FAILED")
print(f"{'='*60}\n")

# COMMAND ----------

# DBTITLE 1,Get Job Run Information
try:
    job_id_val = spark.conf.get("spark.databricks.job.id", None)
    run_id_val = spark.conf.get("spark.databricks.job.runId", None)
    job_run_id = f"{job_id_val}_{run_id_val}" if job_id_val and run_id_val else "unknown"

    # Mark as FAILED since this task only runs when upstream failed
    job_state = "FAILED"

    print(f"Job run ID: {job_run_id}")
    print(f"Job state: {job_state} (upstream tasks failed)")
except Exception as e:
    print(f"Could not retrieve job context: {e}")
    job_run_id = "unknown"
    job_state = "FAILED"

# COMMAND ----------

# DBTITLE 1,Read Connection Configs and Filter for Active Meters Only
# Get all connections for this connector
# Extract meterStatus from other_metadata JSON field and filter for active meters only
configs_df = spark.sql(f"""
    SELECT *,
           get_json_object(other_metadata, '$.meterStatus') as meterStatus
    FROM {config_table}
    WHERE connector = '{CONNECTOR}'
""")

# Filter to only ACTIVE meters - inactive meters are skipped
active_configs_df = configs_df.filter(F.col("meterStatus") == "active")

configs = active_configs_df.collect()

total_configs = configs_df.count()
active_count = len(configs)
inactive_count = total_configs - active_count

print(f"Total connection configs for connector '{CONNECTOR}': {total_configs}")
print(f"  - Active meters: {active_count}")
print(f"  - Inactive meters: {inactive_count} (will be skipped)")

if active_count == 0:
    print(f"\nNo active connection configs found for connector '{CONNECTOR}'")
    print("Nothing to do - exiting")
    dbutils.notebook.exit("NO_ACTIVE_CONFIGS")

print(f"\nWill process {active_count} active connection(s)")

# COMMAND ----------

# DBTITLE 1,Check for Existing Records Today (Idempotency)
today_date = date.today()
today_date_str = today_date.strftime('%Y-%m-%d')

# Check if we already have records for today
existing_check = spark.sql(f"""
    SELECT connection_id, COUNT(*) as count
    FROM {status_table}
    WHERE connector = '{CONNECTOR}'
      AND DATE(checked_at) = DATE('{today_date_str}')
    GROUP BY connection_id
""").collect()

existing_ids = {row['connection_id'] for row in existing_check}

if existing_ids:
    print(f"Found {len(existing_ids)} connection(s) already processed today:")
    for conn_id in list(existing_ids)[:5]:
        print(f"  - {conn_id}")
    if len(existing_ids) > 5:
        print(f"  ... and {len(existing_ids) - 5} more")

# Filter out already-processed connections
configs_to_process = [c for c in configs if c['connection_id'] not in existing_ids]

if not configs_to_process:
    print("\nAll active connections already have status records for today")
    dbutils.notebook.exit("ALREADY_PROCESSED")

print(f"\nWill create DELAYED status for {len(configs_to_process)} connection(s)")

# COMMAND ----------

# DBTITLE 1,Create DELAYED Status Records
status_records = []
checked_at = datetime.now(timezone.utc)

print("\nCreating DELAYED status records:\n")

for config in configs_to_process:
    connection_id = config['connection_id']

    print(f"  ✓ {connection_id} (meterStatus: {config['meterStatus']})")

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

        # Dynamic status - DELAYED because API failed
        "status": "DELAYED",
        "last_sync_timestamp": None,  # Unknown - pipeline failed before data fetch

        # Job metadata
        "job_run_id": job_run_id,
        "job_state": job_state,

        # No hourly metrics available - pipeline failed
        "hourly_metrics": None,

        # Audit timestamp
        "checked_at": checked_at
    }

    status_records.append(status_record)

print(f"\n{'='*60}")
print(f"Prepared {len(status_records)} DELAYED status record(s)")
print(f"{'='*60}")

# COMMAND ----------

# DBTITLE 1,Write Status Records
# Define schema (same as b10_update_batch_connection_metadata)
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

status_df = spark.createDataFrame(status_records, schema=status_schema)

# Append records to the table
status_df.write.mode("append").saveAsTable(status_table)

print(f"\n✓ Successfully appended {len(status_records)} DELAYED status record(s) to {status_table}")

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "="*60)
print("PIPELINE FAILURE STATUS HANDLER - COMPLETE")
print("="*60)
print(f"Connector: {CONNECTOR}")
print(f"Status created: DELAYED (API ingestion failed)")
print(f"Active meters processed: {len(status_records)}")
print(f"Inactive meters skipped: {inactive_count}")
print(f"Table: {status_table}")
print(f"Timestamp: {checked_at}")
print("="*60)

# Display sample records
print("\nSample status records created:\n")
status_df.select(
    "connection_id",
    "data_name",
    "status",
    "job_state",
    "checked_at"
).show(5, truncate=False)

dbutils.notebook.exit(f"SUCCESS: Created {len(status_records)} DELAYED status records for active meters")
