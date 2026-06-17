# Databricks notebook source
# DBTITLE 1,Imports and Configuration
"""
Universal Pause Controller for Job Execution Control

This notebook checks if a job should be paused based on client-defined date ranges.
It should be attached as the FIRST task of any job that requires pause scheduling capability.

**PREREQUISITE**: Run src/lmx/e01_pause_schedules_ddl.py FIRST to create the pause_schedules table.

Behavior:
- Reads pause schedules from {client_catalog}.operational.pause_schedules table
- Checks if current date (UTC) falls within any active pause range for the job
- If paused: Raises exception to stop job execution
- If not paused: Exits successfully and allows subsequent tasks to run
- If table doesn't exist: Allows execution with warning (fail-open)

Parameters:
    - job_name: Identifier for the job (e.g., 'ampcore', 'smartnode')
    - catalog_name: Client catalog name (e.g., 'acme_dev', 'acme_prod')

Usage:
    Attach this notebook as the first task in your job YAML:

    For ACME jobs:
    tasks:
      - task_key: b00_pause_controller
        notebook_task:
          notebook_path: ../src/lmx/b00_pause_controller.py
          base_parameters:
            job_name: ampcore
            catalog_name: ${var.acme_catalog}
          source: WORKSPACE
        job_cluster_key: <your_cluster>

    For another client's jobs:
    tasks:
      - task_key: b00_pause_controller
        notebook_task:
          notebook_path: ../src/lmx/b00_pause_controller.py
          base_parameters:
            job_name: smartnode
            catalog_name: ${var.acme_catalog}
          source: WORKSPACE
        job_cluster_key: <your_cluster>

      - task_key: b01_actual_first_task
        depends_on:
          - task_key: b00_pause_controller
        ...
"""
from datetime import date

# COMMAND ----------

# DBTITLE 1,Widget Configuration
dbutils.widgets.text("job_name", "", "Job Name (e.g., ampcore, smartnode)")
dbutils.widgets.text("catalog_name", "", "Client Catalog Name (e.g., acme_dev)")

JOB_NAME = dbutils.widgets.get("job_name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
PAUSE_SCHEDULES_TABLE = f"{CATALOG_NAME}.operational.pause_schedules"

# Validate required parameters
if not JOB_NAME:
    raise ValueError("job_name parameter is required. Please provide the job identifier.")
if not CATALOG_NAME:
    raise ValueError("catalog_name parameter is required. Please provide the client catalog name (e.g., acme_dev).")

print(f"{'='*60}")
print(f"PAUSE CONTROLLER - JOB EXECUTION CHECK")
print(f"{'='*60}")
print(f"Job Name: {JOB_NAME}")
print(f"Pause Schedules Table: {PAUSE_SCHEDULES_TABLE}")
print(f"Current Date (UTC): {date.today()}")
print(f"{'='*60}\n")

# COMMAND ----------

# DBTITLE 1,Check if Pause Schedules Table Exists
try:
    # Check if the pause_schedules table exists
    spark.sql(f"DESCRIBE TABLE {PAUSE_SCHEDULES_TABLE}")
    print(f"✅ Pause schedules table exists: {PAUSE_SCHEDULES_TABLE}\n")
except Exception as e:
    # Table doesn't exist - allow job to continue (fail-open)
    print(f"ℹ️  Pause schedules table does not exist: {PAUSE_SCHEDULES_TABLE}")
    print(f"   Error: {e}")
    print(f"✅ No pause schedules configured. Job execution allowed.\n")
    print(f"💡 To enable pause scheduling:")
    print(f"   1. Run src/lmx/e01_pause_schedules_ddl.py to create the table")
    print(f"   2. Use src/lmx/e02_manage_pause_schedules.py to add schedules\n")
    dbutils.notebook.exit("SUCCESS: No pause schedules table - execution allowed (fail-open)")

# COMMAND ----------

# DBTITLE 1,Query Active Pause Schedules for Job
current_date_val = date.today()

try:
    # Query pause schedules that:
    # 1. Match the job_name
    # 2. Are currently active (is_active = true)
    # 3. Current date falls within the pause range (inclusive)
    print(f"Querying pause schedules for job: {JOB_NAME}")

    active_pauses_df = spark.sql(f"""
        SELECT
            job_name,
            pause_start_date,
            pause_end_date,
            reason,
            created_by,
            created_at
        FROM {PAUSE_SCHEDULES_TABLE}
        WHERE job_name = '{JOB_NAME}'
          AND is_active = TRUE
          AND CURRENT_DATE() BETWEEN pause_start_date AND pause_end_date
        LIMIT 1
    """)

    print(f"Collecting results...")
    active_pauses = active_pauses_df.collect()
    print(f"Found {len(active_pauses)} active pause(s)")

    if active_pauses:
        # Job is currently in a pause window
        pause = active_pauses[0]
        print(f"🔴 JOB PAUSED - Execution blocked for '{JOB_NAME}'")
        print(f"Pause period: {pause.pause_start_date} to {pause.pause_end_date}")
        print(f"Reason: {pause.reason or 'Not specified'}")
        print(f"Job will resume after: {pause.pause_end_date}\n")

        # Raise exception to stop job execution
        raise Exception(
            f"Job '{JOB_NAME}' is currently paused. "
            f"Pause period: {pause.pause_start_date} to {pause.pause_end_date}. "
            f"Reason: {pause.reason or 'Not specified'}. "
            f"This is expected behavior - the job will resume automatically after the pause period ends."
        )
    else:
        # No active pause schedules - job can run
        print(f"✅ NO ACTIVE PAUSE SCHEDULES")
        print(f"{'='*60}")
        print(f"Job '{JOB_NAME}' is allowed to execute.")
        print(f"Current date {current_date_val} is not within any active pause range.")
        print(f"{'='*60}\n")

        dbutils.notebook.exit(f"SUCCESS: Job '{JOB_NAME}' execution allowed - no active pause schedules")

except Exception as e:
    # If the exception is our intentional pause exception, re-raise it
    if "is currently paused" in str(e):
        raise

    # For any other error, log it but allow execution to continue
    # (fail-open approach to prevent blocking jobs due to controller errors)
    print(f"⚠️  Error checking pause schedules: {e}")
    print(f"✅ Allowing job execution to continue (fail-open behavior)\n")
    dbutils.notebook.exit(f"SUCCESS: Error checking pause schedules - execution allowed (fail-open)")
