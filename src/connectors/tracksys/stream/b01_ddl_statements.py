# Databricks notebook source
# DBTITLE 1,Parameters
dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("bronze_table_name", "tracksys_stream", "Bronze table name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_PATH = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

dbutils.widgets.text("silver_table_name", "tracksys_stream", "Silver table name")
SILVER_TABLE_NAME = dbutils.widgets.get("silver_table_name")
SILVER_PATH = f"{CATALOG_NAME}.silver.{SILVER_TABLE_NAME}"

dbutils.widgets.text("imei_registry_table_name", "imei_signal_mappings_streaming", "IMEI signal mappings for streaming")
IMEI_REGISTRY_TABLE_NAME = dbutils.widgets.get("imei_registry_table_name")
IMEI_REGISTRY_PATH = f"{CATALOG_NAME}.operational.{IMEI_REGISTRY_TABLE_NAME}"

dbutils.widgets.text("imei_state_table_name", "imei_signal_mappings_state_streaming", "IMEI signal mappings state table for registry")
IMEI_STATE_TABLE_NAME = dbutils.widgets.get("imei_state_table_name")
IMEI_STATE_PATH = f"{CATALOG_NAME}.operational.{IMEI_STATE_TABLE_NAME}"

dbutils.widgets.text("state_table_name", "tracksys_ingestion_alert_state", "Monitor state table for TRACKSYS pipeline")
SOURCE_DATA_MONITORING_TABLE_NAME = dbutils.widgets.get("state_table_name")
SOURCE_DATA_MONITORING_PATH = f"{CATALOG_NAME}.operational.{SOURCE_DATA_MONITORING_TABLE_NAME}"

# COMMAND ----------

# DBTITLE 1,Bronze Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_PATH} (
    client_id STRING COMMENT 'API client identifier from API Gateway',
    lambda_received_at STRING COMMENT 'Timestamp when Lambda function received the data from TRACKSYS (epoch milliseconds as string)',
    logger_imei STRING COMMENT 'International Mobile Equipment Identity of the data logger device',
    session_id STRING COMMENT 'Unique identifier for the logging session',
    session_start_time STRING COMMENT 'Timestamp when the logging session started',
    signals STRING COMMENT 'JSON array containing all signals and their time-series values from this message',
    kinesis_arrival_timestamp STRING COMMENT 'Timestamp when the record arrived at AWS Kinesis',
    bronze_processing_timestamp STRING COMMENT 'Timestamp when Databricks processed and wrote the record to Bronze layer',
    signal_count STRING COMMENT 'Number of signals in the signals JSON array'
) CLUSTER BY AUTO
COMMENT 'Bronze layer table containing raw streaming telemetry data from TRACKSYS loggers. Data is minimally transformed from source with all fields stored as strings for maximum flexibility.';
""")

# COMMAND ----------

# DBTITLE 1,Silver Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_PATH} (
  loggerImei BIGINT COMMENT 'International Mobile Equipment Identity of the data logger device (originalIdInSource on the ESDL Battery asset)',
  SoC INT COMMENT 'State of charge percentage 0-100 (ESDL Battery.SoC, %)',
  voltage DOUBLE COMMENT 'Battery pack voltage in volts (ESDL Battery.V, V — DC Bus)',
  current DOUBLE COMMENT 'Battery pack current in amperes (ESDL Battery.A, A — DC Bus)',
  signalTimestamp TIMESTAMP COMMENT 'Signal measurement timestamp',
  kinesisArrivalTimestamp TIMESTAMP COMMENT 'Timestamp when the record arrived at AWS Kinesis source',
  bronzeProcessingTimestamp TIMESTAMP COMMENT 'When the record was processed in the bronze layer',
  silverProcessingTimestamp TIMESTAMP COMMENT 'When the record was processed in the silver layer'
) CLUSTER BY AUTO
COMMENT 'Time-series telemetry data with one row per timestamp, ESDL Battery-aligned';
""")

# COMMAND ----------

# DBTITLE 1,IMEI State and Registry Tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {IMEI_STATE_PATH} (
    imei BIGINT COMMENT 'Device ID being learned',
    message_count INT COMMENT 'Messages seen from this device so far',
    detected_signals ARRAY<STRING> COMMENT 'List of signal names found',
    last_updated TIMESTAMP COMMENT 'When we last saw data from this device'
) COMMENT 'Operational state for IMEI signal mapping detection during streaming processing';
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {IMEI_REGISTRY_PATH} (
    imei BIGINT COMMENT 'International Mobile Equipment Identity of the data logger device',
    -- Signal mappings
    has_all_signals BOOLEAN COMMENT 'Do we have all 3 required signals?',
    soc_signal STRING COMMENT 'Which signal name means battery charge for this device',
    voltage_signal STRING COMMENT 'Which signal name means voltage for this device',
    current_signal STRING COMMENT 'Which signal name means current for this device',
    -- Activity tracking
    first_seen TIMESTAMP COMMENT 'When we first saw this device',
    last_seen TIMESTAMP COMMENT 'When we last saw this device',
    total_messages BIGINT COMMENT 'Total count of messages ever received',
    is_active BOOLEAN COMMENT 'Is the device currently sending data?',
    days_inactive INT COMMENT 'How many days since last data (0 if active)',
    -- Metadata
    detection_timestamp TIMESTAMP COMMENT 'When we figured out the signal mappings',
    last_activity_check TIMESTAMP COMMENT 'When we last checked if device is active'
) COMMENT 'Operational registry tracking IMEI signal mappings and activity status for streaming pipeline';
""")

# COMMAND ----------

# DBTITLE 1,Create Source Data Monitoring Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SOURCE_DATA_MONITORING_PATH} (
    monitor_id STRING COMMENT 'Identifier for the monitor',
    last_alert_timestamp TIMESTAMP COMMENT 'When the last alert was sent',
    alert_active BOOLEAN COMMENT 'Is there an active unresolved alert',
    last_data_timestamp TIMESTAMP COMMENT 'Last time we saw data (for context)',
    PRIMARY KEY (monitor_id)
) COMMENT 'Manage alert state to prevent duplicate emails for ACME/TRACKSYS pipeline.'
""")