# Databricks notebook source
# DBTITLE 1,Parameters
dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("volume_name", "kafka_inverters_stream", "Volume name for the Kafka JSON data")
VOLUME_NAME = dbutils.widgets.get("volume_name")
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/land/{VOLUME_NAME}"

dbutils.widgets.text("bronze_table_name", "kafka_inverters_stream", "Bronze table name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_PATH = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

dbutils.widgets.text("silver_table_name", "kafka_inverters_stream", "Silver table name")
SILVER_TABLE_NAME = dbutils.widgets.get("silver_table_name")
SILVER_PATH = f"{CATALOG_NAME}.silver.{SILVER_TABLE_NAME}"

dbutils.widgets.text("state_table_name", "kafka_inverters_heartbeat_state_table", "Source data monitor table name")
SOURCE_DATA_MONITORING_TABLE_NAME = dbutils.widgets.get("state_table_name")
SOURCE_DATA_MONITORING_PATH = f"{CATALOG_NAME}.operational.{SOURCE_DATA_MONITORING_TABLE_NAME}"

dbutils.widgets.text("dlq_volume_name", "kafka_inverters_dlq_kafka", "DLQ volume name for failed Kafka messages")
DLQ_VOLUME_NAME = dbutils.widgets.get("dlq_volume_name")
DLQ_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/operational/{DLQ_VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Create Volume
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.land.{VOLUME_NAME}
    COMMENT 'Landing volume for raw JSON batches from Kafka inverters stream'
""")

# COMMAND ----------

# DBTITLE 1,Bronze Table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_PATH} (
        -- Batch metadata
        ingestion_timestamp TIMESTAMP COMMENT 'Timestamp of when the batch file was created and ingested into the landing zone',
        source_topic STRING COMMENT 'The source Kafka topic from which the messages were consumed',
        consumer_group STRING COMMENT 'The Kafka consumer group ID used to fetch the messages',
        batch_file_path STRING COMMENT 'Full path to the source JSON file in the landing volume for lineage',
        
        -- Message fields
        project_id STRING COMMENT 'Identifier for the overall project, e.g., "Inverters"',
        application_id STRING COMMENT 'Identifier for the specific application or data source within the project',
        device_id STRING COMMENT 'Unique identifier for the specific device sending the data',
        timestamp STRING COMMENT 'Original event timestamp from the device, as a Unix epoch in milliseconds',
        project_description STRING COMMENT 'Descriptive name for the project',
        application_description STRING COMMENT 'Descriptive name for the application',
        device_description STRING COMMENT 'Descriptive name for the device',
        device_manufacturer STRING COMMENT 'Manufacturer of the device',
        device_type STRING COMMENT 'Model or type of the device',
        device_serial STRING COMMENT 'Serial number of the device',
        location_id STRING COMMENT 'Identifier for the physical location of the device',
        location_description STRING COMMENT 'Descriptive name for the location',
        latitude STRING COMMENT 'GPS latitude of the device',
        longitude STRING COMMENT 'GPS longitude of the device',
        altitude STRING COMMENT 'Altitude of the device in meters',
        
        -- Measurements as JSON string
        measurements STRING COMMENT 'A JSON string containing an array of measurements from the device',
        
        -- Processing metadata
        bronze_processing_timestamp TIMESTAMP COMMENT 'When the record was processed in the bronze layer'
    )
    CLUSTER BY AUTO
    COMMENT 'Bronze table containing raw data ingested from the Kafka topic for solar inverters. Each record represents a single message, with the original measurements stored as a JSON string. Includes batch metadata for traceability.'
""")

# COMMAND ----------

# DBTITLE 1,Silver Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_PATH} (
    -- Haystack-compliant identifier and reference fields
    id STRING COMMENT 'Unique identifier for the device (Haystack: id)',
    idSite STRING COMMENT 'Reference to parent site entity (Haystack: siteRef, materialized as idSite)',

    -- Timestamp fields
    timestamp TIMESTAMP COMMENT 'The originating timestamp of the event, windowed to the minute (Haystack: ts)',
    ingestionTimestamp TIMESTAMP COMMENT 'The timestamp from the initial batch ingestion into the land layer',

    -- Haystack-compliant inverter measurements (SunSpec aligned)
    W DECIMAL(18, 2) COMMENT 'AC Power output in Watts (Haystack: W)',
    WH DECIMAL(18, 2) COMMENT 'Total AC Energy yield in Watt-hours (Haystack: WH)',
    dailyWH DECIMAL(18, 2) COMMENT 'Daily energy yield in Watt-hours (Custom field: dailyWH)',
    PhVphA DECIMAL(18, 2) COMMENT 'Phase Voltage AN in Volts (Haystack: PhVphA)',
    Hz DECIMAL(18, 2) COMMENT 'Line Frequency in Hertz (Haystack: Hz)',
    TmpCab DECIMAL(18, 2) COMMENT 'Cabinet Temperature in Celsius (Haystack: TmpCab)',
    operatingMode DECIMAL(18, 2) COMMENT 'Device operating mode code (Custom field: operatingMode)',

    -- Silver layer processing metadata
    silverProcessingTimestamp TIMESTAMP COMMENT 'Timestamp when the record was processed into the silver table'
)
CLUSTER BY AUTO
COMMENT 'Silver table containing Haystack ontology-compliant measurements for SMA solar inverters.'
""")

# COMMAND ----------

# DBTITLE 1,Source Data Monitoring Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SOURCE_DATA_MONITORING_PATH} (
    monitor_id STRING COMMENT 'Identifier for the monitor',
    last_alert_timestamp TIMESTAMP COMMENT 'When the last alert was sent',
    alert_active BOOLEAN COMMENT 'Is there an active unresolved alert',
    last_data_timestamp TIMESTAMP COMMENT 'Last time we saw data (for context)',
    PRIMARY KEY (monitor_id)
)
COMMENT 'Manage alert state to prevent duplicate emails.'
""")

# COMMAND ----------

# DBTITLE 1,Create DLQ Volume
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.operational.{DLQ_VOLUME_NAME}
    COMMENT 'Dead Letter Queue volume for failed Kafka messages that could not be deserialized'
""")