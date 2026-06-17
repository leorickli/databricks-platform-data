# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

# Widget to control initial position:
# - TRIM_HORIZON: Read all available data (for testing/backfill)
# - LATEST: Only read new data (for real-time monitoring when pipeline is continuously running)
dbutils.widgets.dropdown("initial_position", "TRIM_HORIZON", ["LATEST", "TRIM_HORIZON"], "Initial Position")
INITIAL_POSITION = dbutils.widgets.get("initial_position")

BRONZE_MONITORING_TABLE_NAME = "monitoring_kafka_inverters_bronze_stream"
BRONZE_MONITORING_TABLE = f"{CATALOG_NAME}.operational.{BRONZE_MONITORING_TABLE_NAME}"

AWS_REGION = "eu-central-1"
KINESIS_SOURCE_STREAM_NAME = "dpx-kinesis-globex-bronze"
KINESIS_IAM_ROLE_ARN = "arn:aws:iam::867344430965:role/dpx-databricks-kinesis-role"

CHECKPOINT_LOCATION = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/{BRONZE_MONITORING_TABLE_NAME}"

print(f"Configuration:")
print(f"  Kinesis Stream: {KINESIS_SOURCE_STREAM_NAME}")
print(f"  Initial Position: {INITIAL_POSITION}")
print(f"  Checkpoint: {CHECKPOINT_LOCATION}")
print(f"  Target Table: {BRONZE_MONITORING_TABLE}")

# COMMAND ----------

# DBTITLE 1,Read from Kinesis Stream
kinesis_df = (
    spark
    .readStream
    .format("kinesis")
    .option("streamName", KINESIS_SOURCE_STREAM_NAME)
    .option("region", AWS_REGION)
    .option("roleArn", KINESIS_IAM_ROLE_ARN)
    .option("initialPosition", INITIAL_POSITION)
    .option("awsSTSRoleSessionName", "databricks-kinesis-session")
    .load()
)

# COMMAND ----------

# DBTITLE 1,Define Source Schema
# Bronze layer schema - matches the GLOBEX kafka inverters bronze table structure
bronze_schema = StructType([
    StructField("ingestion_timestamp", StringType(), True),
    StructField("source_topic", StringType(), True),
    StructField("consumer_group", StringType(), True),
    StructField("batch_file_path", StringType(), True),
    StructField("project_id", StringType(), True),
    StructField("application_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("project_description", StringType(), True),
    StructField("application_description", StringType(), True),
    StructField("device_description", StringType(), True),
    StructField("device_manufacturer", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("device_serial", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("location_description", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("altitude", StringType(), True),
    StructField("measurements", StringType(), True),
    StructField("bronze_processing_timestamp", StringType(), True),
    StructField("kinesis_export_timestamp", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Parse and Transform Data
parsed_df = (
    kinesis_df
    .select(
        col("data").cast("string").alias("json_data"),
        col("approximateArrivalTimestamp"),
        col("partitionKey")
    )
    .select(
        from_json(col("json_data"), bronze_schema).alias("parsed"),
        col("approximateArrivalTimestamp")
    )
    .select(
        col("parsed.ingestion_timestamp"),
        col("parsed.source_topic"),
        col("parsed.consumer_group"),
        col("parsed.device_id"),
        col("parsed.timestamp"),
        col("parsed.device_description"),
        col("parsed.device_manufacturer"),
        col("parsed.device_type"),
        col("parsed.device_serial"),
        col("parsed.location_id"),
        col("parsed.location_description"),
        col("parsed.measurements"),
        col("parsed.bronze_processing_timestamp"),
        col("parsed.kinesis_export_timestamp"),
        current_timestamp().cast("string").alias("monitoring_timestamp")
    )
)

# COMMAND ----------

# DBTITLE 1,Create Bronze Monitoring Table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_MONITORING_TABLE} (
        ingestion_timestamp STRING COMMENT 'Timestamp of when the batch file was created and ingested into the landing zone',
        source_topic STRING COMMENT 'The source Kafka topic from which the messages were consumed',
        consumer_group STRING COMMENT 'The Kafka consumer group ID used to fetch the messages',
        device_id STRING COMMENT 'Unique identifier for the specific device sending the data',
        timestamp STRING COMMENT 'Original event timestamp from the device, as a Unix epoch in milliseconds',
        device_description STRING COMMENT 'Descriptive name for the device',
        device_manufacturer STRING COMMENT 'Manufacturer of the device',
        device_type STRING COMMENT 'Model or type of the device',
        device_serial STRING COMMENT 'Serial number of the device',
        location_id STRING COMMENT 'Identifier for the physical location of the device',
        location_description STRING COMMENT 'Descriptive name for the location',
        measurements STRING COMMENT 'A JSON string containing an array of measurements from the device',
        bronze_processing_timestamp STRING COMMENT 'When the record was processed in the bronze layer',
        kinesis_export_timestamp STRING COMMENT 'Timestamp when the record was exported to Kinesis',
        monitoring_timestamp STRING COMMENT 'Timestamp when this monitoring table processed the record from Kinesis'
    ) CLUSTER BY AUTO
    COMMENT 'Monitoring table for the bronze layer from the GLOBEX Kafka inverters Kinesis stream';
""")

# COMMAND ----------

# DBTITLE 1,Write to Bronze Monitoring Delta Table
bronze_query = (
    parsed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("mergeSchema", "true")
    .trigger(processingTime = "1 seconds")
    .toTable(BRONZE_MONITORING_TABLE)
)
