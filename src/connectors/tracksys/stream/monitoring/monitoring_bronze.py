# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql.functions import col, from_json, to_json, current_timestamp, size, from_unixtime, when
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, LongType, IntegerType, BooleanType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

BRONZE_MONITORING_TABLE_NAME = "monitoring_bronze_stream"
BRONZE_MONITORING_TABLE = f"{CATALOG_NAME}.monitoring.{BRONZE_MONITORING_TABLE_NAME}"

AWS_REGION = "eu-central-1"
KINESIS_SOURCE_STREAM_NAME = "dpx-kinesis-acme-bronze"
KINESIS_IAM_ROLE_ARN = "arn:aws:iam::867344430965:role/dpx-databricks-kinesis-role"

CHECKPOINT_LOCATION = f"s3://dpx-s3-dev/acme/checkpoints/stream/{BRONZE_MONITORING_TABLE_NAME}"

# COMMAND ----------

# DBTITLE 1,Read from Kinesis Stream
kinesis_df = (
    spark
    .readStream
    .format("kinesis")
    .option("streamName", KINESIS_SOURCE_STREAM_NAME)
    .option("region", AWS_REGION)
    .option("roleArn", KINESIS_IAM_ROLE_ARN)
    .option("initialPosition", "LATEST")
    .option("awsSTSRoleSessionName", "databricks-kinesis-session")
    .load()
)

# COMMAND ----------

# DBTITLE 1,Define Source Schema
# The actual schema matches the JSON structure directly (no nested payload)
json_schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("lambda_received_at", StringType(), True),
    StructField("logger_imei", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("session_start_time", StringType(), True),
    StructField("signals", StringType(), True),
    StructField("kinesis_arrival_timestamp", StringType(), True),
    StructField("bronze_processing_timestamp", StringType(), True),
    StructField("signal_count", StringType(), True),
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
        from_json(col("json_data"), json_schema).alias("parsed"),
        col("approximateArrivalTimestamp")
    )
    .select(
        col("parsed.client_id"),
        # Convert epoch milliseconds to timestamp
        when(
            col("parsed.lambda_received_at").rlike("^[0-9]+$"),
            from_unixtime(col("parsed.lambda_received_at").cast("bigint") / 1000).cast("string")
        ).otherwise(col("parsed.lambda_received_at")).alias("lambda_received_at"),
        col("parsed.logger_imei"),
        col("parsed.signals"),
        col("parsed.signal_count"),
        col("parsed.session_id"),
        col("parsed.session_start_time"),
        col("parsed.kinesis_arrival_timestamp"),
        col("parsed.bronze_processing_timestamp"),
        current_timestamp().cast("string").alias("monitoring_timestamp")
    )
)

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
spark.sql(f"""   
    CREATE TABLE IF NOT EXISTS {BRONZE_MONITORING_TABLE} (
        client_id STRING COMMENT 'API client identifier from API Gateway',
        lambda_received_at STRING COMMENT 'Timestamp when Lambda function received the data from TRACKSYS (converted from epoch milliseconds)',
        logger_imei STRING COMMENT 'International Mobile Equipment Identity of the data logger device',
        signals STRING COMMENT 'JSON array containing all signals and their time-series values from this message',
        signal_count STRING COMMENT 'Number of signals in the signals JSON array',
        session_id STRING COMMENT 'Unique identifier for the logging session',
        session_start_time STRING COMMENT 'Timestamp when the logging session started',
        kinesis_arrival_timestamp STRING COMMENT 'Timestamp when the record arrived at AWS Kinesis',
        bronze_processing_timestamp STRING COMMENT 'Timestamp when Databricks processed and wrote the record to Bronze layer (from Kinesis source)',
        monitoring_timestamp STRING COMMENT 'Actual timestamp when Databricks processed this record'
    ) CLUSTER BY AUTO
    COMMENT 'Monitoring table for the bronze layer from the Kinesis stream';
""")

# COMMAND ----------

# DBTITLE 1,Write to Bronze Delta Table
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