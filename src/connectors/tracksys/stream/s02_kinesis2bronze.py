# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql.functions import col, from_json, to_json, current_timestamp, size
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, LongType, IntegerType, BooleanType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("bronze_table_name", "tracksys_stream", "Bronze table name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_PATH = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

CHECKPOINT_LOCATION = f"/Volumes/{CATALOG_NAME}/operational/checkpoints/{BRONZE_TABLE_NAME}"

AWS_REGION = "eu-central-1"
KINESIS_SOURCE_STREAM_NAME = "dpx-kinesis-acme-ingestion"
KINESIS_IAM_ROLE_ARN = "arn:aws:iam::867344430965:role/dpx-databricks-kinesis-role"

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
# Nested structure with Lambda wrapper
nested_lambda_schema = StructType([
    StructField("clientId", StringType(), True),
    StructField("receivedAt", LongType(), True),
    StructField("payload", StructType([
        StructField("version", IntegerType(), True),
        StructField("id", StringType(), True),
        StructField("loggerImei", LongType(), True),
        StructField("vin", StringType(), True),
        StructField("startTime", StringType(), True),
        StructField("signals", ArrayType(
            StructType([
                StructField("source", StringType(), True),
                StructField("name", StringType(), True),
                StructField("displayName", StringType(), True),
                StructField("number", StringType(), True),
                StructField("unit", StringType(), True),
                StructField("isNumericComplement", BooleanType(), True),
                StructField("values", ArrayType(
                    StructType([
                        StructField("timestamp", StringType(), True),
                        StructField("value", StringType(), True)
                    ])
                ), True)
            ])
        ), True)
    ]), True)
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
        from_json(col("json_data"), nested_lambda_schema).alias("parsed"),
        col("approximateArrivalTimestamp")
    )
    .select(
        col("parsed.clientId").alias("client_id"),
        col("parsed.receivedAt").cast("string").alias("lambda_received_at"),
        col("parsed.payload.loggerImei").cast("string").alias("logger_imei"), 
        col("parsed.payload.id").alias("session_id"),
        col("parsed.payload.startTime").alias("session_start_time"),
        to_json(col("parsed.payload.signals")).alias("signals"), # Will have the timestamp for each signal
        size(col("parsed.payload.signals")).cast("string").alias("signal_count"),
        col("approximateArrivalTimestamp").cast("string").alias("kinesis_arrival_timestamp"),
        current_timestamp().cast("string").alias("bronze_processing_timestamp")
    )
)

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
spark.sql(f"""   
    CREATE TABLE IF NOT EXISTS {BRONZE_PATH} (
        client_id STRING COMMENT 'API client identifier from API Gateway',
        lambda_received_at STRING COMMENT 'Timestamp when Lambda function received the data from TRACKSYS (epoch milliseconds as string)',
        logger_imei STRING COMMENT 'International Mobile Equipment Identity of the data logger device',
        session_id STRING COMMENT 'Unique identifier for the logging session',
        session_start_time STRING COMMENT 'Timestamp when the logging session (payload) started',
        signals STRING COMMENT 'JSON array containing all signals and their time-series values from this message',
        signal_count STRING COMMENT 'Number of signals in the signals JSON array',
        kinesis_arrival_timestamp STRING COMMENT 'Timestamp when the record arrived at AWS Kinesis',
        bronze_processing_timestamp STRING COMMENT 'When the record was processed in the bronze layer'
    ) CLUSTER BY AUTO
    COMMENT 'Bronze layer table containing raw streaming telemetry data from TRACKSYS loggers. Data is minimally transformed from source with all fields stored as strings for maximum flexibility.';
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
    .trigger(processingTime="1 seconds")
    .toTable(BRONZE_PATH)
)