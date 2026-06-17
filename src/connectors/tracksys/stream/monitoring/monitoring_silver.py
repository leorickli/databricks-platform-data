# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

SILVER_MONITORING_TABLE_NAME = "monitoring_silver_stream"
SILVER_MONITORING_TABLE = f"{CATALOG_NAME}.monitoring.{SILVER_MONITORING_TABLE_NAME}"

AWS_REGION = "eu-central-1"
KINESIS_SOURCE_STREAM_NAME = "dpx-kinesis-acme-silver"
KINESIS_IAM_ROLE_ARN = "arn:aws:iam::867344430965:role/dpx-databricks-kinesis-role"

CHECKPOINT_LOCATION = f"s3://dpx-s3-dev/acme/checkpoints/stream/{SILVER_MONITORING_TABLE_NAME}"

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
# Silver layer schema - flat structure with battery metrics
silver_schema = StructType([
    StructField("imei", LongType(), True),
    StructField("soc", DoubleType(), True),  # State of Charge - nullable
    StructField("voltage", DoubleType(), True),  # nullable
    StructField("current", DoubleType(), True),
    StructField("signal_timestamp", StringType(), True),
    StructField("kinesis_arrival_timestamp", StringType(), True),
    StructField("bronze_processing_timestamp", StringType(), True),
    StructField("silver_processing_timestamp", StringType(), True)
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
        from_json(col("json_data"), silver_schema).alias("parsed"),
        col("approximateArrivalTimestamp")
    )
    .select(
        col("parsed.imei").cast("string").alias("imei"),
        col("parsed.soc"),
        col("parsed.voltage"),
        col("parsed.current"),
        col("parsed.signal_timestamp"),
        col("parsed.kinesis_arrival_timestamp"),
        col("parsed.bronze_processing_timestamp"),
        col("parsed.silver_processing_timestamp"),
        current_timestamp().cast("string").alias("final_processing_timestamp")
    )
)

# COMMAND ----------

# DBTITLE 1,Create Silver Monitoring Table
spark.sql(f"""   
    CREATE TABLE IF NOT EXISTS {SILVER_MONITORING_TABLE} (
        imei STRING COMMENT 'International Mobile Equipment Identity of the data logger device',
        soc DOUBLE COMMENT 'State of Charge percentage (0-100)',
        voltage DOUBLE COMMENT 'Battery voltage in volts',
        current DOUBLE COMMENT 'Battery current in amperes',
        signal_timestamp STRING COMMENT 'Signal measurement timestamp',
        kinesis_arrival_timestamp STRING COMMENT 'Timestamp when the record arrived at AWS Kinesis source',
        bronze_processing_timestamp STRING COMMENT 'When the record was processed in the bronze layer',
        silver_processing_timestamp STRING COMMENT 'When the record was processed in the silver layer',
        final_processing_timestamp STRING COMMENT 'Timestamp when this monitoring table processed the record'
    ) CLUSTER BY AUTO
    COMMENT 'Monitoring table for the silver layer battery metrics from the Kinesis stream';
""")

# COMMAND ----------

# DBTITLE 1,Write to Silver Monitoring Delta Table
silver_query = (
    parsed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("mergeSchema", "true")
    .trigger(processingTime = "1 seconds")
    .toTable(SILVER_MONITORING_TABLE)
)