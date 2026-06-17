# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql.functions import col, from_json, explode, lower, to_timestamp, when, current_timestamp, from_unixtime
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, BooleanType

dbutils.widgets.text("catalog_name", "", "Catalog Name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")

dbutils.widgets.text("bronze_table_name", "tracksys_stream", "Bronze table name")
BRONZE_TABLE_NAME = dbutils.widgets.get("bronze_table_name")
BRONZE_TABLE = f"{CATALOG_NAME}.bronze.{BRONZE_TABLE_NAME}"

dbutils.widgets.text("silver_table_name", "tracksys_stream", "Silver table name")
SILVER_TABLE_NAME = dbutils.widgets.get("silver_table_name")
SILVER_TABLE = f"{CATALOG_NAME}.silver.{SILVER_TABLE_NAME}"

dbutils.widgets.text("imei_registry_table_name", "imei_signal_mappings_streaming", "IMEI signal mappings table name")
IMEI_REGISTRY_TABLE_NAME = dbutils.widgets.get("imei_registry_table_name")
IMEI_REGISTRY_TABLE = f"{CATALOG_NAME}.operational.{IMEI_REGISTRY_TABLE_NAME}"

CHECKPOINT_LOCATION = f"s3://dpx-s3-dev/acme/checkpoints/stream/{SILVER_TABLE_NAME}"

# COMMAND ----------

# DBTITLE 1,Schema for "signals" Field
signals_schema = ArrayType(StructType([
    StructField("source", StringType(), True),
    StructField("name", StringType(), True),
    StructField("displayName", StringType(), True),
    StructField("number", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("isNumericComplement", BooleanType(), True),
    StructField("values", ArrayType(StructType([
        StructField("timestamp", StringType(), True),
        StructField("value", StringType(), True)
    ])), True)
]))

# COMMAND ----------

# DBTITLE 1,Process Bronze to Silver
def process_bronze_to_silver_timeseries(batch_df, batch_id):
    """Process all timestamp/value pairs from bronze to silver"""
    
    print(f"--- Starting Time-Series Batch {batch_id} ---")
    
    if batch_df.isEmpty():
        print("Batch is empty, skipping.")
        return
    
    # Convert lambda_received_at from epoch milliseconds to timestamp if it exists and is not already converted
    if "lambda_received_at" in batch_df.columns:
        batch_df = batch_df.withColumn(
            "lambda_received_at",
            when(
                col("lambda_received_at").rlike("^[0-9]+$"),
                from_unixtime(col("lambda_received_at").cast("bigint") / 1000).cast("string")
            ).otherwise(col("lambda_received_at"))
        )
    
    # Get active, fully mapped IMEIs
    signal_mappings_df = (
        spark.table(IMEI_REGISTRY_TABLE)
        .filter(
            (col("is_active") == True) & 
            (col("has_all_signals") == True)
        )
    )
    
    # Join with mappings
    joined_df = (
        batch_df.alias("bronze")
        .join(
            signal_mappings_df.alias("mappings"),
            col("bronze.logger_imei") == col("mappings.imei"),
            "inner"
        )
    )
    
    # Parse and explode signals array
    exploded_signals_df = (
        joined_df
        .select(
            col("bronze.logger_imei").cast("bigint").alias("imei"),
            col("bronze.kinesis_arrival_timestamp"),
            col("bronze.bronze_processing_timestamp"),
            from_json(col("bronze.signals"), signals_schema).alias("signals_array"),
            col("mappings.soc_signal"),
            col("mappings.voltage_signal"),
            col("mappings.current_signal")
        )
        .select("*", explode(col("signals_array")).alias("signal"))
        .select(
            "imei",
            "kinesis_arrival_timestamp",
            "bronze_processing_timestamp",
            lower(col("signal.displayName")).alias("signal_name"),
            col("signal.values").alias("values_array"),
            "soc_signal",
            "voltage_signal",
            "current_signal"
        )
    )
    
    # Further explode the values array and directly create silver records
    silver_df = (
        exploded_signals_df
        .select("*", explode(col("values_array")).alias("value_data"))
        .select(
            col("imei").alias("loggerImei"),
            col("signal_name"),
            to_timestamp(col("value_data.timestamp")).alias("signalTimestamp"),
            col("value_data.value").alias("value"),
            col("kinesis_arrival_timestamp"),
            col("bronze_processing_timestamp"),
            col("soc_signal"),
            col("voltage_signal"),
            col("current_signal")
        )
        .filter(col("signalTimestamp").isNotNull() & col("value").isNotNull())
        # Filter only the signals we care about
        .filter(
            (col("signal_name") == col("soc_signal")) |
            (col("signal_name") == col("voltage_signal")) |
            (col("signal_name") == col("current_signal"))
        )
        # Create the final silver structure with conditional columns
        .select(
            col("loggerImei"),
            when(col("signal_name") == col("soc_signal"), col("value").cast("int")).otherwise(None).alias("SoC"),
            when(col("signal_name") == col("voltage_signal"), col("value").cast("double")).otherwise(None).alias("voltage"),
            when(col("signal_name") == col("current_signal"), col("value").cast("double")).otherwise(None).alias("current"),
            col("signalTimestamp"),
            to_timestamp(col("kinesis_arrival_timestamp")).alias("kinesisArrivalTimestamp"),
            to_timestamp(col("bronze_processing_timestamp")).alias("bronzeProcessingTimestamp"),
            current_timestamp().alias("silverProcessingTimestamp")
        )
    )
    
    # Write to Silver table
    if not silver_df.rdd.isEmpty():
        record_count = silver_df.count()
        print(f"Writing {record_count} time-series records to {SILVER_TABLE}")
        
        silver_df.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)
    else:
        print("No valid time-series records in this batch.")
    
    print(f"--- Finished Time-Series Batch {batch_id} ---")

# COMMAND ----------

# DBTITLE 1,Create Silver Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
  imei BIGINT COMMENT 'International Mobile Equipment Identity of the data logger device',
  soc INT COMMENT 'State of charge percentage (0-100)',
  voltage DECIMAL(10,2) COMMENT 'Battery pack voltage in volts',
  current DECIMAL(10,2) COMMENT 'Battery pack current in amperes',
  signal_timestamp TIMESTAMP COMMENT 'Signal measurement timestamp',
  kinesis_arrival_timestamp TIMESTAMP COMMENT 'Timestamp when the record arrived at AWS Kinesis source',
  bronze_processing_timestamp TIMESTAMP COMMENT 'When the record was processed in the bronze layer',
  silver_processing_timestamp TIMESTAMP COMMENT 'When the record was processed in the silver layer'
) CLUSTER BY AUTO
COMMENT 'Time-series telemetry data with one row per timestamp';
""")

# COMMAND ----------

# DBTITLE 1,Start Streaming from Bronze to Silver
bronze_stream_df = (
    spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
)

silver_streaming_query = (
    bronze_stream_df
    .writeStream
    .foreachBatch(process_bronze_to_silver_timeseries)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("mergeSchema", "true")
    .trigger(processingTime='1 seconds')
    .start()
)