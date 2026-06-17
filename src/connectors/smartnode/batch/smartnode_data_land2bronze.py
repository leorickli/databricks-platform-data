# Databricks notebook source
# LDP definition for Smartnode hourly data bronze layer. Runs inside acme_smartnode_pipeline
# (see resources/acme_pipelines.yml).
#
# Reads JSON envelopes written by smartnode_data_api2land.py from
# /Volumes/{catalog}/land/smartnode_batch/{account_id}/data/{yyyymmdd}/
# and materialises one row per (accountId, energyassetId, energyassetcategory, timestamp)
# into bronze.smartnode_data_batch. All ~19 energyassetcategories are preserved here;
# silver filters to categories 10 (generation), 26 (grid export), 27 (grid import).

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, explode, to_timestamp
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

SOURCE_PATH = spark.conf.get("smartnode.source_path")

# COMMAND ----------

SMARTNODE_DATA_ENVELOPE_SCHEMA = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("target_date", StringType(), True),
    StructField("range_from", StringType(), True),
    StructField("range_to", StringType(), True),
    StructField("processing_timestamp", StringType(), True),
    StructField("raw_response", StructType([
        StructField("energyassetbundles", ArrayType(StructType([
            StructField("energyasset", IntegerType(), True),
            StructField("energyassetcategory", IntegerType(), True),
            StructField("bundle", DoubleType(), True),
            StructField("expectedvalue", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("value", DoubleType(), True),
        ])), True),
    ]), True),
])

# COMMAND ----------

@dp.table(
    name="bronze.smartnode_data_batch",
    comment=(
        "Bronze layer for Smartnode mySmartnode hourly energyassetbundles. One row per "
        "(accountId, energyassetId, energyassetcategory, timestamp). All ~19 "
        "energyassetcategories are preserved; silver filters to cats 10 "
        "(generation), 26 (grid export), 27 (grid import) and pivots to wide format."
    ),
    cluster_by_auto=True,
    schema=StructType([
        StructField("accountId",               IntegerType(),   True,  {"comment": "Smartnode account identifier"}),
        StructField("target_date",             StringType(),    True,  {"comment": "Target ingestion date (yyyy-MM-dd), derived from OFFSET_DAYS widget"}),
        StructField("processing_timestamp",    StringType(),    True,  {"comment": "Timestamp when the api2land task wrote this file (yyyyMMdd_HHmmss)"}),
        StructField("energyassetId",           IntegerType(),   True,  {"comment": "Smartnode energy asset identifier (building or installation)"}),
        StructField("energyassetcategory",     IntegerType(),   True,  {"comment": "Smartnode energy measurement category (e.g. 10=generation, 26=export, 27=import)"}),
        StructField("bundle",                  DoubleType(),    True,  {"comment": "Bundle value for this category as returned by the API"}),
        StructField("expectedvalue",           DoubleType(),    True,  {"comment": "Expected value for this category as returned by the API"}),
        StructField("timestamp",               TimestampType(), True,  {"comment": "UTC start of the hourly interval (parsed from ISO-8601 string in the API response)"}),
        StructField("value",                   DoubleType(),    True,  {"comment": "Measured value for this category in this hour"}),
        StructField("_source_file",            StringType(),    True,  {"comment": "Path of the land volume file this row was read from"}),
        StructField("_file_modification_time", TimestampType(), True,  {"comment": "Last-modified timestamp of the source file"}),
        StructField("_ingested_at",            TimestampType(), True,  {"comment": "Timestamp when this row was written to bronze"}),
        StructField("_rescued_data",           StringType(),    True,  {"comment": "Fields present in the source JSON that do not match the declared envelope schema"}),
    ]),
)
def smartnode_data_batch():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("multiLine", "true")
        .schema(SMARTNODE_DATA_ENVELOPE_SCHEMA)
        .load(SOURCE_PATH)

        .select(
            col("account_id").alias("accountId"),
            col("target_date"),
            col("processing_timestamp"),
            explode(col("raw_response.energyassetbundles")).alias("record"),
            col("_metadata.file_path").alias("_source_file"),
            col("_metadata.file_modification_time").alias("_file_modification_time"),
            col("_rescued_data"),
        )

        .select(
            col("accountId"),
            col("target_date"),
            col("processing_timestamp"),
            col("record.energyasset").alias("energyassetId"),
            col("record.energyassetcategory").alias("energyassetcategory"),
            col("record.bundle").alias("bundle"),
            col("record.expectedvalue").alias("expectedvalue"),
            to_timestamp(col("record.timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp"),
            col("record.value").alias("value"),
            col("_source_file"),
            col("_file_modification_time"),
            current_timestamp().alias("_ingested_at"),
            col("_rescued_data"),
        )
    )
