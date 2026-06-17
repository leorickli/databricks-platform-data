# Databricks notebook source
# LDP definition for Smartnode metadata bronze layer. Runs inside acme_smartnode_pipeline
# (see resources/acme_pipelines.yml).
#
# Reads daily metadata snapshots written by smartnode_metadata_api2land.py from
# /Volumes/{catalog}/land/smartnode_batch/{account_id}/metadata/{yyyymmdd}/
# and materialises one row per (snapshotDate, accountId, energyassetId) into
# bronze.smartnode_metadata_batch.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, explode, to_date, to_timestamp
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

METADATA_SOURCE_PATH = spark.conf.get("smartnode.metadata_source_path")

# COMMAND ----------

SMARTNODE_METADATA_ENVELOPE_SCHEMA = StructType([
    StructField("snapshot_date", StringType(), True),
    StructField("processing_timestamp", StringType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("raw_response", StructType([
        StructField("energyassets", ArrayType(StructType([
            StructField("energyasset", IntegerType(), True),
            StructField("account", IntegerType(), True),
            StructField("validated", StringType(), True),
            StructField("bundles", MapType(StringType(), ArrayType(DoubleType())), True),
        ])), True),
    ]), True),
])

# COMMAND ----------

@dp.table(
    name="bronze.smartnode_metadata_batch",
    comment=(
        "Bronze layer for Smartnode metadata snapshots. One row per "
        "(snapshotDate, accountId, energyassetId). The validated field indicates "
        "the last validated date per the Smartnode API. bundles is preserved as a "
        "MapType for completeness; it is not used downstream."
    ),
    cluster_by_auto=True,
    schema=StructType([
        StructField("snapshotDate",            DateType(),                                     True,  {"comment": "Date of the metadata snapshot (parsed from yyyyMMdd string in the envelope)"}),
        StructField("accountId",               IntegerType(),                                  True,  {"comment": "Smartnode account identifier"}),
        StructField("processing_timestamp",    StringType(),                                   True,  {"comment": "Timestamp when the api2land task wrote this file (yyyyMMdd_HHmmss)"}),
        StructField("energyassetId",           IntegerType(),                                  True,  {"comment": "Smartnode energy asset identifier (building or installation)"}),
        StructField("validated",               TimestampType(),                                True,  {"comment": "Last validated timestamp for this asset per the Smartnode API"}),
        StructField("bundles",                 MapType(StringType(), ArrayType(DoubleType())), True,  {"comment": "Monthly bundle values per category as returned by the API; preserved for completeness, not used downstream"}),
        StructField("_source_file",            StringType(),                                   True,  {"comment": "Path of the land volume file this row was read from"}),
        StructField("_file_modification_time", TimestampType(),                                True,  {"comment": "Last-modified timestamp of the source file"}),
        StructField("_ingested_at",            TimestampType(),                                True,  {"comment": "Timestamp when this row was written to bronze"}),
        StructField("_rescued_data",           StringType(),                                   True,  {"comment": "Fields present in the source JSON that do not match the declared envelope schema (account and addresses blocks flow here intentionally)"}),
    ]),
)
@dp.expect_or_fail("valid_account", "accountId IS NOT NULL")
@dp.expect_or_drop("valid_energyasset", "energyassetId IS NOT NULL")
@dp.expect_or_drop("valid_snapshot_date", "snapshotDate IS NOT NULL")
@dp.expect("no_unexpected_drift", "_rescued_data IS NULL")
def smartnode_metadata_batch():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("multiLine", "true")
        .schema(SMARTNODE_METADATA_ENVELOPE_SCHEMA)
        .load(METADATA_SOURCE_PATH)

        .select(
            to_date(col("snapshot_date"), "yyyyMMdd").alias("snapshotDate"),
            col("account_id").alias("accountId"),
            col("processing_timestamp"),
            explode(col("raw_response.energyassets")).alias("asset"),
            col("_metadata.file_path").alias("_source_file"),
            col("_metadata.file_modification_time").alias("_file_modification_time"),
            col("_rescued_data"),
        )

        .select(
            col("snapshotDate"),
            col("accountId"),
            col("processing_timestamp"),
            col("asset.energyasset").alias("energyassetId"),
            to_timestamp(col("asset.validated")).alias("validated"),
            col("asset.bundles").alias("bundles"),
            col("_source_file"),
            col("_file_modification_time"),
            current_timestamp().alias("_ingested_at"),
            col("_rescued_data"),
        )
    )
