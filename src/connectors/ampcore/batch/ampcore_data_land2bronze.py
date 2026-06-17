# Databricks notebook source
# This file is a Lakeflow Declarative Pipelines (LDP) definition, not a
# standalone notebook. It only runs inside the `acme_ampcore_pipeline` LDP pipeline
# (see resources/acme_pipelines.yml). Running the cells in a regular notebook
# context will fail because `pyspark.pipelines` is only available to the
# LDP runtime.
#
# The pipeline writes to {catalog}.{target}.ampcore_data_batch, where `catalog`
# and `target` come from the pipeline configuration in the DABs resource.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, explode
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

SOURCE_PATH = spark.conf.get("ampcore.source_path")

# COMMAND ----------

AMPCORE_DATA_ENVELOPE_SCHEMA = StructType([
    StructField("target_date", StringType(), True),
    StructField("window_index", LongType(), True),
    StructField("begin_timestamp", LongType(), True),
    StructField("end_timestamp", LongType(), True),
    StructField("resolution", StringType(), True),
    StructField("processing_timestamp", StringType(), True),
    StructField("raw_response", StructType([
        StructField("id", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("data", ArrayType(StructType([
            StructField("timestamp", LongType(), True),
            StructField("values", MapType(
                StringType(),
                MapType(StringType(), StringType()),
            ), True),
        ])), True),
    ]), True),
])

# COMMAND ----------

@dp.table(
    name="bronze.ampcore_data_batch",
    comment=(
        "Bronze layer for AMPCORE SCU200 data endpoint. One row per (timestamp, sensor) "
        "with the five CurrentSensor variables projected as named columns. "
        "Timestamps are UTC TimestampType (cast from epoch seconds returned by the API)."
    ),
    schema=StructType([
        StructField("timestamp",               TimestampType(), True, {"comment": "UTC timestamp of the 30-second reading, cast from epoch seconds returned by the AMPCORE SCU200 API"}),
        StructField("object_id",               IntegerType(),   True, {"comment": "Sensor object ID within the SCU200 gateway"}),
        StructField("currentTrms",             DoubleType(),    True, {"comment": "RMS current (A); string in the API response, cast to Double here"}),
        StructField("currentAc",               DoubleType(),    True, {"comment": "AC current component (A)"}),
        StructField("currentDc",               DoubleType(),    True, {"comment": "DC current component (A)"}),
        StructField("activePowerTotal",        DoubleType(),    True, {"comment": "Total active power (W)"}),
        StructField("activeEnergyTotal",       DoubleType(),    True, {"comment": "Cumulative active energy counter (kWh)"}),
        StructField("gateway_id",              StringType(),    True, {"comment": "AMPCORE SCU200 gateway identifier"}),
        StructField("gateway_ip",              StringType(),    True, {"comment": "AMPCORE SCU200 gateway IP address at ingestion time"}),
        StructField("resolution",              StringType(),    True, {"comment": "Data resolution reported by the gateway (always '30s' for SCU200)"}),
        StructField("_source_file",            StringType(),    True, {"comment": "Path of the land volume file this row was read from"}),
        StructField("_file_modification_time", TimestampType(), True, {"comment": "Last-modified timestamp of the source file"}),
        StructField("_ingested_at",            TimestampType(), True, {"comment": "Timestamp when this row was written to bronze"}),
        StructField("_rescued_data",           StringType(),    True, {"comment": "Fields present in the source JSON that do not match the declared envelope schema"}),
    ]),
)
def ampcore_data_batch():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("multiLine", "true")
        .schema(AMPCORE_DATA_ENVELOPE_SCHEMA)
        .load(SOURCE_PATH)

        .select(
            col("resolution"),
            col("raw_response.id").alias("gateway_id"),
            col("raw_response.ip").alias("gateway_ip"),
            explode(col("raw_response.data")).alias("record"),
            col("_metadata.file_path").alias("_source_file"),
            col("_metadata.file_modification_time").alias("_file_modification_time"),
            col("_rescued_data"),
        )

        .select(
            col("record.timestamp").cast(TimestampType()).alias("timestamp"),
            col("resolution"),
            col("gateway_id"),
            col("gateway_ip"),
            explode(col("record.values")).alias("object_id", "vars"),
            col("_source_file"),
            col("_file_modification_time"),
            col("_rescued_data"),
        )

        .select(
            col("timestamp"),
            col("object_id").cast(IntegerType()).alias("object_id"),
            col("vars")["currentTrms"].cast(DoubleType()).alias("currentTrms"),
            col("vars")["currentAc"].cast(DoubleType()).alias("currentAc"),
            col("vars")["currentDc"].cast(DoubleType()).alias("currentDc"),
            col("vars")["activePowerTotal"].cast(DoubleType()).alias("activePowerTotal"),
            col("vars")["activeEnergyTotal"].cast(DoubleType()).alias("activeEnergyTotal"),
            col("gateway_id"),
            col("gateway_ip"),
            col("resolution"),
            col("_source_file"),
            col("_file_modification_time"),
            current_timestamp().alias("_ingested_at"),
            col("_rescued_data"),
        )
    )
