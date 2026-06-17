# Databricks notebook source
# LDP definition for AMPCORE metadata bronze layer. Runs inside acme_ampcore_pipeline
# (see resources/acme_pipelines.yml) alongside the data pipeline notebooks.
#
# Reads daily snapshot JSONs written by ampcore_metadata_api2land.py from
# /Volumes/{catalog}/land/ampcore_batch/{gateway_id}/metadata/{yyyymmdd}/ and
# materialises one row per CurrentSensor per snapshot into bronze.ampcore_metadata_batch.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, explode, to_date
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
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

METADATA_SOURCE_PATH = spark.conf.get("ampcore.metadata_source_path")

# COMMAND ----------

VARIABLE_STRUCT = StructType([
    StructField("category", StringType(), True),
    StructField("dataType", StringType(), True),
    StructField("modbusDataType", StringType(), True),
    StructField("address", IntegerType(), True),
    StructField("virtualRegisterAddress", IntegerType(), True),
    StructField("size", IntegerType(), True),
    StructField("readInterval", IntegerType(), True),
    StructField("dbWriteInterval", IntegerType(), True),
    StructField("dbStorePolicy", StringType(), True),
    StructField("invalidValues", ArrayType(StringType()), True),
    StructField("multiplier", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("writable", BooleanType(), True),
    StructField("endianness", StringType(), True),
    StructField("function", IntegerType(), True),
    StructField("readable", BooleanType(), True),
    StructField("relation", ArrayType(StringType()), True),
])

SENSOR_STRUCT = StructType([
    StructField("name", StringType(), True),
    StructField("object_id", IntegerType(), True),
    StructField("is_energy_export", IntegerType(), True),
    StructField("modbus_id", IntegerType(), True),
    StructField("serial_number", StringType(), True),
    StructField("variables", MapType(StringType(), VARIABLE_STRUCT), True),
])

AMPCORE_METADATA_ENVELOPE_SCHEMA = StructType([
    StructField("snapshot_date", StringType(), True),
    StructField("processing_timestamp", StringType(), True),
    StructField("raw_response", StructType([
        StructField("id", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("serialNumber", StringType(), True),
        StructField("CurrentSensor", ArrayType(SENSOR_STRUCT), True),
    ]), True),
])

# COMMAND ----------

@dp.table(
    name="bronze.ampcore_metadata_batch",
    comment=(
        "Bronze layer for AMPCORE SCU200 metadata endpoint. One row per CurrentSensor "
        "per daily snapshot. Gateway fields (id, ip, serialNumber) are denormalised "
        "onto every sensor row. variables is a MapType(varName → struct) preserving "
        "the raw API shape; gold reshapes this into an array of structs."
    ),
    schema=StructType([
        StructField("snapshotDate",            DateType(),                             True,  {"comment": "Date of the metadata snapshot (parsed from yyyyMMdd string in the envelope)"}),
        StructField("gatewayId",               StringType(),                           True,  {"comment": "AMPCORE SCU200 gateway identifier"}),
        StructField("gatewayIp",               StringType(),                           True,  {"comment": "AMPCORE SCU200 gateway IP address at snapshot time"}),
        StructField("gatewaySerialNumber",     StringType(),                           True,  {"comment": "AMPCORE SCU200 gateway serial number"}),
        StructField("name",                    StringType(),                           True,  {"comment": "Human-readable sensor name configured in the SCU200"}),
        StructField("objectId",                IntegerType(),                          True,  {"comment": "Sensor object ID within the SCU200 gateway"}),
        StructField("isEnergyExport",          BooleanType(),                          True,  {"comment": "True if the sensor measures energy flowing out (export/feed-in)"}),
        StructField("modbusId",                IntegerType(),                          True,  {"comment": "Modbus slave address of the physical sensor"}),
        StructField("serialNumber",            StringType(),                           True,  {"comment": "Physical sensor serial number"}),
        StructField("variables",               MapType(StringType(), VARIABLE_STRUCT), True, {"comment": "Modbus variable definitions keyed by variable name; reshaped to array of structs in gold"}),
        StructField("_source_file",            StringType(),                           True,  {"comment": "Path of the land volume file this row was read from"}),
        StructField("_file_modification_time", TimestampType(),                        True,  {"comment": "Last-modified timestamp of the source file"}),
        StructField("_ingested_at",            TimestampType(),                        True,  {"comment": "Timestamp when this row was written to bronze"}),
        StructField("_rescued_data",           StringType(),                           True,  {"comment": "Fields present in the source JSON that do not match the declared envelope schema"}),
    ]),
)
@dp.expect_or_fail("valid_gateway_id", "gatewayId IS NOT NULL")
@dp.expect_or_drop("valid_object_id", "objectId IS NOT NULL")
@dp.expect("no_schema_drift", "_rescued_data IS NULL")
def ampcore_metadata_batch():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("multiLine", "true")
        .schema(AMPCORE_METADATA_ENVELOPE_SCHEMA)
        .load(METADATA_SOURCE_PATH)
        .select(
            to_date(col("snapshot_date"), "yyyyMMdd").alias("snapshotDate"),
            col("raw_response.id").alias("gatewayId"),
            col("raw_response.ip").alias("gatewayIp"),
            col("raw_response.serialNumber").alias("gatewaySerialNumber"),
            explode(col("raw_response.CurrentSensor")).alias("sensor"),
            col("_metadata.file_path").alias("_source_file"),
            col("_metadata.file_modification_time").alias("_file_modification_time"),
            col("_rescued_data"),
        )
        .select(
            col("snapshotDate"),
            col("gatewayId"),
            col("gatewayIp"),
            col("gatewaySerialNumber"),
            col("sensor.name").alias("name"),
            col("sensor.object_id").alias("objectId"),
            col("sensor.is_energy_export").cast(BooleanType()).alias("isEnergyExport"),
            col("sensor.modbus_id").alias("modbusId"),
            col("sensor.serial_number").alias("serialNumber"),
            col("sensor.variables").alias("variables"),
            col("_source_file"),
            col("_file_modification_time"),
            current_timestamp().alias("_ingested_at"),
            col("_rescued_data"),
        )
    )
