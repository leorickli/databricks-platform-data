# Databricks notebook source
# LDP definition for AMPCORE CurrentSensor silver layer. Runs inside acme_ampcore_pipeline
# (see resources/acme_pipelines.yml).
#
# Reads streaming bronze.ampcore_data_batch and publishes
# silver.ampcore_econnection_measurements — one row per (gatewayId, objectId, timestamp)
# after dedup, with normalized column names and energy_interval derived.
#
# All complex PySpark logic lives here so gold can be pure SQL:
#   - Dedup (absorbs ampcore_data_api2land retries and backfill re-emits)
#   - energy_interval via per-sensor stateful streaming (see compute_interval)
#
# Modeling: every AMPCORE SCU200 CurrentSensor is modelled as an EConnection
# metering point. Direction (Import / Export) is a property of the sensor and
# lives on the dim (gold.d_ampcore_econnections); telemetry column names here are
# direction-neutral so the same metrics work for any sensor.

from typing import Iterator, Tuple

import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

STATE_SCHEMA = StructType([
    StructField("last_energy", DoubleType(), True),
    StructField("last_timestamp", TimestampType(), True),
])

SILVER_OUTPUT_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("gateway_id", StringType(), True),
    StructField("object_id", IntegerType(), True),
    StructField("current_rms", DoubleType(), True),
    StructField("current", DoubleType(), True),
    StructField("dc_current", DoubleType(), True),
    StructField("power", DoubleType(), True),
    StructField("energy_cumulative", DoubleType(), True),
    StructField("energy_interval", DoubleType(), True),
    StructField("resolution", StringType(), True),
    StructField("_rescued_data", StringType(), True),
])

# COMMAND ----------

def compute_interval(
    key: Tuple,
    pdfs: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """
    Per-sensor stateful transform that handles two concerns in one pass:

    1. Dedup: rows are sorted by timestamp; any row with timestamp <= last seen
       timestamp is skipped. This absorbs ampcore_data_api2land retries and out-of-order
       arrivals without a separate dropDuplicatesWithinWatermark stateful op.

    2. energy_interval: delta from the previous processed reading.
       Negative deltas (counter resets / meter swaps) become NULL — the true
       energy delivered across a reset is unknown.

    Watermark eviction (8 days): if a sensor produces no readings for 8 days,
    state is purged. The first reading after that gap gets NULL interval —
    accurate, since we don't know what happened in the gap. The 8-day window
    matches the api2land lookback so backfilled rows up to a week late still
    pass the watermark and reach silver.
    """
    if state.hasTimedOut:
        state.remove()
        return

    gateway_id, object_id = key

    if state.exists:
        last_energy, last_timestamp = state.get
    else:
        last_energy, last_timestamp = None, None

    for pdf in pdfs:
        pdf = pdf.sort_values("timestamp").reset_index(drop=True)
        output_rows = []

        for _, row in pdf.iterrows():
            current_ts = row["timestamp"]
            current_energy = row["energy_cumulative"]

            if last_timestamp is not None and current_ts <= last_timestamp:
                continue

            interval = None
            if (
                last_energy is not None
                and current_energy is not None
                and current_energy >= last_energy
            ):
                interval = current_energy - last_energy

            output_rows.append({
                "timestamp": current_ts,
                "gateway_id": gateway_id,
                "object_id": object_id,
                "current_rms": row["current_rms"],
                "current": row["current"],
                "dc_current": row["dc_current"],
                "power": row["power"],
                "energy_cumulative": current_energy,
                "energy_interval": interval,
                "resolution": row["resolution"],
                "_rescued_data": row["_rescued_data"],
            })

            if current_energy is not None:
                last_energy = current_energy
                last_timestamp = current_ts

        if output_rows:
            yield pd.DataFrame(output_rows)[[f.name for f in SILVER_OUTPUT_SCHEMA.fields]]

    state.update((last_energy, last_timestamp))
    if last_timestamp is not None:
        deadline_ms = int(last_timestamp.timestamp() * 1000) + 8 * 24 * 60 * 60 * 1000
        state.setTimeoutTimestamp(deadline_ms)

# COMMAND ----------

@dp.table(
    name="silver.ampcore_econnection_measurements",
    comment=(
        "Silver layer for AMPCORE SCU200 CurrentSensor readings, modelled as "
        "EConnection metering points. One row per (gateway_id, object_id, timestamp) "
        "after dedup. Column names follow telemetry conventions (current_rms, "
        "power, energy_cumulative, ...) and are direction-neutral — "
        "Import/Export direction lives on gold.d_ampcore_econnections. "
        "energy_cumulative is the raw cumulative kWh counter. energy_interval is "
        "the per-30s delta, derived here via stateful streaming so gold can be SQL."
    ),
    schema=StructType([
        StructField("timestamp",                   TimestampType(), True, {"comment": "UTC timestamp of the 30-second reading"}),
        StructField("gateway_id",                  StringType(),    True, {"comment": "AMPCORE SCU200 gateway identifier (original_id_in_source component)"}),
        StructField("object_id",                   IntegerType(),   True, {"comment": "Sensor object ID within the SCU200 gateway (original_id_in_source component)"}),
        StructField("current_rms",                 DoubleType(),    True, {"comment": "True-RMS current (A); CURRENT — renamed from AMPCORE Modbus currentTrms"}),
        StructField("current",                     DoubleType(),    True, {"comment": "AC current component (A); CURRENT — renamed from AMPCORE Modbus currentAc"}),
        StructField("dc_current",                  DoubleType(),    True, {"comment": "DC current component (A); CURRENT, InPort — renamed from AMPCORE Modbus currentDc"}),
        StructField("power",                       DoubleType(),    True, {"comment": "Active power (W); POWER — renamed from AMPCORE Modbus activePowerTotal"}),
        StructField("energy_cumulative",           DoubleType(),    True, {"comment": "Cumulative active energy counter (kWh); ENERGY — renamed from activeEnergyTotal"}),
        StructField("energy_interval",             DoubleType(),    True, {"comment": "Energy consumed in this 30-second interval, derived as a delta via stateful streaming (kWh); NULL on first reading after a gap or counter reset"}),
        StructField("resolution",                  StringType(),    True, {"comment": "Data resolution (always '30s', enforced by DQ constraint)"}),
        StructField("_rescued_data",               StringType(),    True, {"comment": "Fields present in bronze that do not match the declared silver schema"}),
        StructField("silver_processing_timestamp", TimestampType(), True, {"comment": "Timestamp when this row was written to silver"}),
    ]),
)
@dp.expect_or_fail("valid_gateway_id", "gateway_id IS NOT NULL")
@dp.expect_or_fail("expected_resolution", "resolution = '30s'")
@dp.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dp.expect_or_drop("valid_object_id", "object_id IS NOT NULL")
@dp.expect_or_drop(
    "at_least_one_value",
    "current_rms IS NOT NULL OR current IS NOT NULL OR dc_current IS NOT NULL "
    "OR power IS NOT NULL OR energy_cumulative IS NOT NULL",
)
@dp.expect("no_schema_drift", "_rescued_data IS NULL")
@dp.expect("non_negative_current", "current_rms IS NULL OR current_rms >= 0")
@dp.expect("reasonable_current", "current_rms IS NULL OR current_rms < 1000")
def ampcore_econnection_measurements():
    return (
        dp.read_stream("bronze.ampcore_data_batch")
        .select(
            col("timestamp"),
            col("object_id"),
            col("gateway_id"),
            col("currentTrms").alias("current_rms"),
            col("currentAc").alias("current"),
            col("currentDc").alias("dc_current"),
            col("activePowerTotal").alias("power"),
            col("activeEnergyTotal").alias("energy_cumulative"),
            col("resolution"),
            col("_rescued_data"),
        )
        .withWatermark("timestamp", "8 days")
        .groupBy("gateway_id", "object_id")
        .applyInPandasWithState(
            func=compute_interval,
            outputStructType=SILVER_OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
        .withColumn("silver_processing_timestamp", current_timestamp())
    )
