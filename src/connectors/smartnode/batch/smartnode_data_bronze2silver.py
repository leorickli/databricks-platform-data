# Databricks notebook source
# LDP definition for Smartnode data silver layer. Runs inside acme_smartnode_pipeline
# (see resources/acme_pipelines.yml).
#
# Reads bronze.smartnode_data_batch (long format: one row per asset × category × hour),
# filters to energyassetcategories 10/26/27, and publishes silver.smartnode_measurements
# in WIDE format — one row per (accountId, energyassetId, timestamp) with three
# ESDL-aligned electricity metric columns:
#   category 10 → energy_production    (PVInstallation, OutPort, ENERGY)
#   category 26 → energy_export        (EConnection, OutPort, ENERGY)
#   category 27 → energy_import        (EConnection, InPort,  ENERGY)
#
# An energyasset in Smartnode corresponds to a Building containing an EConnection
# and (optionally) a PVInstallation. The wide row keeps the per-hour join cheap;
# gold splits this into two facts (f_smartnode_econnection_measurements,
# f_smartnode_pvinstallation_measurements) so each ESDL asset has its own table.
#
# Streaming + applyInPandasWithState mirrors AMPCORE's pattern: rows are emitted
# immediately (yield-based), watermark only manages state eviction. This avoids
# the append-mode emission lag that a vanilla streaming groupBy.pivot would
# inherit from a 7+ day watermark, while still giving the curated wide shape
# at silver instead of deferring the pivot to gold.
#
# Re-fetched bronze rows from the api2land 7-day lookback are deduped here:
# state tracks last_emitted_timestamp per (accountId, energyassetId), so a
# repeated bronze row with timestamp <= last_emitted is skipped.

from typing import Iterator, Tuple, cast

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
    StructField("last_timestamp", TimestampType(), True),
])

SILVER_OUTPUT_SCHEMA = StructType([
    StructField("account_id",         IntegerType(),   True),
    StructField("energyasset_id",     IntegerType(),   True),
    StructField("timestamp",          TimestampType(), True),
    StructField("energy_production",  DoubleType(),    True),
    StructField("energy_export",      DoubleType(),    True),
    StructField("energy_import",      DoubleType(),    True),
    StructField("_rescued_data",      StringType(),    True),
])

# ESDL-aligned column names per Smartnode energyassetcategory:
#   10 → energy_production   (PV generation, PVInstallation telemetry)
#   26 → energy_export       (grid feed-in, EConnection telemetry, OutPort)
#   27 → energy_import       (grid consumption, EConnection telemetry, InPort)
CATEGORY_COLUMN_MAP = {
    10: "energy_production",
    26: "energy_export",
    27: "energy_import",
}

# COMMAND ----------

def pivot_categories(
    key: Tuple,
    pdfs: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """
    Per-asset stateful transform that pivots Smartnode's long bronze rows
    (one row per category 10/26/27) into a wide silver row (one row per
    hour with three ESDL-aligned metric columns).

    Within each microbatch's pandas frame for the (accountId, energyassetId)
    key, rows are pivoted by timestamp. Any timestamp <= last_emitted is
    skipped — this absorbs re-fetched bronze rows from the api2land 7-day
    lookback without producing duplicate silver rows.

    Watermark eviction (8 days): if an asset goes silent for 8+ days, state
    is purged. The 8-day window matches the api2land lookback so backfilled
    rows up to a week late still pass.
    """
    if state.hasTimedOut:
        state.remove()
        return

    account_id, energyasset_id = key

    if state.exists:
        (last_timestamp,) = state.get
    else:
        last_timestamp = None

    for pdf in pdfs:
        if pdf.empty:
            continue

        if last_timestamp is not None:
            pdf = pdf[pdf["timestamp"] > last_timestamp]
        if pdf.empty:
            continue

        wide = (
            pdf.pivot_table(
                index="timestamp",
                columns="energyassetcategory",
                values="value",
                aggfunc="first",
            )
            .rename(columns=CATEGORY_COLUMN_MAP)
            .reset_index()
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        wide.columns.name = None
        for c in CATEGORY_COLUMN_MAP.values():
            if c not in wide.columns:
                wide[c] = None

        # Carry _rescued_data: any non-null value seen for that timestamp wins.
        rescued = (
            pdf.groupby("timestamp")["_rescued_data"]
               .apply(lambda s: next((v for v in s if v is not None), None))
               .reset_index()
        )
        wide = wide.merge(rescued, on="timestamp", how="left")

        wide["account_id"] = account_id
        wide["energyasset_id"] = energyasset_id

        out = cast(pd.DataFrame, wide[[f.name for f in SILVER_OUTPUT_SCHEMA.fields]])
        yield out

        last_timestamp = wide["timestamp"].max()

    state.update((last_timestamp,))
    if last_timestamp is not None:
        deadline_ms = int(pd.Timestamp(last_timestamp).timestamp() * 1000) + 8 * 24 * 60 * 60 * 1000
        state.setTimeoutTimestamp(deadline_ms)

# COMMAND ----------

@dp.table(
    name="silver.smartnode_measurements",
    comment=(
        "Silver layer for Smartnode hourly energy bundles, ESDL-aligned. One row "
        "per (accountId, energyassetId, timestamp) with three named electricity "
        "metric columns pivoted from bronze categories 10/26/27. "
        "energy_production (cat 10) belongs to a PVInstallation; "
        "energy_export (cat 26) and energy_import (cat 27) belong to the "
        "EConnection of the same energyasset. Gold splits these into two facts. "
        "Resolution is 60 minutes from the Smartnode hourly endpoint. "
        "Streaming + applyInPandasWithState — emits rows immediately; the "
        "8-day watermark only manages per-asset state eviction."
    ),
    cluster_by_auto=True,
    schema=StructType([
        StructField("account_id",                  IntegerType(),   True,  {"comment": "Smartnode account identifier (natural key component)"}),
        StructField("energyasset_id",              IntegerType(),   True,  {"comment": "Smartnode energy asset identifier — a Building containing an EConnection and optionally a PVInstallation"}),
        StructField("timestamp",                   TimestampType(), True,  {"comment": "UTC start of the hourly interval"}),
        StructField("energy_production",           DoubleType(),    True,  {"comment": "Electricity generated by PV in this hour (kWh); ESDL ENERGY OutPort, PVInstallation telemetry — from bronze category 10"}),
        StructField("energy_export",               DoubleType(),    True,  {"comment": "Electricity exported to the grid in this hour (kWh); ESDL ENERGY OutPort, EConnection telemetry — from bronze category 26"}),
        StructField("energy_import",               DoubleType(),    True,  {"comment": "Electricity imported from the grid in this hour (kWh); ESDL ENERGY InPort, EConnection telemetry — from bronze category 27"}),
        StructField("_rescued_data",               StringType(),    True,  {"comment": "Fields present in bronze that do not match the declared silver schema"}),
        StructField("silver_processing_timestamp", TimestampType(), True,  {"comment": "Timestamp when this row was written to silver"}),
    ]),
)
@dp.expect_or_fail("valid_account", "account_id IS NOT NULL")
@dp.expect_or_fail("valid_energyasset", "energyasset_id IS NOT NULL")
@dp.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dp.expect_or_drop(
    "at_least_one_metric",
    "energy_production IS NOT NULL OR energy_export IS NOT NULL OR energy_import IS NOT NULL",
)
@dp.expect("no_schema_drift", "_rescued_data IS NULL")
@dp.expect("non_negative_production", "energy_production IS NULL OR energy_production >= 0")
@dp.expect("non_negative_export",     "energy_export IS NULL OR energy_export >= 0")
@dp.expect("non_negative_import",     "energy_import IS NULL OR energy_import >= 0")
def smartnode_measurements():
    return (
        dp.read_stream("bronze.smartnode_data_batch")
        .filter(col("energyassetcategory").isin(10, 26, 27))
        .select(
            col("accountId"),
            col("energyassetId"),
            col("timestamp"),
            col("energyassetcategory"),
            col("value"),
            col("_rescued_data"),
        )
        .withWatermark("timestamp", "8 days")
        .groupBy("accountId", "energyassetId")
        .applyInPandasWithState(
            func=pivot_categories,
            outputStructType=SILVER_OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
        .withColumn("silver_processing_timestamp", current_timestamp())
    )
