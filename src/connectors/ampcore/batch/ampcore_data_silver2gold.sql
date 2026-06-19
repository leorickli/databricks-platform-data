-- Databricks notebook source
-- LDP SQL definition for the AMPCORE EConnection gold fact table.
-- Runs inside acme_ampcore_pipeline (see resources/acme_pipelines.yml).
--
-- Silver owns all complex PySpark logic (stateful interval derivation, dedup).
-- This notebook is intentionally a thin SQL projection: surrogate key, rounding,
-- and data quality constraints. normalized column names are inherited from
-- silver.ampcore_econnection_measurements.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold.f_abb_econnection_measurements (
  sk_econnection            STRING NOT NULL    COMMENT 'Surrogate key for the EConnection metering point (sha2-256 on gateway_id|object_id), stable across gateway IP changes',
  timestamp                 TIMESTAMP NOT NULL COMMENT 'UTC timestamp of the 30-second reading (cast from epoch seconds returned by the ABB SCU200 API)',
  event_date                DATE               COMMENT 'Calendar date derived from timestamp',
  gateway_id                STRING             COMMENT 'ABB SCU200 gateway identifier (original_id_in_source component)',
  object_id                 INT                COMMENT 'Sensor object ID within the SCU200 gateway (original_id_in_source component)',
  current_rms               DOUBLE             COMMENT 'AC RMS current (A); ESDL CURRENT, rounded to 2dp',
  current                   DOUBLE             COMMENT 'AC current component (A); ESDL CURRENT, rounded to 2dp',
  dc_current                DOUBLE             COMMENT 'DC current component (A); ESDL CURRENT InPort, rounded to 2dp',
  power                     DOUBLE             COMMENT 'AC active power (W); ESDL POWER, rounded to 2dp',
  energy_cumulative         DOUBLE             COMMENT 'Cumulative active energy counter (kWh); ESDL ENERGY — gap-resilient source of truth',
  energy_interval           DOUBLE             COMMENT 'Energy consumed in this 30-second interval, derived as a delta in the silver layer (kWh)',
  gold_processing_timestamp TIMESTAMP          COMMENT 'Timestamp when this row was written to gold',
  CONSTRAINT valid_sk                   EXPECT (sk_econnection IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_timestamp            EXPECT (timestamp IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_cumulative    EXPECT (energy_cumulative IS NULL OR energy_cumulative >= 0),
  CONSTRAINT reasonable_interval_energy EXPECT (energy_interval IS NULL OR energy_interval < 5),
  CONSTRAINT reasonable_active_power    EXPECT (power IS NULL OR ABS(power) < 1000000),
  CONSTRAINT pk_f_abb_econnection_measurements PRIMARY KEY (sk_econnection, timestamp)
)
CLUSTER BY AUTO
COMMENT
  'Gold fact for AMPCORE SCU200 CurrentSensor telemetry, normalized as EConnection
   metering-point measurements. One row per (sk_econnection, timestamp).
   Resolution is always 30s — enforced by silver expect_or_fail on resolution = 30s.
   energy_cumulative is the meter raw kWh counter (gap-resilient source of truth).
   energy_interval is the per-30s delta derived in silver.
   Join to gold.d_ampcore_econnections on sk_econnection for metering-point metadata
   (including direction = Import / Export).'
AS
SELECT
    sha2(concat_ws('|', gateway_id, cast(object_id AS STRING)), 256) AS sk_econnection,
    timestamp,
    to_date(timestamp)                   AS event_date,
    gateway_id,
    object_id,
    round(current_rms, 2)               AS current_rms,
    round(current, 2)                  AS current,
    round(dc_current, 2)                  AS dc_current,
    round(power, 2)              AS power,
    round(energy_cumulative, 2)         AS energy_cumulative,
    round(energy_interval, 2)           AS energy_interval,
    current_timestamp()                  AS gold_processing_timestamp
FROM STREAM(silver.ampcore_econnection_measurements)
