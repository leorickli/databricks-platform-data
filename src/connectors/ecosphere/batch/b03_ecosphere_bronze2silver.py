# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Setup Widgets and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")

CATALOG_NAME = dbutils.widgets.get("catalog_name")

# Bronze sources
BRONZE_HISTORY       = f"{CATALOG_NAME}.bronze.ecosphere_batch_history"
BRONZE_METERREADINGS = f"{CATALOG_NAME}.bronze.ecosphere_batch_meterreadings"
BRONZE_POINT_VALUE   = f"{CATALOG_NAME}.bronze.ecosphere_batch_point_value"
BRONZE_EV_SOCKET     = f"{CATALOG_NAME}.bronze.ecosphere_batch_ev_socket"

# Silver targets — ESDL ontology-aligned (snake_case field names per v2)
# From bronze.ecosphere_batch_history (split by energy carrier / ESDL asset type)
SILVER_ECONN_HISTORY  = f"{CATALOG_NAME}.silver.ecosphere_batch_econnection_history"
SILVER_HCONN_HISTORY  = f"{CATALOG_NAME}.silver.ecosphere_batch_hconnection_history"
SILVER_GCONN_HISTORY  = f"{CATALOG_NAME}.silver.ecosphere_batch_gconnection_history"
SILVER_WATER_HISTORY  = f"{CATALOG_NAME}.silver.ecosphere_batch_waterconnection_history"
SILVER_FCR_HISTORY    = f"{CATALOG_NAME}.silver.ecosphere_batch_fcr_history"

# From bronze.ecosphere_batch_meterreadings (split by energy carrier / ESDL asset type)
SILVER_ECONN_METER    = f"{CATALOG_NAME}.silver.ecosphere_batch_econnection_meterreadings"
SILVER_HCONN_METER    = f"{CATALOG_NAME}.silver.ecosphere_batch_hconnection_meterreadings"
SILVER_WATER_METER    = f"{CATALOG_NAME}.silver.ecosphere_batch_waterconnection_meterreadings"

# From bronze.ecosphere_batch_point_value (split by endpoint / ESDL asset type)
SILVER_BATTERY        = f"{CATALOG_NAME}.silver.ecosphere_batch_battery"
SILVER_SOLAR          = f"{CATALOG_NAME}.silver.ecosphere_batch_solar_irradiance"

# From bronze.ecosphere_batch_ev_socket
SILVER_EV             = f"{CATALOG_NAME}.silver.ecosphere_batch_ev_charging_station"

print(f"Bronze sources : {BRONZE_HISTORY}, {BRONZE_METERREADINGS}, {BRONZE_POINT_VALUE}, {BRONZE_EV_SOCKET}")
print(f"Silver targets : 11 tables across EConnection, HConnection, GConnection, WaterConnection, Battery, EV, FCR, Solar")

# COMMAND ----------

# DBTITLE 1,Create Silver Tables

# ---------------------------------------------------------------------------
# From bronze.ecosphere_history
# ---------------------------------------------------------------------------

# --- silver.ecosphere_econnection_history ---
# EConnection (site meter) — electricity 15-min intervals, all energy_methods pivoted to columns.
# energy_method values: delivery, return_delivery, production, consumption, charge, discharge.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_ECONN_HISTORY} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT '15-min interval timestamp',
    power_import            DOUBLE    COMMENT 'Grid import — delivery (W). ESDL: EConnection.power_import',
    power_export            DOUBLE    COMMENT 'Grid export — return_delivery (W). ESDL: EConnection.power_export',
    power_production     DOUBLE    COMMENT 'On-site solar PV generation — production (W). ESDL: PVInstallation.power_production',
    power_net            DOUBLE    COMMENT 'Net building consumption — consumption (W)',
    power_charge             DOUBLE    COMMENT 'Battery charging power — charge (W). ESDL: Battery.W (InPort)',
    power_discharge          DOUBLE    COMMENT 'Battery discharging power — discharge (W). ESDL: Battery.W (OutPort)',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'ESDL-aligned electricity history at 15-min granularity. All energy_methods pivoted to columns. One row per interval per building.'
""")

# --- silver.ecosphere_hconnection_history ---
# HConnection (district heating) — heat 15-min intervals.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_HCONN_HISTORY} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT '15-min interval timestamp',
    thermal_power        DOUBLE    COMMENT 'Heat power imported (W). ESDL: HConnection.thermal_power',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'ESDL-aligned heat history at 15-min granularity. One row per interval per building.'
""")

# --- silver.ecosphere_gconnection_history ---
# GConnection (gas) — gas history intervals.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_GCONN_HISTORY} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT 'Interval timestamp',
    gas_volume_consumption      DOUBLE    COMMENT 'Gas volume consumed (m3). ESDL: GConnection.gas_volume_consumption',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'ESDL-aligned gas history. One row per interval per building.'
""")

# --- silver.ecosphere_waterconnection_history ---
# WaterConnection — water history intervals.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_WATER_HISTORY} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT 'Interval timestamp',
    water_volume_consumption    DOUBLE    COMMENT 'Water volume consumed (m3). ESDL: WaterConnection.water_volume_consumption',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'ESDL-aligned water history. One row per interval per building.'
""")

# --- silver.ecosphere_fcr_history ---
# FCR (Frequency Containment Reserve) — no direct ESDL asset mapping, kept as-is.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_FCR_HISTORY} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT '15-min interval timestamp',
    fcr_value                 DOUBLE    COMMENT 'FCR registration value; Building telemetry (non-standard ESDL).',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'Ecosphere FCR history at 15-min granularity. No direct ESDL asset mapping.'
""")

# ---------------------------------------------------------------------------
# From bronze.ecosphere_meterreadings
# ---------------------------------------------------------------------------

# --- silver.ecosphere_econnection_meterreadings ---
# EConnection cumulative meter readings — electricity, pivoted by energy_method.
# energy_method values in bronze: delivery, return_delivery, consumption.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_ECONN_METER} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    sensor_uuid                STRING    COMMENT 'UUID of the individual meter/sensor',
    meter_name                 STRING    COMMENT 'Human-readable meter name (e.g. Hoofdmeter)',
    measurement_timestamp      TIMESTAMP COMMENT 'Timestamp of the meter reading (nullable for some meters)',
    energy_import             DOUBLE    COMMENT 'Cumulative delivery reading (kWh). ESDL: EConnection.energy_import',
    energy_export             DOUBLE    COMMENT 'Cumulative return_delivery reading (kWh). ESDL: EConnection.energy_export',
    energy_consumption        DOUBLE    COMMENT 'Cumulative consumption reading (kWh). ESDL: EConnection.energy_consumption',
    tariff                    STRING    COMMENT 'Tariff info — usually null; retained for future tariff-level split',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'ESDL-aligned electricity cumulative meter readings. energy_method pivoted to columns. One row per meter per timestamp.'
""")

# --- silver.ecosphere_hconnection_meterreadings ---
# HConnection cumulative meter readings — heat.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_HCONN_METER} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    sensor_uuid                STRING    COMMENT 'UUID of the individual meter/sensor',
    meter_name                 STRING    COMMENT 'Human-readable meter name',
    measurement_timestamp      TIMESTAMP COMMENT 'Timestamp of the meter reading (nullable for some meters)',
    thermal_energy_consumption  DOUBLE    COMMENT 'Cumulative heat energy consumed (GJ). ESDL: HConnection.thermal_energy_consumption',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'ESDL-aligned heat cumulative meter readings. One row per meter per timestamp.'
""")

# --- silver.ecosphere_waterconnection_meterreadings ---
# WaterConnection cumulative meter readings — water.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_WATER_METER} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    sensor_uuid                STRING    COMMENT 'UUID of the individual meter/sensor',
    meter_name                 STRING    COMMENT 'Human-readable meter name',
    measurement_timestamp      TIMESTAMP COMMENT 'Timestamp of the meter reading (nullable for some meters)',
    water_volume_consumption    DOUBLE    COMMENT 'Cumulative water volume consumed (m3). ESDL: WaterConnection.water_volume_consumption',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'ESDL-aligned water cumulative meter readings. One row per meter per timestamp.'
""")

# ---------------------------------------------------------------------------
# From bronze.ecosphere_point_value
# ---------------------------------------------------------------------------

# --- silver.ecosphere_battery ---
# Battery state of charge (from electricity_soc endpoint).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_BATTERY} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT 'Point-in-time timestamp',
    state_of_charge                       DOUBLE    COMMENT 'State of charge (%). ESDL: Battery.state_of_charge',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'ESDL-aligned battery state of charge. One row per API call per building.'
""")

# --- silver.ecosphere_solar_irradiance ---
# Solar irradiance (environmental measurement, no direct ESDL asset).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_SOLAR} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT 'Point-in-time timestamp',
    solar_irradiance           DOUBLE    COMMENT 'Solar radiation intensity (W/m2). Environmental measurement, no ESDL asset.',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'Ecosphere solar irradiance measurements. One row per API call per building.'
""")

# ---------------------------------------------------------------------------
# From bronze.ecosphere_ev_socket
# ---------------------------------------------------------------------------

# --- silver.ecosphere_ev_charging_station ---
# EV charger socket status (status-only; no energy telemetry available from Ecosphere API).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_EV} (
    building_uuid              STRING    COMMENT 'Ecosphere building UUID',
    building_name              STRING    COMMENT 'Human-readable building name',
    sensor_uuid                STRING    COMMENT 'UUID of the individual EV charger socket',
    measurement_date           DATE      COMMENT 'Date of the status reading',
    measurement_timestamp      TIMESTAMP COMMENT 'Timestamp of the status reading',
    is_available               BOOLEAN   COMMENT 'True if socket is available for charging. ESDL: EVChargingStation',
    is_in_session               BOOLEAN   COMMENT 'True if socket has an active charging session. ESDL: EVChargingStation',
    silver_processing_timestamp TIMESTAMP COMMENT 'When this record was written to silver'
)
CLUSTER BY AUTO
COMMENT 'ESDL-aligned EV charger socket status. Status-only (no power/energy data available from Ecosphere API).'
""")

print("All 11 silver tables created/verified.")

# COMMAND ----------

# DBTITLE 1,Helper: Get Max Timestamp for Incremental Processing
def get_max_bronze_ts(silver_table):
    """Returns the max silver_processing_timestamp already in a silver table, or None for initial load."""
    try:
        result = spark.sql(f"SELECT MAX(silver_processing_timestamp) as max_ts FROM {silver_table}").collect()[0]
        return result.max_ts
    except Exception:
        return None

# COMMAND ----------

# DBTITLE 1,Read and Cache Bronze Tables
# Cache each bronze DataFrame once to avoid re-reading per energy type filter.

history_df_all = spark.read.table(BRONZE_HISTORY)
history_df_all.cache()

meter_df_all = spark.read.table(BRONZE_METERREADINGS)
meter_df_all.cache()

point_df_all = spark.read.table(BRONZE_POINT_VALUE)
point_df_all.cache()

ev_df_all = spark.read.table(BRONZE_EV_SOCKET)
ev_df_all.cache()

print("Bronze tables cached.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze History -> Silver EConnection History (Electricity, Pivoted)
# Pivot all 6 energy_method variants into separate columns.
# delivery        -> power_import       (ESDL: EConnection.power_import)
# return_delivery -> power_export       (ESDL: EConnection.power_export)
# production      -> power_production (ESDL: PVInstallation.power_production)
# consumption     -> power_net
# charge          -> power_charge        (ESDL: Battery.W InPort)
# discharge       -> power_discharge     (ESDL: Battery.W OutPort)

max_ts = get_max_bronze_ts(SILVER_ECONN_HISTORY)
print(f"Latest silver_processing_timestamp in ecosphere_econnection_history: {max_ts or 'none (initial load)'}")

history_df = history_df_all
if max_ts:
    history_df = history_df.filter(F.col("bronze_processing_timestamp") > max_ts)

# Extract energy_type from endpoint_name then filter to electricity only
history_df = history_df.withColumn(
    "energy_type",
    F.when(F.col("endpoint_name") == "fcr_history_quarter", F.lit("fcr"))
     .otherwise(F.regexp_extract(F.col("endpoint_name"), r"^(\w+)_history", 1))
)

elec_history_df = history_df.filter(F.col("energy_type") == "electricity")
new_rows = elec_history_df.count()
print(f"Bronze electricity history rows to process: {new_rows:,}")

if new_rows > 0:
    silver_econn_history_df = (
        elec_history_df
        .withColumn("power_import",        F.when(F.col("energy_method") == "delivery",        F.col("value").cast("double")))
        .withColumn("power_export",        F.when(F.col("energy_method") == "return_delivery", F.col("value").cast("double")))
        .withColumn("power_production", F.when(F.col("energy_method") == "production",      F.col("value").cast("double")))
        .withColumn("power_net",        F.when(F.col("energy_method") == "consumption",     F.col("value").cast("double")))
        .withColumn("power_charge",         F.when(F.col("energy_method") == "charge",          F.col("value").cast("double")))
        .withColumn("power_discharge",      F.when(F.col("energy_method") == "discharge",       F.col("value").cast("double")))
        .groupBy("building_uuid", "building_name", "measurement_date", "measurement_timestamp")
        .agg(
            F.max("power_import").alias("power_import"),
            F.max("power_export").alias("power_export"),
            F.max("power_production").alias("power_production"),
            F.max("power_net").alias("power_net"),
            F.max("power_charge").alias("power_charge"),
            F.max("power_discharge").alias("power_discharge"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
        .select(
            F.col("building_uuid").alias("building_uuid"),
            F.col("building_name").alias("building_name"),
            F.to_date(F.col("measurement_date"), "yyyy-MM-dd").alias("measurement_date"),
            F.col("measurement_timestamp").alias("measurement_timestamp"),
            F.col("power_import"),
            F.col("power_export"),
            F.col("power_production"),
            F.col("power_net"),
            F.col("power_charge"),
            F.col("power_discharge"),
            F.col("silver_processing_timestamp"),
        )
    )
    silver_econn_history_df.write.mode("append").saveAsTable(SILVER_ECONN_HISTORY)
    print(f"OK: Appended {silver_econn_history_df.count():,} rows to {SILVER_ECONN_HISTORY}")
else:
    print("No new electricity history data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze History -> Silver HConnection History (Heat)
max_ts = get_max_bronze_ts(SILVER_HCONN_HISTORY)
print(f"Latest silver_processing_timestamp in ecosphere_hconnection_history: {max_ts or 'none (initial load)'}")

heat_history_df = history_df.filter(F.col("energy_type") == "heat")
if max_ts:
    heat_history_df = heat_history_df.filter(F.col("bronze_processing_timestamp") > max_ts)

new_rows = heat_history_df.count()
print(f"Bronze heat history rows to process: {new_rows:,}")

if new_rows > 0:
    silver_hconn_history_df = (
        heat_history_df
        .select(
            F.col("building_uuid").alias("building_uuid"),
            F.col("building_name").alias("building_name"),
            F.to_date(F.col("measurement_date"), "yyyy-MM-dd").alias("measurement_date"),
            F.col("measurement_timestamp").alias("measurement_timestamp"),
            F.col("value").cast("double").alias("thermal_power"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
        .filter(F.col("thermal_power").isNotNull())
    )
    silver_hconn_history_df.write.mode("append").saveAsTable(SILVER_HCONN_HISTORY)
    print(f"OK: Appended {silver_hconn_history_df.count():,} rows to {SILVER_HCONN_HISTORY}")
else:
    print("No new heat history data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze History -> Silver GConnection History (Gas)
max_ts = get_max_bronze_ts(SILVER_GCONN_HISTORY)
print(f"Latest silver_processing_timestamp in ecosphere_gconnection_history: {max_ts or 'none (initial load)'}")

gas_history_df = history_df.filter(F.col("energy_type") == "gas")
if max_ts:
    gas_history_df = gas_history_df.filter(F.col("bronze_processing_timestamp") > max_ts)

new_rows = gas_history_df.count()
print(f"Bronze gas history rows to process: {new_rows:,}")

if new_rows > 0:
    silver_gconn_history_df = (
        gas_history_df
        .select(
            F.col("building_uuid").alias("building_uuid"),
            F.col("building_name").alias("building_name"),
            F.to_date(F.col("measurement_date"), "yyyy-MM-dd").alias("measurement_date"),
            F.col("measurement_timestamp").alias("measurement_timestamp"),
            F.col("value").cast("double").alias("gas_volume_consumption"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
        .filter(F.col("gas_volume_consumption").isNotNull())
    )
    silver_gconn_history_df.write.mode("append").saveAsTable(SILVER_GCONN_HISTORY)
    print(f"OK: Appended {silver_gconn_history_df.count():,} rows to {SILVER_GCONN_HISTORY}")
else:
    print("No new gas history data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze History -> Silver WaterConnection History (Water)
max_ts = get_max_bronze_ts(SILVER_WATER_HISTORY)
print(f"Latest silver_processing_timestamp in ecosphere_waterconnection_history: {max_ts or 'none (initial load)'}")

water_history_df = history_df.filter(F.col("energy_type") == "water")
if max_ts:
    water_history_df = water_history_df.filter(F.col("bronze_processing_timestamp") > max_ts)

new_rows = water_history_df.count()
print(f"Bronze water history rows to process: {new_rows:,}")

if new_rows > 0:
    silver_water_history_df = (
        water_history_df
        .select(
            F.col("building_uuid").alias("building_uuid"),
            F.col("building_name").alias("building_name"),
            F.to_date(F.col("measurement_date"), "yyyy-MM-dd").alias("measurement_date"),
            F.col("measurement_timestamp").alias("measurement_timestamp"),
            F.col("value").cast("double").alias("water_volume_consumption"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
        .filter(F.col("water_volume_consumption").isNotNull())
    )
    silver_water_history_df.write.mode("append").saveAsTable(SILVER_WATER_HISTORY)
    print(f"OK: Appended {silver_water_history_df.count():,} rows to {SILVER_WATER_HISTORY}")
else:
    print("No new water history data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze History -> Silver FCR History
max_ts = get_max_bronze_ts(SILVER_FCR_HISTORY)
print(f"Latest silver_processing_timestamp in ecosphere_fcr_history: {max_ts or 'none (initial load)'}")

fcr_history_df = history_df.filter(F.col("energy_type") == "fcr")
if max_ts:
    fcr_history_df = fcr_history_df.filter(F.col("bronze_processing_timestamp") > max_ts)

new_rows = fcr_history_df.count()
print(f"Bronze FCR history rows to process: {new_rows:,}")

if new_rows > 0:
    silver_fcr_history_df = (
        fcr_history_df
        .select(
            F.col("building_uuid").alias("building_uuid"),
            F.col("building_name").alias("building_name"),
            F.to_date(F.col("measurement_date"), "yyyy-MM-dd").alias("measurement_date"),
            F.col("measurement_timestamp").alias("measurement_timestamp"),
            F.col("value").cast("double").alias("fcr_value"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
        .filter(F.col("fcr_value").isNotNull())
    )
    silver_fcr_history_df.write.mode("append").saveAsTable(SILVER_FCR_HISTORY)
    print(f"OK: Appended {silver_fcr_history_df.count():,} rows to {SILVER_FCR_HISTORY}")
else:
    print("No new FCR history data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze Meterreadings -> Silver EConnection Meterreadings (Electricity, Pivoted)
# Pivot energy_method variants into columns:
# delivery        -> power_import       (ESDL: EConnection.power_import)
# return_delivery -> power_export       (ESDL: EConnection.power_export)
# consumption     -> energy_consumption

max_ts = get_max_bronze_ts(SILVER_ECONN_METER)
print(f"Latest silver_processing_timestamp in ecosphere_econnection_meterreadings: {max_ts or 'none (initial load)'}")

meter_df = meter_df_all
if max_ts:
    meter_df = meter_df.filter(F.col("bronze_processing_timestamp") > max_ts)

meter_df = meter_df.withColumn(
    "energy_type",
    F.regexp_extract(F.col("endpoint_name"), r"^(\w+)_meterreadings$", 1)
)

elec_meter_df = meter_df.filter(F.col("energy_type") == "electricity")
new_rows = elec_meter_df.count()
print(f"Bronze electricity meterreadings rows to process: {new_rows:,}")

if new_rows > 0:
    silver_econn_meter_df = (
        elec_meter_df
        .withColumn("energy_import",      F.when(F.col("energy_method") == "delivery",        F.col("value").cast("double")))
        .withColumn("energy_export",      F.when(F.col("energy_method") == "return_delivery", F.col("value").cast("double")))
        .withColumn("energy_consumption", F.when(F.col("energy_method") == "consumption",     F.col("value").cast("double")))
        .groupBy("building_uuid", "building_name", "sensor_uuid", "meter_name", "measurement_timestamp")
        .agg(
            F.max("energy_import").alias("energy_import"),
            F.max("energy_export").alias("energy_export"),
            F.max("energy_consumption").alias("energy_consumption"),
            F.max("tariff").alias("tariff"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
        .select(
            F.col("building_uuid"),
            F.col("building_name"),
            F.col("sensor_uuid"),
            F.col("meter_name"),
            F.col("measurement_timestamp"),
            F.col("energy_import"),
            F.col("energy_export"),
            F.col("energy_consumption"),
            F.col("tariff"),
            F.col("silver_processing_timestamp"),
        )
    )
    silver_econn_meter_df.write.mode("append").saveAsTable(SILVER_ECONN_METER)
    print(f"OK: Appended {silver_econn_meter_df.count():,} rows to {SILVER_ECONN_METER}")
else:
    print("No new electricity meterreadings data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze Meterreadings -> Silver HConnection Meterreadings (Heat)
max_ts = get_max_bronze_ts(SILVER_HCONN_METER)
print(f"Latest silver_processing_timestamp in ecosphere_hconnection_meterreadings: {max_ts or 'none (initial load)'}")

heat_meter_df = meter_df.filter(F.col("energy_type") == "heat")
if max_ts:
    heat_meter_df = heat_meter_df.filter(F.col("bronze_processing_timestamp") > max_ts)

new_rows = heat_meter_df.count()
print(f"Bronze heat meterreadings rows to process: {new_rows:,}")

if new_rows > 0:
    silver_hconn_meter_df = (
        heat_meter_df
        .select(
            F.col("building_uuid").alias("building_uuid"),
            F.col("building_name").alias("building_name"),
            F.col("sensor_uuid").alias("sensor_uuid"),
            F.col("meter_name").alias("meter_name"),
            F.col("measurement_timestamp").alias("measurement_timestamp"),
            F.col("value").cast("double").alias("thermal_energy_consumption"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
    )
    silver_hconn_meter_df.write.mode("append").saveAsTable(SILVER_HCONN_METER)
    print(f"OK: Appended {silver_hconn_meter_df.count():,} rows to {SILVER_HCONN_METER}")
else:
    print("No new heat meterreadings data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze Meterreadings -> Silver WaterConnection Meterreadings (Water)
max_ts = get_max_bronze_ts(SILVER_WATER_METER)
print(f"Latest silver_processing_timestamp in ecosphere_waterconnection_meterreadings: {max_ts or 'none (initial load)'}")

water_meter_df = meter_df.filter(F.col("energy_type") == "water")
if max_ts:
    water_meter_df = water_meter_df.filter(F.col("bronze_processing_timestamp") > max_ts)

new_rows = water_meter_df.count()
print(f"Bronze water meterreadings rows to process: {new_rows:,}")

if new_rows > 0:
    silver_water_meter_df = (
        water_meter_df
        .select(
            F.col("building_uuid").alias("building_uuid"),
            F.col("building_name").alias("building_name"),
            F.col("sensor_uuid").alias("sensor_uuid"),
            F.col("meter_name").alias("meter_name"),
            F.col("measurement_timestamp").alias("measurement_timestamp"),
            F.col("value").cast("double").alias("water_volume_consumption"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
    )
    silver_water_meter_df.write.mode("append").saveAsTable(SILVER_WATER_METER)
    print(f"OK: Appended {silver_water_meter_df.count():,} rows to {SILVER_WATER_METER}")
else:
    print("No new water meterreadings data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze Point Value -> Silver Battery (electricity_soc)
max_ts = get_max_bronze_ts(SILVER_BATTERY)
print(f"Latest silver_processing_timestamp in ecosphere_battery: {max_ts or 'none (initial load)'}")

battery_df = point_df_all.filter(F.col("endpoint_name") == "electricity_soc")
if max_ts:
    battery_df = battery_df.filter(F.col("bronze_processing_timestamp") > max_ts)

new_rows = battery_df.count()
print(f"Bronze electricity_soc rows to process: {new_rows:,}")

if new_rows > 0:
    silver_battery_df = (
        battery_df
        .select(
            F.col("building_uuid").alias("building_uuid"),
            F.col("building_name").alias("building_name"),
            F.to_date(F.col("measurement_date"), "yyyy-MM-dd").alias("measurement_date"),
            F.col("measurement_timestamp").alias("measurement_timestamp"),
            F.col("value").cast("double").alias("state_of_charge"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
        .filter(F.col("state_of_charge").isNotNull())
    )
    silver_battery_df.write.mode("append").saveAsTable(SILVER_BATTERY)
    print(f"OK: Appended {silver_battery_df.count():,} rows to {SILVER_BATTERY}")
else:
    print("No new battery SOC data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze Point Value -> Silver Solar Irradiance
max_ts = get_max_bronze_ts(SILVER_SOLAR)
print(f"Latest silver_processing_timestamp in ecosphere_solar_irradiance: {max_ts or 'none (initial load)'}")

solar_df = point_df_all.filter(F.col("endpoint_name") == "solar_irradiance")
if max_ts:
    solar_df = solar_df.filter(F.col("bronze_processing_timestamp") > max_ts)

new_rows = solar_df.count()
print(f"Bronze solar_irradiance rows to process: {new_rows:,}")

if new_rows > 0:
    silver_solar_df = (
        solar_df
        .select(
            F.col("building_uuid").alias("building_uuid"),
            F.col("building_name").alias("building_name"),
            F.to_date(F.col("measurement_date"), "yyyy-MM-dd").alias("measurement_date"),
            F.col("measurement_timestamp").alias("measurement_timestamp"),
            F.col("value").cast("double").alias("solar_irradiance"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
        .filter(F.col("solar_irradiance").isNotNull())
    )
    silver_solar_df.write.mode("append").saveAsTable(SILVER_SOLAR)
    print(f"OK: Appended {silver_solar_df.count():,} rows to {SILVER_SOLAR}")
else:
    print("No new solar irradiance data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Bronze EV Socket -> Silver EV Charging Station
max_ts = get_max_bronze_ts(SILVER_EV)
print(f"Latest silver_processing_timestamp in ecosphere_ev_charging_station: {max_ts or 'none (initial load)'}")

ev_df = ev_df_all
if max_ts:
    ev_df = ev_df.filter(F.col("bronze_processing_timestamp") > max_ts)

new_rows = ev_df.count()
print(f"Bronze ev_socket rows to process: {new_rows:,}")

if new_rows > 0:
    silver_ev_df = (
        ev_df
        .select(
            F.col("building_uuid").alias("building_uuid"),
            F.col("building_name").alias("building_name"),
            F.col("sensor_uuid").alias("sensor_uuid"),
            F.to_date(F.col("measurement_date"), "yyyy-MM-dd").alias("measurement_date"),
            F.col("measurement_timestamp").alias("measurement_timestamp"),
            (F.col("available") == "yes").alias("is_available"),
            (F.col("in_session") == "yes").alias("is_in_session"),
            F.current_timestamp().alias("silver_processing_timestamp"),
        )
        .filter(F.col("sensor_uuid").isNotNull())
    )
    silver_ev_df.write.mode("append").saveAsTable(SILVER_EV)
    print(f"OK: Appended {silver_ev_df.count():,} rows to {SILVER_EV}")
else:
    print("No new EV socket data to process.")

# COMMAND ----------

# DBTITLE 1,Unpersist Cached Bronze Tables
history_df_all.unpersist()
meter_df_all.unpersist()
point_df_all.unpersist()
ev_df_all.unpersist()

# COMMAND ----------

# DBTITLE 1,Silver Layer Summary
print(f"\n{'='*60}")
print(f"SILVER LAYER SUMMARY (ESDL-Aligned)")
print(f"{'='*60}")

silver_tables = {
    "ecosphere_batch_econnection_history":          SILVER_ECONN_HISTORY,
    "ecosphere_batch_hconnection_history":          SILVER_HCONN_HISTORY,
    "ecosphere_batch_gconnection_history":          SILVER_GCONN_HISTORY,
    "ecosphere_batch_waterconnection_history":      SILVER_WATER_HISTORY,
    "ecosphere_batch_fcr_history":                  SILVER_FCR_HISTORY,
    "ecosphere_batch_econnection_meterreadings":    SILVER_ECONN_METER,
    "ecosphere_batch_hconnection_meterreadings":    SILVER_HCONN_METER,
    "ecosphere_batch_waterconnection_meterreadings":SILVER_WATER_METER,
    "ecosphere_batch_battery":                      SILVER_BATTERY,
    "ecosphere_batch_solar_irradiance":             SILVER_SOLAR,
    "ecosphere_batch_ev_charging_station":          SILVER_EV,
}

for label, table_path in silver_tables.items():
    try:
        total = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_path}").collect()[0]["cnt"]
        print(f"\n  {label}: {total:,} total rows")
    except Exception as e:
        print(f"  ERROR reading {table_path}: {e}")
