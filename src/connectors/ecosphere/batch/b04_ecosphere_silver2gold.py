# Databricks notebook source
# DBTITLE 1,Imports and Configuration
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Setup Widgets and Configuration
dbutils.widgets.text("catalog_name", "", "Catalog Name")

CATALOG_NAME = dbutils.widgets.get("catalog_name")

# Silver sources
SILVER_ECONN_HISTORY  = f"{CATALOG_NAME}.silver.ecosphere_batch_econnection_history"
SILVER_HCONN_HISTORY  = f"{CATALOG_NAME}.silver.ecosphere_batch_hconnection_history"
SILVER_GCONN_HISTORY  = f"{CATALOG_NAME}.silver.ecosphere_batch_gconnection_history"
SILVER_WATER_HISTORY  = f"{CATALOG_NAME}.silver.ecosphere_batch_waterconnection_history"
SILVER_FCR_HISTORY    = f"{CATALOG_NAME}.silver.ecosphere_batch_fcr_history"
SILVER_ECONN_METER    = f"{CATALOG_NAME}.silver.ecosphere_batch_econnection_meterreadings"
SILVER_HCONN_METER    = f"{CATALOG_NAME}.silver.ecosphere_batch_hconnection_meterreadings"
SILVER_WATER_METER    = f"{CATALOG_NAME}.silver.ecosphere_batch_waterconnection_meterreadings"
SILVER_BATTERY        = f"{CATALOG_NAME}.silver.ecosphere_batch_battery"
SILVER_SOLAR          = f"{CATALOG_NAME}.silver.ecosphere_batch_solar_irradiance"
SILVER_EV             = f"{CATALOG_NAME}.silver.ecosphere_batch_ev_charging_station"

# Gold targets — ESDL-aligned fact tables with surrogate foreign keys
GOLD_ECONN_HISTORY    = f"{CATALOG_NAME}.gold.f_ecosphere_econnection_history"
GOLD_HCONN_HISTORY    = f"{CATALOG_NAME}.gold.f_ecosphere_hconnection_history"
GOLD_GCONN_HISTORY    = f"{CATALOG_NAME}.gold.f_ecosphere_gconnection_history"
GOLD_WATER_HISTORY    = f"{CATALOG_NAME}.gold.f_ecosphere_waterconnection_history"
GOLD_FCR_HISTORY      = f"{CATALOG_NAME}.gold.f_ecosphere_fcr_history"
GOLD_ECONN_METER      = f"{CATALOG_NAME}.gold.f_ecosphere_econnection_meterreadings"
GOLD_HCONN_METER      = f"{CATALOG_NAME}.gold.f_ecosphere_hconnection_meterreadings"
GOLD_WATER_METER      = f"{CATALOG_NAME}.gold.f_ecosphere_waterconnection_meterreadings"
GOLD_BATTERY          = f"{CATALOG_NAME}.gold.f_ecosphere_battery"
GOLD_SOLAR            = f"{CATALOG_NAME}.gold.f_ecosphere_solar_irradiance"
GOLD_EV               = f"{CATALOG_NAME}.gold.f_ecosphere_ev_charging_station"

print(f"Silver sources : 11 tables in {CATALOG_NAME}.silver")
print(f"Gold targets   : 11 fact tables in {CATALOG_NAME}.gold")

# COMMAND ----------

# DBTITLE 1,Create Gold Fact Tables
# Note: the `gold` schema is Terraform-managed in dataplatformx-infra; this notebook never creates it.

# --- gold.f_ecosphere_econnection_history ---
# Electricity 15-min interval fact table.
# sk_building FK to d_ecosphere_buildings, sk_econnection FK to d_ecosphere_econnections.
# sk_econnection is an artificial key: MD5(CONCAT(building_uuid, '_econnection')).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_ECONN_HISTORY} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    sk_econnection             STRING    COMMENT 'Surrogate key — MD5(CONCAT(building_uuid, "_econnection")). FK to d_ecosphere_econnections.sk_econnection',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT '15-min interval timestamp',
    power_import            DOUBLE    COMMENT 'Grid import — delivery (W). ESDL: EConnection.power_import',
    power_export            DOUBLE    COMMENT 'Grid export — return_delivery (W). ESDL: EConnection.power_export',
    power_production     DOUBLE    COMMENT 'On-site solar PV generation (W). ESDL: PVInstallation.power_production',
    power_net            DOUBLE    COMMENT 'Net building consumption (W)',
    power_charge             DOUBLE    COMMENT 'Battery charging power (W). ESDL: Battery.W (InPort)',
    power_discharge          DOUBLE    COMMENT 'Battery discharging power (W). ESDL: Battery.W (OutPort)',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — electricity 15-min intervals. sk_building FK to d_ecosphere_buildings, sk_econnection FK to d_ecosphere_econnections.'
""")

# --- gold.f_ecosphere_hconnection_history ---
# Heat 15-min interval fact table.
# sk_hconnection is an artificial key: MD5(CONCAT(building_uuid, '_hconnection')).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_HCONN_HISTORY} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    sk_hconnection             STRING    COMMENT 'Surrogate key — MD5(CONCAT(building_uuid, "_hconnection")). FK to d_ecosphere_hconnections.sk_hconnection',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT '15-min interval timestamp',
    thermal_power        DOUBLE    COMMENT 'Heat power imported (W). ESDL: HConnection.thermal_power',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — heat 15-min intervals. sk_building FK to d_ecosphere_buildings, sk_hconnection FK to d_ecosphere_hconnections.'
""")

# --- gold.f_ecosphere_gconnection_history ---
# Gas interval fact table.
# sk_gconnection is an artificial key: MD5(CONCAT(building_uuid, '_gconnection')).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_GCONN_HISTORY} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    sk_gconnection             STRING    COMMENT 'Surrogate key — MD5(CONCAT(building_uuid, "_gconnection")). FK to d_ecosphere_gconnections.sk_gconnection',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT 'Interval timestamp',
    gas_volume_consumption      DOUBLE    COMMENT 'Gas volume consumed (m3). ESDL: GConnection.gas_volume_consumption',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — gas intervals. sk_building FK to d_ecosphere_buildings, sk_gconnection FK to d_ecosphere_gconnections.'
""")

# --- gold.f_ecosphere_waterconnection_history ---
# Water interval fact table.
# sk_waterconnection is an artificial key: MD5(CONCAT(building_uuid, '_waterconnection')).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_WATER_HISTORY} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    sk_waterconnection         STRING    COMMENT 'Surrogate key — MD5(CONCAT(building_uuid, "_waterconnection")). FK to d_ecosphere_waterconnections.sk_waterconnection',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT 'Interval timestamp',
    water_volume_consumption    DOUBLE    COMMENT 'Water volume consumed (m3). ESDL: WaterConnection.water_volume_consumption',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — water intervals. sk_building FK to d_ecosphere_buildings, sk_waterconnection FK to d_ecosphere_waterconnections.'
""")

# --- gold.f_ecosphere_fcr_history ---
# FCR interval fact table. No ESDL device dimension; sk_building is the only FK.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FCR_HISTORY} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT '15-min interval timestamp',
    fcr_value                 DOUBLE    COMMENT 'FCR registration value; Building telemetry (non-standard ESDL).',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — FCR 15-min intervals. sk_building FK to d_ecosphere_buildings.'
""")

# --- gold.f_ecosphere_econnection_meterreadings ---
# Electricity cumulative meter readings fact table.
# sk_econnection links to d_ecosphere_econnections.sk_econnection via MD5(sensor_uuid).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_ECONN_METER} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    sk_econnection             STRING    COMMENT 'Surrogate key — MD5(sensor_uuid). FK to d_ecosphere_econnections.sk_econnection',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    sensor_uuid                STRING    COMMENT 'UUID of the individual meter/sensor (natural key)',
    meter_name                 STRING    COMMENT 'Human-readable meter name',
    measurement_timestamp      TIMESTAMP COMMENT 'Timestamp of the meter reading',
    energy_import             DOUBLE    COMMENT 'Cumulative delivery reading (kWh). ESDL: EConnection.energy_import',
    energy_export             DOUBLE    COMMENT 'Cumulative return_delivery reading (kWh). ESDL: EConnection.energy_export',
    energy_consumption        DOUBLE    COMMENT 'Cumulative consumption reading (kWh). ESDL: EConnection.energy_consumption',
    tariff                    STRING    COMMENT 'Tariff info (usually null)',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — electricity cumulative meter readings. sk_building FK to d_ecosphere_buildings, sk_econnection FK to d_ecosphere_econnections.'
""")

# --- gold.f_ecosphere_hconnection_meterreadings ---
# Heat cumulative meter readings fact table.
# sk_hconnection links to d_ecosphere_hconnections.sk_hconnection via MD5(sensor_uuid).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_HCONN_METER} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    sk_hconnection             STRING    COMMENT 'Surrogate key — MD5(sensor_uuid). FK to d_ecosphere_hconnections.sk_hconnection',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    sensor_uuid                STRING    COMMENT 'UUID of the individual meter/sensor (natural key)',
    meter_name                 STRING    COMMENT 'Human-readable meter name',
    measurement_timestamp      TIMESTAMP COMMENT 'Timestamp of the meter reading',
    thermal_energy_consumption  DOUBLE    COMMENT 'Cumulative heat energy consumed (GJ). ESDL: HConnection.thermal_energy_consumption',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — heat cumulative meter readings. sk_building FK to d_ecosphere_buildings, sk_hconnection FK to d_ecosphere_hconnections.'
""")

# --- gold.f_ecosphere_waterconnection_meterreadings ---
# Water cumulative meter readings fact table.
# sk_waterconnection links to d_ecosphere_waterconnections.sk_waterconnection via MD5(sensor_uuid).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_WATER_METER} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    sk_waterconnection         STRING    COMMENT 'Surrogate key — MD5(sensor_uuid). FK to d_ecosphere_waterconnections.sk_waterconnection',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    sensor_uuid                STRING    COMMENT 'UUID of the individual meter/sensor (natural key)',
    meter_name                 STRING    COMMENT 'Human-readable meter name',
    measurement_timestamp      TIMESTAMP COMMENT 'Timestamp of the meter reading',
    water_volume_consumption    DOUBLE    COMMENT 'Cumulative water volume consumed (m3). ESDL: WaterConnection.water_volume_consumption',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — water cumulative meter readings. sk_building FK to d_ecosphere_buildings, sk_waterconnection FK to d_ecosphere_waterconnections.'
""")

# --- gold.f_ecosphere_battery ---
# Battery 15-min interval fact table.
# Granularity driven by /electricity/history/day/ (charge + discharge energy_methods) — 96 rows/building/day.
# state_of_charge comes from /electricity/capacity/actual/soc/ (1 reading/day) and is NULL for most 15-min intervals.
# sk_battery is an artificial key: MD5(CONCAT(building_uuid, '_battery')).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_BATTERY} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    sk_battery                 STRING    COMMENT 'Surrogate key — MD5(CONCAT(building_uuid, "_battery")). FK to d_ecosphere_batteries.sk_battery',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT '15-min interval timestamp',
    state_of_charge                       DOUBLE    COMMENT 'State of charge (%) — point-in-time daily snapshot; NULL for most 15-min intervals. ESDL: Battery.state_of_charge',
    power_charge            DOUBLE    COMMENT 'Battery charging power (W). ESDL: Battery.W (InPort)',
    power_discharge         DOUBLE    COMMENT 'Battery discharging power (W). ESDL: Battery.W (OutPort)',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — battery 15-min intervals. sk_building FK to d_ecosphere_buildings, sk_battery FK to d_ecosphere_batteries.'
""")

# Add new columns to existing deployments (idempotent — silently skips if column already exists).
for col_ddl in [
    "power_charge DOUBLE COMMENT 'Battery charging power (W). ESDL: Battery.W (InPort)'",
    "power_discharge DOUBLE COMMENT 'Battery discharging power (W). ESDL: Battery.W (OutPort)'",
]:
    try:
        spark.sql(f"ALTER TABLE {GOLD_BATTERY} ADD COLUMN {col_ddl}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Column already exists, skipping: {col_ddl.split()[0]}")
        else:
            raise

# --- gold.f_ecosphere_solar_irradiance ---
# Solar irradiance fact table. No ESDL device dimension; sk_building is the only FK.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_SOLAR} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    measurement_date           DATE      COMMENT 'Date of the measurement',
    measurement_timestamp      TIMESTAMP COMMENT 'Point-in-time timestamp',
    solar_irradiance           DOUBLE    COMMENT 'Solar radiation intensity (W/m2). Environmental measurement.',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — solar irradiance. sk_building FK to d_ecosphere_buildings.'
""")

# --- gold.f_ecosphere_ev_charging_station ---
# EV charger socket status fact table.
# sk_evchargingstation links to d_ecosphere_ev_charging_stations.sk_evchargingstation via MD5(sensor_uuid).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_EV} (
    sk_building                STRING    COMMENT 'Surrogate key — MD5(building_uuid). FK to d_ecosphere_buildings.sk_building',
    sk_evchargingstation       STRING    COMMENT 'Surrogate key — MD5(sensor_uuid). FK to d_ecosphere_ev_charging_stations.sk_evchargingstation',
    building_uuid              STRING    COMMENT 'Ecosphere building UUID (natural key)',
    building_name              STRING    COMMENT 'Human-readable building name',
    sensor_uuid                STRING    COMMENT 'UUID of the individual EV charger socket (natural key)',
    measurement_date           DATE      COMMENT 'Date of the status reading',
    measurement_timestamp      TIMESTAMP COMMENT 'Timestamp of the status reading',
    is_available               BOOLEAN   COMMENT 'True if socket is available for charging',
    is_in_session               BOOLEAN   COMMENT 'True if socket has an active charging session',
    silver_processing_timestamp TIMESTAMP COMMENT 'Processing timestamp inherited from silver'
)
CLUSTER BY AUTO
COMMENT 'Gold fact table — EV charger socket status. sk_building FK to d_ecosphere_buildings, sk_evchargingstation FK to d_ecosphere_ev_charging_stations.'
""")

print("All 11 gold fact tables created/verified.")

# COMMAND ----------

# DBTITLE 1,Helper: Get Max Timestamp for Incremental Processing
def get_max_silver_ts(gold_table):
    """Returns the max silver_processing_timestamp already in a gold table, or None for initial load."""
    try:
        result = spark.sql(f"SELECT MAX(silver_processing_timestamp) as max_ts FROM {gold_table}").collect()[0]
        return result.max_ts
    except Exception:
        return None

# COMMAND ----------

# DBTITLE 1,Transform: Silver EConnection History -> Gold Fact (Electricity 15-min)
max_ts = get_max_silver_ts(GOLD_ECONN_HISTORY)
print(f"Latest silver_processing_timestamp in {GOLD_ECONN_HISTORY}: {max_ts or 'none (initial load)'}")

silver_df = spark.read.table(SILVER_ECONN_HISTORY)
if max_ts:
    silver_df = silver_df.filter(F.col("silver_processing_timestamp") > max_ts)

new_rows = silver_df.count()
print(f"Silver electricity history rows to process: {new_rows:,}")

if new_rows > 0:
    gold_df = silver_df.select(
        F.md5(F.col("building_uuid")).alias("sk_building"),
        F.md5(F.concat(F.col("building_uuid"), F.lit("_econnection"))).alias("sk_econnection"),
        F.col("building_uuid"),
        F.col("building_name"),
        F.col("measurement_date"),
        F.col("measurement_timestamp"),
        F.col("power_import"),
        F.col("power_export"),
        F.col("power_production"),
        F.col("power_net"),
        F.col("power_charge"),
        F.col("power_discharge"),
        F.col("silver_processing_timestamp"),
    )
    gold_df.write.mode("append").saveAsTable(GOLD_ECONN_HISTORY)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_ECONN_HISTORY}")
else:
    print("No new electricity history data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Silver HConnection History -> Gold Fact (Heat 15-min)
max_ts = get_max_silver_ts(GOLD_HCONN_HISTORY)
print(f"Latest silver_processing_timestamp in {GOLD_HCONN_HISTORY}: {max_ts or 'none (initial load)'}")

silver_df = spark.read.table(SILVER_HCONN_HISTORY)
if max_ts:
    silver_df = silver_df.filter(F.col("silver_processing_timestamp") > max_ts)

new_rows = silver_df.count()
print(f"Silver heat history rows to process: {new_rows:,}")

if new_rows > 0:
    gold_df = silver_df.select(
        F.md5(F.col("building_uuid")).alias("sk_building"),
        F.md5(F.concat(F.col("building_uuid"), F.lit("_hconnection"))).alias("sk_hconnection"),
        F.col("building_uuid"),
        F.col("building_name"),
        F.col("measurement_date"),
        F.col("measurement_timestamp"),
        F.col("thermal_power"),
        F.col("silver_processing_timestamp"),
    )
    gold_df.write.mode("append").saveAsTable(GOLD_HCONN_HISTORY)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_HCONN_HISTORY}")
else:
    print("No new heat history data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Silver GConnection History -> Gold Fact (Gas)
max_ts = get_max_silver_ts(GOLD_GCONN_HISTORY)
print(f"Latest silver_processing_timestamp in {GOLD_GCONN_HISTORY}: {max_ts or 'none (initial load)'}")

silver_df = spark.read.table(SILVER_GCONN_HISTORY)
if max_ts:
    silver_df = silver_df.filter(F.col("silver_processing_timestamp") > max_ts)

new_rows = silver_df.count()
print(f"Silver gas history rows to process: {new_rows:,}")

if new_rows > 0:
    gold_df = silver_df.select(
        F.md5(F.col("building_uuid")).alias("sk_building"),
        F.md5(F.concat(F.col("building_uuid"), F.lit("_gconnection"))).alias("sk_gconnection"),
        F.col("building_uuid"),
        F.col("building_name"),
        F.col("measurement_date"),
        F.col("measurement_timestamp"),
        F.col("gas_volume_consumption"),
        F.col("silver_processing_timestamp"),
    )
    gold_df.write.mode("append").saveAsTable(GOLD_GCONN_HISTORY)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_GCONN_HISTORY}")
else:
    print("No new gas history data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Silver WaterConnection History -> Gold Fact (Water)
max_ts = get_max_silver_ts(GOLD_WATER_HISTORY)
print(f"Latest silver_processing_timestamp in {GOLD_WATER_HISTORY}: {max_ts or 'none (initial load)'}")

silver_df = spark.read.table(SILVER_WATER_HISTORY)
if max_ts:
    silver_df = silver_df.filter(F.col("silver_processing_timestamp") > max_ts)

new_rows = silver_df.count()
print(f"Silver water history rows to process: {new_rows:,}")

if new_rows > 0:
    gold_df = silver_df.select(
        F.md5(F.col("building_uuid")).alias("sk_building"),
        F.md5(F.concat(F.col("building_uuid"), F.lit("_waterconnection"))).alias("sk_waterconnection"),
        F.col("building_uuid"),
        F.col("building_name"),
        F.col("measurement_date"),
        F.col("measurement_timestamp"),
        F.col("water_volume_consumption"),
        F.col("silver_processing_timestamp"),
    )
    gold_df.write.mode("append").saveAsTable(GOLD_WATER_HISTORY)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_WATER_HISTORY}")
else:
    print("No new water history data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Silver FCR History -> Gold Fact (FCR)
max_ts = get_max_silver_ts(GOLD_FCR_HISTORY)
print(f"Latest silver_processing_timestamp in {GOLD_FCR_HISTORY}: {max_ts or 'none (initial load)'}")

silver_df = spark.read.table(SILVER_FCR_HISTORY)
if max_ts:
    silver_df = silver_df.filter(F.col("silver_processing_timestamp") > max_ts)

new_rows = silver_df.count()
print(f"Silver FCR history rows to process: {new_rows:,}")

if new_rows > 0:
    gold_df = silver_df.select(
        F.md5(F.col("building_uuid")).alias("sk_building"),
        F.col("building_uuid"),
        F.col("building_name"),
        F.col("measurement_date"),
        F.col("measurement_timestamp"),
        F.col("fcr_value"),
        F.col("silver_processing_timestamp"),
    )
    gold_df.write.mode("append").saveAsTable(GOLD_FCR_HISTORY)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_FCR_HISTORY}")
else:
    print("No new FCR history data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Silver EConnection Meterreadings -> Gold Fact (Electricity Meterreadings)
max_ts = get_max_silver_ts(GOLD_ECONN_METER)
print(f"Latest silver_processing_timestamp in {GOLD_ECONN_METER}: {max_ts or 'none (initial load)'}")

silver_df = spark.read.table(SILVER_ECONN_METER)
if max_ts:
    silver_df = silver_df.filter(F.col("silver_processing_timestamp") > max_ts)

new_rows = silver_df.count()
print(f"Silver electricity meterreadings rows to process: {new_rows:,}")

if new_rows > 0:
    gold_df = silver_df.select(
        F.md5(F.col("building_uuid")).alias("sk_building"),
        F.md5(F.col("sensor_uuid")).alias("sk_econnection"),
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
    gold_df.write.mode("append").saveAsTable(GOLD_ECONN_METER)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_ECONN_METER}")
else:
    print("No new electricity meterreadings data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Silver HConnection Meterreadings -> Gold Fact (Heat Meterreadings)
max_ts = get_max_silver_ts(GOLD_HCONN_METER)
print(f"Latest silver_processing_timestamp in {GOLD_HCONN_METER}: {max_ts or 'none (initial load)'}")

silver_df = spark.read.table(SILVER_HCONN_METER)
if max_ts:
    silver_df = silver_df.filter(F.col("silver_processing_timestamp") > max_ts)

new_rows = silver_df.count()
print(f"Silver heat meterreadings rows to process: {new_rows:,}")

if new_rows > 0:
    gold_df = silver_df.select(
        F.md5(F.col("building_uuid")).alias("sk_building"),
        F.md5(F.col("sensor_uuid")).alias("sk_hconnection"),
        F.col("building_uuid"),
        F.col("building_name"),
        F.col("sensor_uuid"),
        F.col("meter_name"),
        F.col("measurement_timestamp"),
        F.col("thermal_energy_consumption"),
        F.col("silver_processing_timestamp"),
    )
    gold_df.write.mode("append").saveAsTable(GOLD_HCONN_METER)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_HCONN_METER}")
else:
    print("No new heat meterreadings data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Silver WaterConnection Meterreadings -> Gold Fact (Water Meterreadings)
max_ts = get_max_silver_ts(GOLD_WATER_METER)
print(f"Latest silver_processing_timestamp in {GOLD_WATER_METER}: {max_ts or 'none (initial load)'}")

silver_df = spark.read.table(SILVER_WATER_METER)
if max_ts:
    silver_df = silver_df.filter(F.col("silver_processing_timestamp") > max_ts)

new_rows = silver_df.count()
print(f"Silver water meterreadings rows to process: {new_rows:,}")

if new_rows > 0:
    gold_df = silver_df.select(
        F.md5(F.col("building_uuid")).alias("sk_building"),
        F.md5(F.col("sensor_uuid")).alias("sk_waterconnection"),
        F.col("building_uuid"),
        F.col("building_name"),
        F.col("sensor_uuid"),
        F.col("meter_name"),
        F.col("measurement_timestamp"),
        F.col("water_volume_consumption"),
        F.col("silver_processing_timestamp"),
    )
    gold_df.write.mode("append").saveAsTable(GOLD_WATER_METER)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_WATER_METER}")
else:
    print("No new water meterreadings data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Silver Battery -> Gold Fact (Battery 15-min — charge/discharge + state_of_charge)
# Granularity: econnection_history drives the 15-min rows (96/building/day).
# state_of_charge joins on exact building_uuid + measurement_timestamp — NULL for all other intervals.
# Only buildings that appear in silver.ecosphere_batch_battery (i.e. have a battery) are kept.
max_ts = get_max_silver_ts(GOLD_BATTERY)
print(f"Latest silver_processing_timestamp in {GOLD_BATTERY}: {max_ts or 'none (initial load)'}")

econn_df = spark.read.table(SILVER_ECONN_HISTORY)
if max_ts:
    econn_df = econn_df.filter(F.col("silver_processing_timestamp") > max_ts)

# Restrict to buildings that have a battery (appear in silver battery table).
battery_buildings = spark.read.table(SILVER_BATTERY).select("building_uuid").distinct()
econn_df = econn_df.join(battery_buildings, "building_uuid", "inner")

new_rows = econn_df.count()
print(f"Silver econnection history rows to process for battery buildings: {new_rows:,}")

if new_rows > 0:
    # state_of_charge: 1 reading/building/day — join on exact timestamp; NULL everywhere else.
    soc_df = spark.read.table(SILVER_BATTERY).select("building_uuid", "measurement_timestamp", "state_of_charge")

    gold_df = (
        econn_df
        .join(soc_df, ["building_uuid", "measurement_timestamp"], "left")
        .select(
            F.md5(F.col("building_uuid")).alias("sk_building"),
            F.md5(F.concat(F.col("building_uuid"), F.lit("_battery"))).alias("sk_battery"),
            F.col("building_uuid"),
            F.col("building_name"),
            F.col("measurement_date"),
            F.col("measurement_timestamp"),
            F.col("state_of_charge"),
            F.col("power_charge").alias("power_charge"),
            F.col("power_discharge").alias("power_discharge"),
            F.col("silver_processing_timestamp"),
        )
    )
    gold_df.write.mode("append").option("mergeSchema", "true").saveAsTable(GOLD_BATTERY)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_BATTERY}")
else:
    print("No new battery data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Silver Solar Irradiance -> Gold Fact (Solar)
max_ts = get_max_silver_ts(GOLD_SOLAR)
print(f"Latest silver_processing_timestamp in {GOLD_SOLAR}: {max_ts or 'none (initial load)'}")

silver_df = spark.read.table(SILVER_SOLAR)
if max_ts:
    silver_df = silver_df.filter(F.col("silver_processing_timestamp") > max_ts)

new_rows = silver_df.count()
print(f"Silver solar irradiance rows to process: {new_rows:,}")

if new_rows > 0:
    gold_df = silver_df.select(
        F.md5(F.col("building_uuid")).alias("sk_building"),
        F.col("building_uuid"),
        F.col("building_name"),
        F.col("measurement_date"),
        F.col("measurement_timestamp"),
        F.col("solar_irradiance"),
        F.col("silver_processing_timestamp"),
    )
    gold_df.write.mode("append").saveAsTable(GOLD_SOLAR)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_SOLAR}")
else:
    print("No new solar irradiance data to process.")

# COMMAND ----------

# DBTITLE 1,Transform: Silver EV Charging Station -> Gold Fact (EV)
max_ts = get_max_silver_ts(GOLD_EV)
print(f"Latest silver_processing_timestamp in {GOLD_EV}: {max_ts or 'none (initial load)'}")

silver_df = spark.read.table(SILVER_EV)
if max_ts:
    silver_df = silver_df.filter(F.col("silver_processing_timestamp") > max_ts)

new_rows = silver_df.count()
print(f"Silver EV charging station rows to process: {new_rows:,}")

if new_rows > 0:
    gold_df = silver_df.select(
        F.md5(F.col("building_uuid")).alias("sk_building"),
        F.md5(F.col("sensor_uuid")).alias("sk_evchargingstation"),
        F.col("building_uuid"),
        F.col("building_name"),
        F.col("sensor_uuid"),
        F.col("measurement_date"),
        F.col("measurement_timestamp"),
        F.col("is_available"),
        F.col("is_in_session"),
        F.col("silver_processing_timestamp"),
    )
    gold_df.write.mode("append").saveAsTable(GOLD_EV)
    print(f"OK: Appended {gold_df.count():,} rows to {GOLD_EV}")
else:
    print("No new EV charging station data to process.")

# COMMAND ----------

# DBTITLE 1,Gold Layer Summary
print(f"\n{'='*60}")
print(f"GOLD LAYER SUMMARY (ESDL-Aligned Fact Tables)")
print(f"{'='*60}")

gold_tables = {
    "f_ecosphere_econnection_history":           GOLD_ECONN_HISTORY,
    "f_ecosphere_hconnection_history":           GOLD_HCONN_HISTORY,
    "f_ecosphere_gconnection_history":           GOLD_GCONN_HISTORY,
    "f_ecosphere_waterconnection_history":       GOLD_WATER_HISTORY,
    "f_ecosphere_fcr_history":                   GOLD_FCR_HISTORY,
    "f_ecosphere_econnection_meterreadings":     GOLD_ECONN_METER,
    "f_ecosphere_hconnection_meterreadings":     GOLD_HCONN_METER,
    "f_ecosphere_waterconnection_meterreadings": GOLD_WATER_METER,
    "f_ecosphere_battery":                       GOLD_BATTERY,
    "f_ecosphere_solar_irradiance":              GOLD_SOLAR,
    "f_ecosphere_ev_charging_station":           GOLD_EV,
}

for label, table_path in gold_tables.items():
    try:
        total = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_path}").collect()[0]["cnt"]
        print(f"\n  {label}: {total:,} total rows")
    except Exception as e:
        print(f"  ERROR reading {table_path}: {e}")
