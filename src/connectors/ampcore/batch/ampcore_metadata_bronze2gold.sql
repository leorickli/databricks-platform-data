-- Databricks notebook source
-- LDP SQL definition for the AMPCORE SCU200 EConnection metering-point dimension.
-- Runs inside acme_ampcore_pipeline (see resources/acme_pipelines.yml).
--
-- Deduplicates bronze snapshots to one row per (gatewayId, objectId, snapshotDate).
-- variables is reshaped from MapType(varName → struct) to ArrayType<Struct> so
-- downstream SQL can iterate it with LATERAL VIEW explode instead of map bracket access.
--
-- Ontology: each AMPCORE CurrentSensor is modelled as an ESDL EConnection metering
-- point. ESDL attributes (id, original_id_in_source, name, asset_type, manufacturer,
-- serialNumber) are surfaced explicitly; AMPCORE-specific fields (gatewayIp,
-- modbusId, variables) are kept for operational use. Direction is the ESDL
-- representation of isEnergyExport: 'Export' for feed-in sensors, 'Import' for
-- grid-consumption sensors.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold.d_ampcore_econnections (
  sk_econnection        STRING       COMMENT 'ESDL EConnection id — sha2-256 on gatewayId|objectId, stable across gateway IP changes',
  original_id_in_source   STRING       COMMENT 'ESDL EConnection.original_id_in_source — composite gatewayId|objectId',
  name                 STRING       COMMENT 'ESDL EConnection.name — human-readable sensor name configured in the SCU200',
  asset_type            STRING       COMMENT 'ESDL EConnection.asset_type — always ''AMPCORE SCU200 CurrentSensor''',
  manufacturer         STRING       COMMENT 'ESDL EConnection.manufacturer — always ''AMPCORE''',
  serial_number        STRING       COMMENT 'Physical sensor serial number',
  direction            STRING       COMMENT 'ESDL energy flow direction: ''Export'' (feed-in to grid) or ''Import'' (grid consumption), derived from isEnergyExport',
  gateway_id           STRING       COMMENT 'AMPCORE SCU200 gateway identifier (natural key component)',
  gateway_ip           STRING       COMMENT 'AMPCORE SCU200 gateway IP address at snapshot time',
  gateway_serial_number STRING      COMMENT 'AMPCORE SCU200 gateway serial number',
  object_id            INT          COMMENT 'Sensor object ID within the SCU200 gateway (natural key component)',
  modbus_id            INT          COMMENT 'Modbus slave address of the physical sensor',
  variables            ARRAY<STRUCT<name:STRING,category:STRING,dataType:STRING,unit:STRING,multiplier:DOUBLE,dbWriteInterval:INT,dbStorePolicy:STRING,readable:BOOLEAN,writable:BOOLEAN,invalidValues:ARRAY<STRING>,relation:ARRAY<STRING>>>
                                    COMMENT 'Modbus variable definitions for this sensor, reshaped from a map to an array for SQL-native iteration',
  snapshot_date        DATE         COMMENT 'Date of the metadata snapshot from which this row was derived',
  gold_processing_timestamp TIMESTAMP COMMENT 'Timestamp when this row was written to gold',
  CONSTRAINT valid_sk            EXPECT (sk_econnection IS NOT NULL)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_snapshot_date EXPECT (snapshot_date IS NOT NULL)   ON VIOLATION DROP ROW
)
CLUSTER BY AUTO
COMMENT
  'Gold dimension for AMPCORE SCU200 CurrentSensor metering points, ESDL-aligned as
   EConnection. One row per (gatewayId, objectId, snapshotDate). sk_econnection is
   sha2-256 on gatewayId|objectId, stable across gateway IP changes. direction
   encodes the ESDL energy-flow direction (Import / Export) of the metering
   point. variables is an array of structs (one element per Modbus variable) for
   SQL-native LATERAL VIEW iteration.'
AS
WITH deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY gatewayId, objectId, snapshotDate
           ORDER BY _ingested_at DESC
         ) AS rn
  FROM bronze.ampcore_metadata_batch
)
SELECT
    sha2(concat_ws('|', gatewayId, cast(objectId AS STRING)), 256)
                                AS sk_econnection,
    concat_ws('|', gatewayId, cast(objectId AS STRING))
                                AS original_id_in_source,
    name                        AS name,
    'AMPCORE SCU200 CurrentSensor'  AS asset_type,
    'AMPCORE'                       AS manufacturer,
    serialNumber                AS serial_number,
    CASE WHEN isEnergyExport THEN 'Export' ELSE 'Import' END
                                AS direction,
    gatewayId                   AS gateway_id,
    gatewayIp                   AS gateway_ip,
    gatewaySerialNumber         AS gateway_serial_number,
    objectId                    AS object_id,
    modbusId                    AS modbus_id,
    transform(
      map_entries(variables),
      e -> named_struct(
        'name',            e.key,
        'category',        e.value.category,
        'dataType',        e.value.dataType,
        'unit',            e.value.unit,
        'multiplier',      e.value.multiplier,
        'dbWriteInterval', e.value.dbWriteInterval,
        'dbStorePolicy',   e.value.dbStorePolicy,
        'readable',        e.value.readable,
        'writable',        e.value.writable,
        'invalidValues',   e.value.invalidValues,
        'relation',        e.value.relation
      )
    )                           AS variables,
    snapshotDate                AS snapshot_date,
    current_timestamp()         AS gold_processing_timestamp
FROM deduped
WHERE rn = 1
