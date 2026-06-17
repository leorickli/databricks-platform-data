-- Databricks notebook source
-- LDP SQL definition for the Smartnode energyasset metadata gold dimensions,
-- normalized. Runs inside acme_smartnode_pipeline (see resources/acme_pipelines.yml).
--
-- A Smartnode energyasset corresponds to a Building containing an EConnection
-- and (optionally) a PVInstallation. We do not yet have a metadata signal
-- telling us which energyassets actually have PV, so we emit one row in
-- BOTH dimensions for every energyasset. The PVInstallation fact table will
-- naturally be empty for assets that never produce a cat-10 reading.
--
-- Both materialised views deduplicate bronze snapshots to one row per
-- (accountId, energyassetId, snapshotDate) and compute the stable surrogate
-- keys (sk_econnection / sk_pvinstallation) used in filterClause in connection_config.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold.d_smartnode_econnections (
  sk_econnection           STRING    COMMENT 'EConnection id — sha2-256 on accountId|energyassetId|EConnection',
  original_id_in_source      STRING    COMMENT 'EConnection.original_id_in_source — composite accountId|energyassetId',
  asset_type               STRING    COMMENT 'EConnection.asset_type — always ''Smartnode EConnection''',
  manufacturer            STRING    COMMENT 'EConnection.manufacturer — always ''Smartnode''',
  account_id              INT       COMMENT 'Smartnode account identifier (natural key component)',
  energyasset_id          INT       COMMENT 'Smartnode energy asset identifier (natural key component)',
  validated               TIMESTAMP COMMENT 'Last validated timestamp for this asset per the Smartnode API',
  snapshot_date           DATE      COMMENT 'Date of the metadata snapshot from which this row was derived',
  gold_processing_timestamp TIMESTAMP COMMENT 'Timestamp when this row was written to gold',
  CONSTRAINT valid_sk            EXPECT (sk_econnection IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_snapshot_date EXPECT (snapshot_date IS NOT NULL)  ON VIOLATION DROP ROW
)
CLUSTER BY AUTO
COMMENT
  'Gold dimension for Smartnode EConnection assets (grid connection of an energyasset),
   normalized. One row per (accountId, energyassetId, snapshotDate).
   sk_econnection is sha2-256 on accountId|energyassetId|EConnection — stable
   across re-provisioning. Used by smartnode_connection_config.py to seed
   connection_config for the EConnection side of the energyasset.'
AS
WITH deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY accountId, energyassetId, snapshotDate
           ORDER BY _ingested_at DESC
         ) AS rn
  FROM bronze.smartnode_metadata_batch
)
SELECT
    sha2(concat_ws('|', cast(accountId AS STRING), cast(energyassetId AS STRING), 'EConnection'), 256)
                            AS sk_econnection,
    concat_ws('|', cast(accountId AS STRING), cast(energyassetId AS STRING))
                            AS original_id_in_source,
    'Smartnode EConnection'    AS asset_type,
    'Smartnode'                AS manufacturer,
    accountId               AS account_id,
    energyassetId           AS energyasset_id,
    validated,
    snapshotDate            AS snapshot_date,
    current_timestamp()     AS gold_processing_timestamp
FROM deduped
WHERE rn = 1

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold.d_smartnode_pvinstallations (
  sk_pvinstallation        STRING    COMMENT 'PVInstallation id — sha2-256 on accountId|energyassetId|PVInstallation',
  original_id_in_source      STRING    COMMENT 'PVInstallation.original_id_in_source — composite accountId|energyassetId',
  asset_type               STRING    COMMENT 'PVInstallation.asset_type — always ''Smartnode PVInstallation''',
  manufacturer            STRING    COMMENT 'PVInstallation.manufacturer — always ''Smartnode''',
  account_id              INT       COMMENT 'Smartnode account identifier (natural key component)',
  energyasset_id          INT       COMMENT 'Smartnode energy asset identifier (natural key component)',
  validated               TIMESTAMP COMMENT 'Last validated timestamp for this asset per the Smartnode API',
  snapshot_date           DATE      COMMENT 'Date of the metadata snapshot from which this row was derived',
  gold_processing_timestamp TIMESTAMP COMMENT 'Timestamp when this row was written to gold',
  CONSTRAINT valid_sk            EXPECT (sk_pvinstallation IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_snapshot_date EXPECT (snapshot_date IS NOT NULL)     ON VIOLATION DROP ROW
)
CLUSTER BY AUTO
COMMENT
  'Gold dimension for Smartnode PVInstallation assets (optional PV system attached
   to an energyasset), normalized. Currently emitted for every energyasset
   (we lack a metadata signal for PV presence); the f_smartnode_pvinstallation_measurements
   fact will be empty for assets that never produce a cat-10 reading. One row
   per (accountId, energyassetId, snapshotDate). sk_pvinstallation is sha2-256
   on accountId|energyassetId|PVInstallation. Used by smartnode_connection_config.py
   to seed connection_config for the PVInstallation side of the energyasset.'
AS
WITH deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY accountId, energyassetId, snapshotDate
           ORDER BY _ingested_at DESC
         ) AS rn
  FROM bronze.smartnode_metadata_batch
)
SELECT
    sha2(concat_ws('|', cast(accountId AS STRING), cast(energyassetId AS STRING), 'PVInstallation'), 256)
                            AS sk_pvinstallation,
    concat_ws('|', cast(accountId AS STRING), cast(energyassetId AS STRING))
                            AS original_id_in_source,
    'Smartnode PVInstallation' AS asset_type,
    'Smartnode'                AS manufacturer,
    accountId               AS account_id,
    energyassetId           AS energyasset_id,
    validated,
    snapshotDate            AS snapshot_date,
    current_timestamp()     AS gold_processing_timestamp
FROM deduped
WHERE rn = 1
