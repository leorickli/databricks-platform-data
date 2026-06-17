-- ESDL EConnection dim for wattflow DAP meters.
-- Seed CSV (wattflow_sites.csv) keeps Dutch + camelCase source names (sleutel,
-- klantnummer, dapId, geoAddr…). This dim translates everything to snake_case
-- English on output so silver/gold/frontend see a single consistent vocabulary.
SELECT
  MD5(CONCAT(ean, sleutel))   AS sk_econnection,
  ean,
  sleutel                     AS key,
  dapId                       AS dap_id,
  dis,
  geoAddr                     AS geo_addr,
  geoStreet                   AS geo_street,
  geoCity                     AS geo_city,
  geoPostalCode               AS geo_postal_code,
  geoCountry                  AS geo_country,
  tz,
  meter_status,
  entity,
  klantnummer                 AS customer_number,
  type
FROM {{ ref('wattflow_sites') }}
