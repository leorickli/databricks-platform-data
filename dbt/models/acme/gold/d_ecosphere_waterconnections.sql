{{
  config(
    materialized='table',
    file_format='delta',
    post_hook="DROP TABLE IF EXISTS {{ var('acme_catalog', 'acme_dev') }}.gold.ecosphere_waterconnections"
  )
}}

SELECT
  MD5(CONCAT(building_uuid, '_waterconnection')) AS sk_waterconnection,
  *
FROM {{ ref('ecosphere_waterconnections') }}
