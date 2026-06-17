{{
  config(
    materialized='table',
    file_format='delta',
    post_hook="DROP TABLE IF EXISTS {{ var('acme_catalog', 'acme_dev') }}.gold.ecosphere_gconnections"
  )
}}

SELECT
  MD5(CONCAT(building_uuid, '_gconnection')) AS sk_gconnection,
  *
FROM {{ ref('ecosphere_gconnections') }}
