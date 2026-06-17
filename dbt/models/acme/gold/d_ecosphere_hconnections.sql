{{
  config(
    materialized='table',
    file_format='delta',
    post_hook="DROP TABLE IF EXISTS {{ var('acme_catalog', 'acme_dev') }}.gold.ecosphere_hconnections"
  )
}}

SELECT
  MD5(CONCAT(building_uuid, '_hconnection')) AS sk_hconnection,
  *
FROM {{ ref('ecosphere_hconnections') }}
