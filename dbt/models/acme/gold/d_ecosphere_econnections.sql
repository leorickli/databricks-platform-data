{{
  config(
    materialized='table',
    file_format='delta',
    post_hook="DROP TABLE IF EXISTS {{ var('acme_catalog', 'acme_dev') }}.gold.ecosphere_econnections"
  )
}}

SELECT
  MD5(CONCAT(building_uuid, '_econnection')) AS sk_econnection,
  *
FROM {{ ref('ecosphere_econnections') }}
