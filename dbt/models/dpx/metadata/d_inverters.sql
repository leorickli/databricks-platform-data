{{
  config(
    materialized='table',
    file_format='delta',
    post_hook="DROP TABLE IF EXISTS {{ var('dpx_catalog', 'dpx_dev') }}.metadata.inverters"
  )
}}

SELECT
  MD5(CONCAT(brand, model_series, model_sku)) AS sk_inverters,
  *
FROM {{ ref('inverters') }}