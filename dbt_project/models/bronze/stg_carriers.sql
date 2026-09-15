-- models/bronze/stg_carriers.sql
-- ============================================================
-- Bronze Layer: Đọc dữ liệu thô công ty vận chuyển từ schema raw
-- ============================================================

{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

SELECT
    carrier_id,
    carrier_name,
    carrier_type,
    CAST(max_weight_kg AS DOUBLE) AS max_weight_kg,
    CAST(rating AS DOUBLE) AS rating,
    CAST(is_active AS BOOLEAN) AS is_active,
    CAST(created_at AS DATE) AS created_at,
    -- Metadata
    current_timestamp() AS _loaded_at
FROM raw.raw_carriers

