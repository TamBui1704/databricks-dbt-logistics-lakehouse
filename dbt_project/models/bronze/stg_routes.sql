-- models/bronze/stg_routes.sql
-- ============================================================
-- Bronze Layer: Đọc dữ liệu thô tuyến đường từ schema raw
-- ============================================================

{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

SELECT
    route_id,
    origin_province,
    dest_province,
    CAST(distance_km AS INT) AS distance_km,
    CAST(est_transit_days AS INT) AS est_transit_days,
    -- Metadata
    current_timestamp() AS _loaded_at
FROM raw.raw_routes
