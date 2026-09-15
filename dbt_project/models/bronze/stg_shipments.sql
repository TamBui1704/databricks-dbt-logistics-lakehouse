-- models/bronze/stg_shipments.sql
-- ============================================================
-- Bronze Layer: Đọc dữ liệu thô đơn hàng từ schema raw
-- ============================================================

{{
  config(
    materialized = 'view',
    schema       = 'bronze'
  )
}}

SELECT
    shipment_id,
    carrier_id,
    route_id,
    product_type,
    vehicle_type,
    CAST(weight_kg AS DOUBLE) AS weight_kg,
    CAST(shipping_fee AS DOUBLE) AS shipping_fee,
    status,
    sender_name,
    receiver_name,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(delivered_at AS TIMESTAMP) AS delivered_at,
    CAST(data_date AS DATE) AS data_date,
    -- Metadata
    current_timestamp() AS _loaded_at
FROM raw.raw_shipments

