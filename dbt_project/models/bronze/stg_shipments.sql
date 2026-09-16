-- models/bronze/stg_shipments.sql
-- ============================================================
-- Bronze Layer: Đọc dữ liệu thô đơn hàng từ schema raw
-- ============================================================

{{
  config(
    materialized = 'incremental',
    schema       = 'bronze',
    unique_key   = 'shipment_id'
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
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    -- Metadata
    current_timestamp() AS _loaded_at
FROM raw.raw_shipments

{% if is_incremental() %}
  WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
{% endif %}

