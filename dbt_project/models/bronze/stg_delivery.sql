-- models/bronze/stg_delivery.sql
SELECT
    shipment_id,
    delivery_pos_id,
    service_id,
    delivery_fee,
    TO_TIMESTAMP(delivered_at, 'yyyy-MM-dd HH:mm:ss') AS delivered_at,
    TO_DATE(delivered_at, 'yyyy-MM-dd') AS delivered_date
FROM {{ source('main', 'raw_delivery') }}
