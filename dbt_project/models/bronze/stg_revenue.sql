-- models/bronze/stg_revenue.sql
SELECT
    shipment_id,
    customer_id,
    service_id,
    origin_pos_id,
    dest_pos_id,
    revenue_amount,
    weight_kg,
    TO_TIMESTAMP(created_at, 'yyyy-MM-dd HH:mm:ss') AS created_at,
    TO_DATE(created_at, 'yyyy-MM-dd') AS created_date
FROM {{ source('main', 'raw_revenue') }}
