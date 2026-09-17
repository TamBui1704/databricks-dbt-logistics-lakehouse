-- models/gold/gold_view_delivery.sql
{{ config(materialized='view') }}
SELECT
    f.shipment_id,
    f.delivery_fee,
    s.service_name,
    s.service_group,
    p.pos_name AS delivery_pos_name,
    p.province AS delivery_province,
    p.region AS delivery_region,
    f.date_id
FROM {{ ref('fact_delivery_remuneration') }} f
LEFT JOIN {{ ref('dim_service') }} s ON f.service_id = s.service_id
LEFT JOIN {{ ref('dim_pos_location') }} p ON f.delivery_pos_id = p.pos_id
LEFT JOIN {{ ref('dim_date') }} d ON f.date_id = d.date_id
