-- models/gold/gold_view_revenue.sql
{{ config(materialized='view') }}
SELECT
    f.shipment_id,
    f.revenue_amount,
    f.weight_kg,
    c.customer_name,
    c.customer_type,
    s.service_name,
    s.service_group,
    p_orig.pos_name AS origin_pos_name,
    p_orig.province AS origin_province,
    p_orig.region AS origin_region,
    p_dest.pos_name AS dest_pos_name,
    p_dest.province AS dest_province,
    p_dest.region AS dest_region,
    f.date_id
FROM {{ ref('fact_revenue') }} f
LEFT JOIN {{ ref('dim_customer') }} c ON f.customer_id = c.customer_id
LEFT JOIN {{ ref('dim_service') }} s ON f.service_id = s.service_id
LEFT JOIN {{ ref('dim_pos_location') }} p_orig ON f.origin_pos_id = p_orig.pos_id
LEFT JOIN {{ ref('dim_pos_location') }} p_dest ON f.dest_pos_id = p_dest.pos_id
LEFT JOIN {{ ref('dim_date') }} d ON f.date_id = d.date_id
