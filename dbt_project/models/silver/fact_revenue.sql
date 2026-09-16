-- models/silver/fact_revenue.sql
{{ config(materialized='incremental', unique_key='shipment_id') }}
SELECT
    shipment_id,
    customer_id,
    service_id,
    origin_pos_id,
    dest_pos_id,
    revenue_amount,
    weight_kg,
    created_date AS date_id,
    created_at AS last_updated
FROM {{ ref('stg_revenue') }}
{% if is_incremental() %}
WHERE created_at > (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
{% endif %}
