-- models/silver/fact_delivery_remuneration.sql
{{ config(materialized='incremental', unique_key='shipment_id') }}
SELECT
    shipment_id,
    delivery_pos_id,
    service_id,
    delivery_fee,
    delivered_date AS date_id,
    delivered_at AS last_updated
FROM {{ ref('stg_delivery') }}
{% if is_incremental() %}
WHERE delivered_at > (SELECT COALESCE(MAX(last_updated), '1900-01-01') FROM {{ this }})
{% endif %}
