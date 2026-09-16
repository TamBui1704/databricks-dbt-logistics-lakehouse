-- models/silver/dim_customer.sql
{{ config(materialized='table') }}
SELECT
    customer_id,
    customer_name,
    customer_type
FROM {{ ref('stg_customers') }}
