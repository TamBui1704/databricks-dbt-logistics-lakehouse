-- models/bronze/stg_customers.sql
SELECT
    customer_id,
    customer_name,
    customer_type
FROM {{ source('main', 'raw_customers') }}
