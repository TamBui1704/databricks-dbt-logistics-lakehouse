-- models/silver/dim_service.sql
{{ config(materialized='table') }}
SELECT
    service_id,
    service_name,
    service_group
FROM {{ ref('stg_services') }}
