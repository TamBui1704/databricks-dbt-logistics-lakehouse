-- models/silver/dim_pos_location.sql
{{ config(materialized='table') }}
SELECT
    pos_id,
    pos_name,
    province,
    region
FROM {{ ref('stg_pos_locations') }}
