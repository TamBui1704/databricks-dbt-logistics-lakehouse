-- models/bronze/stg_pos_locations.sql
SELECT
    pos_id,
    pos_name,
    province,
    region
FROM {{ source('main', 'raw_pos_locations') }}
