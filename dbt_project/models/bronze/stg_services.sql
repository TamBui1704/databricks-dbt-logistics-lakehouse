-- models/bronze/stg_services.sql
SELECT
    service_id,
    service_name,
    service_group
FROM {{ source('main', 'raw_services') }}
