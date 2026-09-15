-- models/silver/all_shipments.sql
-- ============================================================
-- View tổng hợp toàn bộ dữ liệu Silver trên Databricks
-- ============================================================

{{
  config(
    materialized = 'view',
    schema       = 'silver'
  )
}}

SELECT *
FROM {{ ref('int_shipment_details') }}

