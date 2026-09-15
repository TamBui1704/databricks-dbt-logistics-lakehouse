-- models/gold/all_kpis.sql
-- ============================================================
-- View tổng hợp toàn bộ dữ liệu Gold trên Databricks
-- ============================================================

{{
  config(
    materialized = 'view',
    schema       = 'gold'
  )
}}

SELECT *
FROM {{ ref('fct_logistics_kpis') }}

