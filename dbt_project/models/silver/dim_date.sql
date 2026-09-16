-- models/silver/dim_date.sql
{{ config(materialized='table') }}

-- Sử dụng hàm sequence() tối ưu của Spark SQL để tránh lỗi giới hạn đệ quy RECURSION_LEVEL_LIMIT_EXCEEDED
WITH dates AS (
  SELECT EXPLODE(SEQUENCE(DATE '2024-01-01', DATE '2026-12-31', INTERVAL 1 DAY)) AS date_id
)
SELECT
    date_id,
    YEAR(date_id) AS year,
    MONTH(date_id) AS month,
    DAY(date_id) AS day,
    QUARTER(date_id) AS quarter,
    DAYOFWEEK(date_id) AS day_of_week
FROM dates
