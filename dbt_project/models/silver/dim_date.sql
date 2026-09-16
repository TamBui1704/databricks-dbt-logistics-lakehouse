-- models/silver/dim_date.sql
{{ config(materialized='table') }}
-- A simple date dimension generation
WITH RECURSIVE dates AS (
  SELECT DATE '2026-01-01' AS date_id
  UNION ALL
  SELECT date_id + INTERVAL 1 DAY
  FROM dates
  WHERE date_id < DATE '2026-12-31'
)
SELECT
    date_id,
    YEAR(date_id) AS year,
    MONTH(date_id) AS month,
    DAY(date_id) AS day,
    QUARTER(date_id) AS quarter,
    DAYOFWEEK(date_id) AS day_of_week
FROM dates
