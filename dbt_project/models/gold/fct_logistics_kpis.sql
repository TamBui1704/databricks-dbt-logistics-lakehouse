-- models/gold/fct_logistics_kpis.sql
-- ============================================================
-- Gold Layer: KPI tổng hợp theo ngày + carrier
-- Lưu trữ: Incremental Delta Table trên Databricks (Schema: gold)
-- ============================================================

{{
  config(
    materialized         = 'incremental',
    schema               = 'gold',
    file_format          = 'delta',
    unique_key           = ['data_date', 'carrier_id'],
    incremental_strategy = 'merge'
  )
}}

WITH base AS (
    SELECT * FROM {{ ref('int_shipment_details') }}
    {% if is_incremental() %}
    -- Chế độ Incremental: Chỉ tính toán KPI cho dữ liệu các ngày mới
    WHERE data_date >= (SELECT COALESCE(MAX(data_date), '1900-01-01') FROM {{ this }})
    {% endif %}
)


SELECT
    data_date,
    carrier_id,
    carrier_name,
    carrier_type,

    -- ── Khối lượng ─────────────────────────────────────────────────────
    COUNT(shipment_id)                                                     AS total_shipments,
    COUNT(CASE WHEN status IN ('Đã giao', 'DELIVERED') THEN 1 END)          AS delivered_count,
    COUNT(CASE WHEN status IN ('Hoàn hàng', 'RETURNED') THEN 1 END)         AS returned_count,
    COUNT(CASE WHEN status IN ('Thất lạc', 'CANCELLED') THEN 1 END)        AS lost_count,

    -- ── Tỷ lệ ──────────────────────────────────────────────────────────
    ROUND(
        COUNT(CASE WHEN status IN ('Đã giao', 'DELIVERED') THEN 1 END) * 100.0 / COUNT(shipment_id), 2
    )                                                                      AS delivery_rate_pct,

    ROUND(
        COUNT(CASE WHEN is_on_time = TRUE THEN 1 END) * 100.0
        / NULLIF(COUNT(CASE WHEN status IN ('Đã giao', 'DELIVERED') THEN 1 END), 0), 2
    )                                                                      AS on_time_rate_pct,

    -- ── Doanh thu & Trọng lượng ─────────────────────────────────────────
    ROUND(SUM(shipping_fee), 0)                                            AS total_revenue,
    ROUND(AVG(shipping_fee), 0)                                            AS avg_revenue_per_shipment,
    ROUND(SUM(weight_kg), 2)                                               AS total_weight_kg,
    ROUND(AVG(weight_kg), 2)                                               AS avg_weight_kg,

    -- ── Thời gian vận chuyển ────────────────────────────────────────────
    ROUND(AVG(actual_transit_days), 1)                                     AS avg_transit_days,
    ROUND(AVG(distance_km), 0)                                             AS avg_distance_km,
    ROUND(AVG(fee_per_km), 0)                                              AS avg_fee_per_km,

    -- ── Rating ─────────────────────────────────────────────────────────
    MAX(carrier_rating)                                                    AS carrier_rating,

    -- ── Breakdown theo loại sản phẩm ───────────────────────────────────
    COUNT(CASE WHEN product_type IN ('Điện tử', 'Electronics') THEN 1 END)       AS cnt_electronics,
    COUNT(CASE WHEN product_type IN ('Thực phẩm', 'Food & Beverage') THEN 1 END) AS cnt_food,
    COUNT(CASE WHEN product_type IN ('Quần áo', 'Fashion') THEN 1 END)           AS cnt_fashion,
    COUNT(CASE WHEN product_type IN ('Mỹ phẩm', 'Cosmetics') THEN 1 END)         AS cnt_cosmetics,
    COUNT(CASE WHEN product_type IN ('Đồ gia dụng', 'Home Appliances') THEN 1 END) AS cnt_household,

    current_timestamp()                                                    AS _updated_at

FROM base
GROUP BY
    data_date,
    carrier_id,
    carrier_name,
    carrier_type

