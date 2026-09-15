# Databricks notebook source
# ==============================================================================
# DATABRICKS NOTEBOOK: 01_generate_raw_data.py
# Schema đích: raw (Tầng dữ liệu thô)
# Bảng sinh ra: raw.raw_carriers, raw.raw_routes, raw.raw_shipments
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, expr
from datetime import datetime, timedelta
import random

print("🚀 Bắt đầu quá trình tạo Schema và sinh dữ liệu giả lập vào Schema 'raw'...")

# ------------------------------------------------------------------------------
# 0. Tự động tạo các Schemas nếu chưa tồn tại
# ------------------------------------------------------------------------------
spark.sql("CREATE SCHEMA IF NOT EXISTS raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
print("✅ Đã đảm bảo tồn tại các Schemas: raw, bronze, silver, gold.")

# ------------------------------------------------------------------------------
# 1. Sinh dữ liệu bảng raw.raw_carriers (Nhà vận chuyển)
# ------------------------------------------------------------------------------
carriers_data = [
    ("C001", "Giao Hàng Nhanh (GHN)", "Express", 500.0, 4.8, True, "2024-01-15"),
    ("C002", "Viettel Post", "Standard", 2000.0, 4.6, True, "2024-01-10"),
    ("C003", "VNPost (Bưu điện VN)", "Standard", 5000.0, 4.2, True, "2024-02-01"),
    ("C004", "J&T Express", "Express", 300.0, 4.5, True, "2024-03-05"),
    ("C005", "Ninja Van", "Express", 400.0, 4.3, True, "2024-03-12"),
    ("C006", "Vận tải Đa Quốc Gia", "Freight", 20000.0, 4.9, True, "2024-04-01"),
]

carriers_schema = ["carrier_id", "carrier_name", "carrier_type", "max_weight_kg", "rating", "is_active", "created_at"]

df_carriers = spark.createDataFrame(carriers_data, carriers_schema)

df_carriers.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("raw.raw_carriers")

print("✅ Đã tạo bảng: raw.raw_carriers (6 bản ghi)")

# ------------------------------------------------------------------------------
# 2. Sinh dữ liệu bảng raw.raw_routes (Tuyến đường vận chuyển)
# ------------------------------------------------------------------------------
routes_data = [
    ("R001", "Hà Nội", "TP. Hồ Chí Minh", 1720, 3),
    ("R002", "Hà Nội", "Đà Nẵng", 760, 2),
    ("R003", "TP. Hồ Chí Minh", "Đà Nẵng", 960, 2),
    ("R004", "TP. Hồ Chí Minh", "Cần Thơ", 160, 1),
    ("R005", "Hà Nội", "Hải Phòng", 120, 1),
    ("R006", "Đà Nẵng", "Nha Trang", 530, 2),
    ("R007", "TP. Hồ Chí Minh", "Bình Dương", 45, 1),
]

routes_schema = ["route_id", "origin_province", "dest_province", "distance_km", "est_transit_days"]

df_routes = spark.createDataFrame(routes_data, routes_schema)

df_routes.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("raw.raw_routes")

print("✅ Đã tạo bảng: raw.raw_routes (7 bản ghi)")

# ------------------------------------------------------------------------------
# 3. Sinh dữ liệu bảng raw.raw_shipments (Đơn vận chuyển - 200 bản ghi)
# ------------------------------------------------------------------------------
product_types = ["Electronics", "Fashion", "Food & Beverage", "Home Appliances", "Documents", "Cosmetics"]
vehicle_types = ["Van 1T", "Truck 5T", "Container 20ft", "Motorbike"]
statuses = ["DELIVERED", "DELIVERED", "DELIVERED", "IN_TRANSIT", "PENDING", "CANCELLED", "RETURNED"]
senders = ["Tiki Trading", "Shopee Mall", "Samsung VN", "Unilever FC", "Thế Giới Di Động", "Phong Vũ Computer"]
receivers = ["Nguyễn Văn A", "Trần Thị B", "Lê Hoàng C", "Phạm Minh D", "Vũ Quốc E", "Đặng Thảo F"]

shipments_data = []
today = datetime.now()

for i in range(1, 201):
    shipment_id = f"SHP{20260900 + i:08d}"
    carrier_id = f"C00{random.randint(1, 6)}"
    route_id = f"R00{random.randint(1, 7)}"
    prod = random.choice(product_types)
    veh = random.choice(vehicle_types)
    weight = round(random.uniform(0.5, 150.0), 2)
    fee = round(weight * random.uniform(15000, 30000), -3)
    status = random.choice(statuses)
    sender = random.choice(senders)
    receiver = random.choice(receivers)
    
    days_ago = random.randint(0, 30)
    created_dt = today - timedelta(days=days_ago, hours=random.randint(1, 10))
    created_at_str = created_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    if status == "DELIVERED":
        delivered_dt = created_dt + timedelta(days=random.randint(1, 4), hours=random.randint(1, 5))
        delivered_at_str = delivered_dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        delivered_at_str = None
        
    data_date_str = created_dt.strftime("%Y-%m-%d")

    shipments_data.append((
        shipment_id, carrier_id, route_id, prod, veh, weight, fee, 
        status, sender, receiver, created_at_str, delivered_at_str, data_date_str
    ))

shipments_schema = [
    "shipment_id", "carrier_id", "route_id", "product_type", "vehicle_type", 
    "weight_kg", "shipping_fee", "status", "sender_name", "receiver_name", 
    "created_at", "delivered_at", "data_date"
]

df_shipments = spark.createDataFrame(shipments_data, shipments_schema)

df_shipments.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("raw.raw_shipments")

print("✅ Đã tạo bảng: raw.raw_shipments (200 bản ghi)")
print("\n🎉 HOÀN THÀNH: Đã tạo xong 3 bảng Delta trong schema 'raw'!")
