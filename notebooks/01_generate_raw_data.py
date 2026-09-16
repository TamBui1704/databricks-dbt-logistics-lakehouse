# Databricks notebook source
# ==============================================================================
# DATABRICKS NOTEBOOK: 01_generate_raw_data.py
# Schema đích: raw (Tầng dữ liệu thô)
# Bảng sinh ra: raw.raw_carriers, raw.raw_routes, raw.raw_shipments
# Hỗ trợ tham số LOAD_TYPE: FULL hoặc INCREMENTAL
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from datetime import datetime, timedelta
import random

# Lấy tham số LOAD_TYPE (Mặc định là INCREMENTAL để chạy hàng ngày)
dbutils.widgets.dropdown("LOAD_TYPE", "INCREMENTAL", ["FULL", "INCREMENTAL"], "Load Type")
load_type = dbutils.widgets.get("LOAD_TYPE")

print(f"🚀 Bắt đầu tạo dữ liệu giả lập. Chế độ: {load_type} Load")

# 0. Tự động tạo các Schemas nếu chưa tồn tại
spark.sql("CREATE SCHEMA IF NOT EXISTS raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# ------------------------------------------------------------------------------
# 1 & 2: Bảng Danh mục (Carriers & Routes) thường tĩnh nên có thể ghi đè
# ------------------------------------------------------------------------------
if load_type == "FULL":
    carriers_data = [
        ("C001", "Giao Hàng Nhanh (GHN)", "Express", 500.0, 4.8, True, "2024-01-15"),
        ("C002", "Viettel Post", "Standard", 2000.0, 4.6, True, "2024-01-10"),
        ("C003", "VNPost (Bưu điện VN)", "Standard", 5000.0, 4.2, True, "2024-02-01"),
        ("C004", "J&T Express", "Express", 300.0, 4.5, True, "2024-03-05"),
        ("C005", "Ninja Van", "Express", 400.0, 4.3, True, "2024-03-12"),
        ("C006", "Vận tải Đa Quốc Gia", "Freight", 20000.0, 4.9, True, "2024-04-01"),
    ]
    carriers_schema = ["carrier_id", "carrier_name", "carrier_type", "max_weight_kg", "rating", "is_active", "created_at"]
    spark.createDataFrame(carriers_data, carriers_schema).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw.raw_carriers")

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
    spark.createDataFrame(routes_data, routes_schema).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw.raw_routes")
    print("✅ Đã ghi đè các bảng danh mục (Carriers & Routes).")

# ------------------------------------------------------------------------------
# 3. Bảng Giao dịch (Shipments) - Upsert or Overwrite
# ------------------------------------------------------------------------------
product_types = ["Electronics", "Fashion", "Food & Beverage", "Home Appliances", "Documents", "Cosmetics"]
vehicle_types = ["Van 1T", "Truck 5T", "Container 20ft", "Motorbike"]
senders = ["Tiki Trading", "Shopee Mall", "Samsung VN", "Unilever FC", "Thế Giới Di Động", "Phong Vũ Computer"]
receivers = ["Nguyễn Văn A", "Trần Thị B", "Lê Hoàng C", "Phạm Minh D", "Vũ Quốc E", "Đặng Thảo F"]
shipments_schema = [
    "shipment_id", "carrier_id", "route_id", "product_type", "vehicle_type", 
    "weight_kg", "shipping_fee", "status", "sender_name", "receiver_name", 
    "created_at", "delivered_at", "data_date", "updated_at"
]
today = datetime.now()

def generate_new_shipments(start_id, count, max_days_ago=5):
    data = []
    for i in range(count):
        shipment_id = f"SHP{start_id + i:08d}"
        created_dt = today - timedelta(days=random.randint(0, max_days_ago), hours=random.randint(1, 10))
        # Đơn mới mặc định là PENDING hoặc IN_TRANSIT
        status = random.choice(["PENDING", "IN_TRANSIT"])
        
        data.append((
            shipment_id, f"C00{random.randint(1, 6)}", f"R00{random.randint(1, 7)}",
            random.choice(product_types), random.choice(vehicle_types),
            round(random.uniform(0.5, 150.0), 2),
            round(random.uniform(0.5, 150.0) * random.uniform(15000, 30000), -3),
            status, random.choice(senders), random.choice(receivers),
            created_dt.strftime("%Y-%m-%d %H:%M:%S"), None, created_dt.strftime("%Y-%m-%d"), 
            today.strftime("%Y-%m-%d %H:%M:%S") # updated_at
        ))
    return data

if load_type == "FULL":
    print("⏳ Đang tạo 200 bản ghi dữ liệu mẫu ban đầu...")
    data = generate_new_shipments(20260900, 200, max_days_ago=30)
    df_shipments = spark.createDataFrame(data, shipments_schema)
    
    df_shipments.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw.raw_shipments")
    print("✅ Đã ghi đè tạo bảng: raw.raw_shipments (200 bản ghi FULL LOAD)")

elif load_type == "INCREMENTAL":
    print("⏳ Đang giả lập thay đổi trạng thái (Upsert)...")
    
    # 1. Lấy dữ liệu hiện tại để thay đổi trạng thái
    try:
        df_existing = spark.table("raw.raw_shipments").toPandas()
        
        # Tìm các đơn chưa giao
        pending_mask = df_existing['status'].isin(['PENDING', 'IN_TRANSIT'])
        pending_indices = df_existing[pending_mask].index.tolist()
        
        # Chọn random 30% đơn chưa giao để cập nhật thành DELIVERED
        num_to_update = max(1, int(len(pending_indices) * 0.3))
        indices_to_update = random.sample(pending_indices, min(num_to_update, len(pending_indices)))
        
        updates_data = []
        for idx in indices_to_update:
            row = df_existing.iloc[idx]
            created_dt = datetime.strptime(row['created_at'], "%Y-%m-%d %H:%M:%S")
            delivered_dt = today - timedelta(hours=random.randint(1, 10))
            if delivered_dt < created_dt:
                delivered_dt = created_dt + timedelta(hours=2)
                
            updates_data.append((
                row['shipment_id'], row['carrier_id'], row['route_id'], row['product_type'], 
                row['vehicle_type'], float(row['weight_kg']), float(row['shipping_fee']),
                "DELIVERED", row['sender_name'], row['receiver_name'],
                row['created_at'], delivered_dt.strftime("%Y-%m-%d %H:%M:%S"), row['data_date'], 
                today.strftime("%Y-%m-%d %H:%M:%S") # updated_at mới
            ))
            
        print(f"   -> Đã giả lập {len(updates_data)} đơn hàng cập nhật thành DELIVERED.")
        
        # 2. Sinh thêm đơn hàng mới (Ví dụ: 10 đơn mới)
        max_id_str = df_existing['shipment_id'].max()
        next_id = int(max_id_str.replace("SHP", "")) + 1
        new_data = generate_new_shipments(next_id, 10, max_days_ago=1)
        print(f"   -> Đã sinh {len(new_data)} đơn hàng mới (INSERT).")
        
        # Gộp dữ liệu update và insert thành 1 DataFrame thay đổi (Delta)
        df_upsert = spark.createDataFrame(updates_data + new_data, shipments_schema)
        
        # 3. Thực hiện lệnh MERGE INTO (Upsert) vào bảng gốc
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forName(spark, "raw.raw_shipments")
        
        delta_table.alias("target").merge(
            df_upsert.alias("source"),
            "target.shipment_id = source.shipment_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        print("✅ Đã hoàn tất MERGE (Upsert) vào raw.raw_shipments!")

    except Exception as e:
        print(f"⚠️ Lỗi khi chạy Incremental: {e}. Vui lòng chạy FULL load trước để tạo bảng!")

print("\n🎉 HOÀN THÀNH: Xử lý dữ liệu raw thành công!")
