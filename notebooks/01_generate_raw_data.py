import os
from pyspark.sql.functions import col, rand, round, expr, when, date_add, to_timestamp, lit
from databricks.connect import DatabricksSession
from dotenv import load_dotenv

def get_spark_session():
    load_dotenv()
    if "DATABRICKS_TOKEN" in os.environ:
        del os.environ["DATABRICKS_TOKEN"]
    
    print("Dang khoi tao Spark Session qua Databricks Connect (Serverless)...")
    return DatabricksSession.builder.serverless().getOrCreate()

def main():
    spark = get_spark_session()
    print("Da ket noi Spark Session thanh cong!")

    catalog = "main"
    schema = "raw"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"Da tao schema {catalog}.{schema}")

    # ==========================================
    # 1. TẠO DỮ LIỆU DIMENSIONS (Ghi đè)
    # ==========================================
    print("Generating Dimensions...")
    customer_data = [("CUST_001", "Shopee", "B2B"), ("CUST_002", "Tiki", "B2B"), ("CUST_003", "Lazada", "B2B"), ("CUST_004", "Nguyen Van A", "B2C"), ("CUST_005", "Tran Thi B", "B2C")]
    service_data = [("SRV_EMS", "Chuyen phat nhanh EMS", "Nhanh"), ("SRV_STD", "Chuyen phat Tieu chuan", "Thuong"), ("SRV_EXP", "Chuyen phat Hoa toc", "Hoa toc")]
    pos_data = [("POS_HN1", "Buu cuc TT Ha Noi", "Ha Noi", "Mien Bac"), ("POS_HN2", "Buu cuc Cau Giay", "Ha Noi", "Mien Bac"), ("POS_HCM1", "Buu cuc TT HCM", "Ho Chi Minh", "Mien Nam"), ("POS_DN1", "Buu cuc Da Nang", "Da Nang", "Mien Trung")]
    
    df_customers = spark.createDataFrame(customer_data, ["customer_id", "customer_name", "customer_type"])
    df_services = spark.createDataFrame(service_data, ["service_id", "service_name", "service_group"])
    df_pos = spark.createDataFrame(pos_data, ["pos_id", "pos_name", "province", "region"])
    
    df_customers.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_customers")
    df_services.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_services")
    df_pos.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_pos_locations")

    # ==========================================
    # 2. TẠO DỮ LIỆU CÓ PHÂN PHỐI THỜI GIAN THỰC TẾ
    # ==========================================
    print("Generating ~10M records with realistic time-series distribution...")
    
    # 2.1. Sinh bộ lịch (Calendar) 990 ngày
    df_days = spark.range(990).withColumn("created_at_date", expr("date_add(cast('2024-01-01' as date), cast(id as int))"))
    
    # 2.2. Gán số lượng đơn hàng (Volume) cho từng ngày
    # Thứ 1 (Sunday) và Thứ 7 (Saturday) theo PySpark dayofweek là 1 và 7
    # Ngày thường: ~12,000 - 14,000 đơn/ngày
    # Cuối tuần: ~3,000 - 5,000 đơn/ngày
    df_days = df_days \
        .withColumn("dow", expr("dayofweek(created_at_date)")) \
        .withColumn("is_weekend", expr("dow = 1 OR dow = 7")) \
        .withColumn("daily_orders", 
                    when(col("is_weekend"), expr("cast(4000 + rand() * 2000 as int)"))
                    .otherwise(expr("cast(12000 + rand() * 2000 as int)")))

    # 2.3. Nhân bản dòng (Explode) để tạo ra đúng số lượng đơn hàng cho mỗi ngày
    # Đây là kỹ thuật cực kỳ tối ưu của Spark để sinh dữ liệu Time-series
    df_base = df_days \
        .withColumn("dummy", expr("explode(array_repeat(1, daily_orders))")) \
        .drop("dummy") \
        .withColumn("row_id", expr("monotonically_increasing_id()")) \
        .withColumn("id_str", col("row_id").cast("string"))

    # Random Customers & Services
    customers_expr = "CASE WHEN rand() < 0.3 THEN 'CUST_001' WHEN rand() < 0.6 THEN 'CUST_002' WHEN rand() < 0.8 THEN 'CUST_003' ELSE 'CUST_004' END"
    services_expr = "CASE WHEN rand() < 0.5 THEN 'SRV_EMS' WHEN rand() < 0.8 THEN 'SRV_STD' ELSE 'SRV_EXP' END"
    pos_expr = "CASE WHEN rand() < 0.4 THEN 'POS_HN1' WHEN rand() < 0.7 THEN 'POS_HCM1' WHEN rand() < 0.9 THEN 'POS_DN1' ELSE 'POS_HN2' END"
    
    df_fact = df_base \
        .withColumn("shipment_id", expr("concat('SHP_', id_str)")) \
        .withColumn("customer_id", expr(customers_expr)) \
        .withColumn("service_id", expr(services_expr)) \
        .withColumn("origin_pos_id", expr(pos_expr)) \
        .withColumn("dest_pos_id", expr(pos_expr)) \
        .withColumn("weight_kg", round(rand() * 20 + 0.5, 1))

    # Gán giờ tạo ngẫu nhiên trong ngày và thời gian giao hàng
    df_fact = df_fact \
        .withColumn("created_at", to_timestamp(expr("concat(created_at_date, ' ', cast(rand()*12 + 7 as int), ':', cast(rand()*59 as int), ':00')"))) \
        .withColumn("delivered_at", to_timestamp(expr("date_add(created_at_date, cast(rand() * 3 + 1 as int))")))
        
    # Tính tiền cước và thù lao
    df_revenue = df_fact \
        .withColumn("base_rate", when(col("service_id") == "SRV_EMS", 30000).when(col("service_id") == "SRV_STD", 20000).otherwise(50000)) \
        .withColumn("revenue_amount", col("base_rate") + (col("weight_kg") * 5000).cast("int")) \
        .select("shipment_id", "customer_id", "service_id", "origin_pos_id", "dest_pos_id", "revenue_amount", "weight_kg", "created_at")

    df_delivery = df_fact \
        .withColumn("delivery_fee", when(col("dest_pos_id").like("POS_HN%"), 10000).when(col("dest_pos_id").like("POS_HCM%"), 12000).otherwise(15000)) \
        .select("shipment_id", col("dest_pos_id").alias("delivery_pos_id"), "service_id", "delivery_fee", "delivered_at")

    # ==========================================
    # 3. LƯU XUỐNG DELTA TABLES
    # ==========================================
    print("Saving 10M records to raw_revenue... (Please wait)")
    df_revenue.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_revenue")
    
    print("Saving 10M records to raw_delivery... (Please wait)")
    df_delivery.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_delivery")

    print("Hoan tat sinh 10 TRIEU du lieu mau vao Unity Catalog!")

if __name__ == "__main__":
    main()
