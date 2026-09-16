# Databricks notebook source
# MAGIC %md
# MAGIC # Parse dbt run_results.json to Delta Table
# MAGIC Kịch bản này đọc file `run_results.json` sinh ra từ lệnh dbt run/build, 
# MAGIC bóc tách trạng thái của từng model và ghi log vào một bảng Delta (dbt_run_logs) 
# MAGIC để dễ dàng tạo dashboard theo dõi chất lượng và thời gian xử lý.

# COMMAND ----------
import json
import os
from datetime import datetime
from pyspark.sql import Row

# 1. Định nghĩa đường dẫn file run_results.json
# File này thường nằm trong folder `target` của thư mục dbt_project
dbt_project_dir = os.path.abspath("../dbt_project")
run_results_path = os.path.join(dbt_project_dir, "target", "run_results.json")

if not os.path.exists(run_results_path):
    print(f"⚠️ Không tìm thấy file {run_results_path}. Vui lòng chạy dbt run/build trước.")
    dbutils.notebook.exit("No run_results.json found")

# 2. Đọc và parse nội dung JSON
with open(run_results_path, "r", encoding="utf-8") as f:
    run_results = json.load(f)

metadata = run_results.get("metadata", {})
results = run_results.get("results", [])

invocation_id = metadata.get("invocation_id")
generated_at = metadata.get("generated_at")

# Kiểm tra args để xem có cờ --full-refresh hay không
args = run_results.get("args", {})
is_full_refresh = args.get("full_refresh", False)
load_type_val = "FULL" if is_full_refresh else "INCREMENTAL"

# Bóc tách thông tin từng model
parsed_records = []
for res in results:
    unique_id = res.get("unique_id", "")
    status = res.get("status", "")
    message = res.get("message", "")
    execution_time_raw = res.get("execution_time")
    execution_time = float(execution_time_raw) if execution_time_raw is not None else 0.0
    
    # Số dòng thay đổi (thường có trong dbt build của Delta)
    adapter_response = res.get("adapter_response") or {}
    rows_affected_raw = adapter_response.get("rows_affected")
    rows_affected = int(rows_affected_raw) if rows_affected_raw is not None else 0
    
    parsed_records.append(Row(
        invocation_id=invocation_id,
        generated_at=generated_at,
        load_type=load_type_val,
        unique_id=unique_id,
        status=status,
        message=message,
        execution_time_seconds=execution_time,
        rows_affected=rows_affected,
        logged_at=datetime.now()
    ))

print(f"✅ Đã parse được {len(parsed_records)} models từ run_results.json.")

# COMMAND ----------
# 3. Tạo DataFrame và lưu vào Delta Table (sử dụng catalog/schema của bạn)
if parsed_records:
    df_logs = spark.createDataFrame(parsed_records)
    
    table_name = "workspace.default.dbt_run_logs"
    
    # Append log mới vào bảng
    (df_logs.write
     .format("delta")
     .mode("append")
     .option("mergeSchema", "true")
     .saveAsTable(table_name))
     
    print(f"🎉 Đã lưu log thành công vào bảng {table_name}")
    display(df_logs)
else:
    print("Không có kết quả nào để lưu.")
