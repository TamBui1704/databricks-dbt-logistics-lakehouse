# Databricks notebook source
# MAGIC %md
# MAGIC # Run dbt Pipeline
# MAGIC Chạy lệnh dbt build và đảm bảo file `run_results.json` được sinh ra đúng trong thư mục `target/` của Repo.

# COMMAND ----------
import os
import subprocess
import sys

# Chuyển hướng thư mục làm việc (CWD) tới dbt_project trong Git
dbt_project_dir = os.path.abspath("../dbt_project")
os.chdir(dbt_project_dir)

print(f"📁 CWD: {os.getcwd()}")

# Lấy các tham số chứng thực để truyền cho profiles.yml
dbutils.widgets.text("DATABRICKS_HOST", "", "Databricks Host")
dbutils.widgets.text("DATABRICKS_HTTP_PATH", "", "Databricks HTTP Path")
dbutils.widgets.text("DATABRICKS_TOKEN", "", "Databricks Token")

os.environ["DATABRICKS_HOST"] = dbutils.widgets.get("DATABRICKS_HOST")
os.environ["DATABRICKS_HTTP_PATH"] = dbutils.widgets.get("DATABRICKS_HTTP_PATH")
os.environ["DATABRICKS_TOKEN"] = dbutils.widgets.get("DATABRICKS_TOKEN")

# Cài đặt thư viện dbt-databricks
print("🚀 Cài đặt dbt...")
subprocess.run([sys.executable, "-m", "pip", "install", "-r", "../requirements.txt", "--quiet"], check=True)

# Chạy lệnh dbt deps
print("🚀 Chạy dbt deps...")
subprocess.run(["dbt", "deps"], check=True)

# Chạy lệnh dbt build
print("🚀 Chạy dbt build...")
process = subprocess.run(["dbt", "build"], capture_output=False)

if process.returncode != 0:
    print(f"⚠️ dbt build hoàn tất nhưng có một số model bị lỗi (Exit code: {process.returncode}).")
    print("Vui lòng chạy Task 2 (Parse log) để xem chi tiết model nào bị lỗi trên bảng dbt_run_logs.")
    # Ta không raise Exception ở đây để Task 2 (Parse Log) có thể tiếp tục được chạy và ghi nhận lỗi
else:
    print("🎉 dbt build hoàn tất thành công 100%!")
