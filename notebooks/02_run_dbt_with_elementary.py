# Databricks notebook source
# MAGIC %md
# MAGIC # Run dbt and Generate Elementary Report
# MAGIC Notebook này thực thi dbt pipeline và Elementary data observability.
# MAGIC Đã được cấu hình Fail-Fast: nếu `dbt build` hoặc `edr report` lỗi, task sẽ NGỪNG NGAY và báo FAILED (Đỏ) trên Databricks Workflow.

# COMMAND ----------
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------
import os
import sys
import shutil
import subprocess
import datetime

# -------------------------------------------------------------------------
# 1. NHẬN CREDENTIALS & CẤU HÌNH TỪ TASK PARAMETERS / WIDGETS
# -------------------------------------------------------------------------
dbutils.widgets.text("DATABRICKS_HOST", "", "Databricks Host")
dbutils.widgets.text("DATABRICKS_HTTP_PATH", "", "Cluster/Warehouse HTTP Path")
dbutils.widgets.text("DATABRICKS_TOKEN", "", "Personal Access Token")
dbutils.widgets.text("TARGET_VOLUME_PATH", "/Volumes/raw/default/dbt_reports", "Volume Report Path")

host = dbutils.widgets.get("DATABRICKS_HOST").strip()
http_path = dbutils.widgets.get("DATABRICKS_HTTP_PATH").strip()
token = dbutils.widgets.get("DATABRICKS_TOKEN").strip()
volume_path = dbutils.widgets.get("TARGET_VOLUME_PATH").strip()

if host:
    os.environ["DATABRICKS_HOST"] = host
if http_path:
    os.environ["DATABRICKS_HTTP_PATH"] = http_path
if token:
    os.environ["DATABRICKS_TOKEN"] = token

# -------------------------------------------------------------------------
# 2. CHUẨN BỊ THƯ MỤC ~/.dbt ĐỂ ELEMENTARY CLI (edr) TÌM THẤY PROFILES
# -------------------------------------------------------------------------
home_dbt = os.path.expanduser("~/.dbt")
os.makedirs(home_dbt, exist_ok=True)

repo_dir = os.path.abspath("..")
dbt_project_dir = os.path.join(repo_dir, "dbt_project")
source_profiles = os.path.join(dbt_project_dir, "profiles.yml")
target_profiles = os.path.join(home_dbt, "profiles.yml")

shutil.copyfile(source_profiles, target_profiles)
os.environ["DBT_PROFILES_DIR"] = home_dbt

print(f"✅ Đã đồng bộ profiles.yml vào: {target_profiles}")

# -------------------------------------------------------------------------
# 3. HÀM CHẠY LỆNH (CHO PHÉP CHECK=FALSE ĐỂ TIẾP TỤC TẠO BÁO CÁO KHI DBT FAIL)
# -------------------------------------------------------------------------
def run_command(command, cwd=dbt_project_dir, check=True):
    print(f"\n=======================================================")
    print(f"🚀 RUNNING: {command}")
    print(f"📁 CWD: {cwd}")
    print(f"=======================================================\n")
    
    process = subprocess.Popen(
        command,
        shell=True,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=os.environ.copy()
    )

    for line in iter(process.stdout.readline, ''):
        sys.stdout.write(line)
        sys.stdout.flush()

    process.stdout.close()
    return_code = process.wait()

    if return_code != 0:
        error_msg = f"⚠️ Command finished with non-zero exit code {return_code}: {command}"
        print(error_msg, file=sys.stderr)
        
        # Tự động in file edr.log ra màn hình nếu có lỗi từ edr
        edr_log_path = os.path.join(cwd, "edr.log")
        if "edr " in command and os.path.exists(edr_log_path):
            print("\n" + "="*20 + " EDR.LOG CÓ THỂ CHỨA LỖI CHI TIẾT " + "="*20, file=sys.stderr)
            with open(edr_log_path, "r") as f:
                print(f.read(), file=sys.stderr)
            print("="*75 + "\n", file=sys.stderr)
            
        if check:
            raise RuntimeError(error_msg)

    return return_code

# -------------------------------------------------------------------------
# 4. THỰC THI DBT & ELEMENTARY (VẪN XUẤT REPORT CHI TIẾT KỂ CẢ KHI DBT FAIL)
# -------------------------------------------------------------------------
# Bước 4.1: Cài đặt dependencies (Bắt buộc pass mới build được)
run_command("dbt deps", check=True)

# Bước 4.2: Build models & tests (Không dừng ngay để Elementary kịp ghi nhận kết quả test thất bại)
dbt_build_code = run_command("dbt build", check=False)

# Bước 4.3: Luôn tạo Elementary Report để phân tích nguyên nhân lỗi (Failures/Errors)
os.makedirs("/tmp/edr", exist_ok=True)
report_local_path = "/tmp/edr/elementary_report.html"
edr_code = run_command(f"edr report --profiles-dir {home_dbt} --file-path {report_local_path}", check=False)

# -------------------------------------------------------------------------
# 5. LƯU BÁO CÁO VÀO VOLUME HOẶC DBFS ĐỂ USER CÓ THỂ MỞ CHECK
# -------------------------------------------------------------------------
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
saved_location = None

if os.path.exists(report_local_path):
    # Thử lưu vào Unity Catalog Volume (tương thích Serverless)
    if volume_path:
        try:
            os.makedirs(volume_path, exist_ok=True)
            dest_path = os.path.join(volume_path, f"elementary_report_{timestamp}.html")
            shutil.copyfile(report_local_path, dest_path)
            saved_location = dest_path
            print(f"\n📊 [ELEMENTARY REPORT] Đã lưu báo cáo tại Volume: {dest_path}")
        except Exception as e:
            print(f"⚠️ Không thể lưu vào Volume ({volume_path}): {e}")

    # Fallback: Lưu vào DBFS qua dbutils (cổ điển, nằm ngoài Git)
    # File lưu tại dbfs:/FileStore/... có thể được truy cập và tải xuống qua URL:
    # https://<workspace-url>/files/dbt_reports/elementary_report_xxxx.html
    if not saved_location:
        dbfs_path_relative = f"dbt_reports/elementary_report_{timestamp}.html"
        dbfs_target = f"dbfs:/FileStore/{dbfs_path_relative}"
        try:
            dbutils.fs.cp(f"file:{report_local_path}", dbfs_target)
            saved_location = dbfs_target
            
            print(f"\n📊 [ELEMENTARY REPORT] Đã lưu báo cáo tại DBFS FileStore (Nằm ngoài Git): {dbfs_target}")
            print(f"👉 ĐỂ TẢI BÁO CÁO: Hãy thêm '/files/{dbfs_path_relative}' vào sau tên miền Databricks của bạn trên trình duyệt.")
            print(f"   Ví dụ: https://<your-workspace>.cloud.databricks.com/files/{dbfs_path_relative}")
        except Exception as e:
            print(f"⚠️ Không thể lưu vào DBFS qua dbutils: {e}")
else:
    print("⚠️ Không tìm thấy file báo cáo /tmp/edr/elementary_report.html để lưu.")

# -------------------------------------------------------------------------
# 6. ĐÁNH DẤU THẤT BẠI (BÁO ĐỎ TASK) NẾU DBT CÓ LỖI, KÈM ĐƯỜNG DẪN BÁO CÁO
# -------------------------------------------------------------------------
if dbt_build_code != 0:
    raise RuntimeError(
        f"\n❌ [PIPELINE FAILED] dbt build thất bại với Exit Code: {dbt_build_code}!\n"
        f"🔍 Báo cáo chi tiết nguyên nhân lỗi đã được Elementary tạo tại:\n"
        f"👉 {saved_location or 'Không thể lưu file báo cáo'}\n"
        f"Hãy mở báo cáo HTML trên để xem chi tiết model/data test nào bị lỗi."
    )

if edr_code != 0:
    raise RuntimeError(f"❌ Elementary edr report thất bại với Exit Code: {edr_code}!")

print(f"\n🎉 Toàn bộ dbt pipeline và Elementary data tests đều THÀNH CÔNG! Báo cáo: {saved_location}")


