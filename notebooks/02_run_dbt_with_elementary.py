# Databricks notebook source
# MAGIC %md
# MAGIC # Run dbt and Generate Elementary Report
# MAGIC This notebook is designed to be run as a Databricks Job Task. It installs the necessary dependencies, runs dbt, and then generates an Elementary Data Observability report.

# COMMAND ----------
# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------
# MAGIC %sh
# MAGIC cd ../dbt_project
# MAGIC 
# MAGIC # 1. Install dbt packages (including elementary)
# MAGIC dbt deps
# MAGIC 
# MAGIC # 2. Run the dbt project
# MAGIC dbt build
# MAGIC 
# MAGIC # 3. Generate the Elementary report
# MAGIC # This reads the run_results.json from the dbt build and generates edr_target/elementary_report.html
# MAGIC edr report
# MAGIC 
# MAGIC # 4. Copy the report to a persistent location (DBFS or Volumes) so it isn't lost
# MAGIC # Replace this path with your desired persistent storage path if needed
# MAGIC mkdir -p /dbfs/FileStore/dbt_reports
# MAGIC cp edr_target/elementary_report.html /dbfs/FileStore/dbt_reports/elementary_report_$(date +%Y%m%d_%H%M%S).html
# MAGIC echo "Report saved to DBFS: /FileStore/dbt_reports/"
