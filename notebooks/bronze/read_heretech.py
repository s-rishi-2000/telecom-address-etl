# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# DBTITLE 1,Load bronze utilities
# MAGIC %run /Users/rushi@vishaldamale4820gmail.onmicrosoft.com/telecom-address-etl/notebooks/bronze/bronze_pipeline_utils

# COMMAND ----------

# Call the shared utility function for HERE Technologies
ingest_vendor_bronze(
    vendor_name="here_tech",
    file_path="abfss://datalake@attinlapsa.dfs.core.windows.net/vendors/here_tech.csv",
    table_name="inlap.bronze.heretech",
    date_column_name="last_updated"
)