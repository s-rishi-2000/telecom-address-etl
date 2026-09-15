# Databricks notebook source
# DBTITLE 1,Conform geolink silver records
from pyspark.sql.functions import col, current_timestamp, to_json, struct, lit, trim, upper, regexp_replace, round, when, coalesce
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

vendor_name = "geolink"
layer_name = "silver_conform"

print(f"--- Starting Silver Conformance for: {vendor_name} ---")

# 1. Read directly from the Bronze table
df_bronze = spark.table("inlap.bronze.geolink")

# 2. Normalize address registry fields while preserving nullable city/state/zip for thin-coverage areas
df_cleaned = (
    df_bronze
    .withColumn("baseglid", trim(col("baseglid")))
    .withColumn("glid", trim(col("glid")))
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn("full_address", trim(col("full_address")))
    .withColumn("unit_number", trim(col("unit_number")))
    .withColumn("city", upper(trim(col("city"))))
    .withColumn("state", upper(trim(col("state"))))
    .withColumn(
        "zip",
        when(col("zip").isNull(), lit(None).cast("string"))
        .otherwise(regexp_replace(col("zip").cast("string"), "\\.0$", ""))
    )
    .withColumn("location_type", upper(trim(col("location_type"))))
    .withColumn("confidence_score", col("confidence_score").cast("double"))
    .withColumn("last_verified", col("last_verified").cast("date"))
    .withColumn("lat_rounded", round(col("latitude"), 4))
    .withColumn("lon_rounded", round(col("longitude"), 4))
    .withColumn("location_glid", coalesce(col("baseglid"), col("glid")))
)

# 3. Define quality rules for the registry keys and coordinate/address essentials
valid_condition = (
    col("glid").isNotNull()
    & col("location_glid").isNotNull()
    & col("full_address").isNotNull()
    & (col("full_address") != "")
    & col("latitude").isNotNull()
    & col("longitude").isNotNull()
    & col("latitude").between(-90.0, 90.0)
    & col("longitude").between(-180.0, 180.0)
    & col("confidence_score").isNotNull()
)

df_passed_dq = df_cleaned.filter(valid_condition)
df_dq_quarantine = df_cleaned.filter(~valid_condition).withColumn(
    "failure_reason",
    lit("missing GLID/address, invalid coordinates, or null confidence score")
)

# 4. Remove duplicate registry rows while preserving legitimate multi-unit GLIDs at the same coordinate
window_spec = Window.partitionBy("glid").orderBy(
    col("confidence_score").desc(),
    col("last_verified").desc(),
    col("baseglid").asc(),
)

df_with_rn = df_passed_dq.withColumn("row_num", row_number().over(window_spec))

df_valid = df_with_rn.filter(col("row_num") == 1).drop("row_num")
df_dup_quarantine = df_with_rn.filter(col("row_num") > 1).drop("row_num").withColumn(
    "failure_reason",
    lit("duplicate GLID registry row"),
)

# 5. Combine quarantine records and preserve the raw bronze payload
df_all_quarantine = df_dq_quarantine.unionByName(df_dup_quarantine)
df_quarantine_final = (
    df_all_quarantine
    .withColumn("source_vendor", lit(vendor_name))
    .withColumn("quarantine_timestamp", current_timestamp())
    .withColumn("raw_record", to_json(struct([col(c) for c in df_bronze.columns])))
    .select("raw_record", "failure_reason", "source_vendor", "quarantine_timestamp")
)

# 6. Refresh Silver Conformed and append quarantine rows
(
    df_valid.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("inlap.silver.geolink_conformed")
)

(
    df_quarantine_final.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("inlap.silver.quarantine_records")
)

# 7. Log Audit Metrics
rows_read = df_bronze.count()
rows_passed = df_valid.count()
rows_quarantined = df_quarantine_final.count()

spark.sql(f"""
    INSERT INTO inlap.control.audit_log 
    VALUES (
        '{vendor_name}', 
        '{layer_name}', 
        current_timestamp(), 
        'SUCCESS', 
        {rows_read}, 
        {rows_passed}, 
        {rows_quarantined}
    )
""")

print(f"Conformance complete for {vendor_name}. Read: {rows_read} | Passed: {rows_passed} | Quarantined: {rows_quarantined}")

display(df_valid.orderBy(col("last_verified").desc(), col("glid").asc()).limit(5))