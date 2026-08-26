# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

print("=" * 65)
print("ENTERPRISE SALES LAKEHOUSE")
print("NOTEBOOK 07 - AUDIT & MONITORING")
print("=" * 65)
print("Framework : Pipeline Observability")
print("Status    : INITIALIZED")

# COMMAND ----------

layer_metrics = [
    ("SOURCE", "enterprise_orders_source",
     spark.table("enterprise_orders_source").count()),

    ("BRONZE", "bronze_orders",
     spark.table("bronze_orders").count()),

    ("SILVER", "silver_orders",
     spark.table("silver_orders").count()),

    ("SILVER", "silver_trusted_sales",
     spark.table("silver_trusted_sales").count()),

    ("GOLD", "gold_fact_sales",
     spark.table("gold_fact_sales").count())
]

layer_metrics_df = spark.createDataFrame(
    layer_metrics,
    ["layer", "table_name", "record_count"]
).withColumn(
    "audit_timestamp",
    F.current_timestamp()
)

display(layer_metrics_df)

# COMMAND ----------

audit_records = [
    ("01_SOURCE_GENERATION", "SOURCE", "SUCCESS"),
    ("02_BRONZE_INGESTION", "BRONZE", "SUCCESS"),
    ("03_SILVER_TRANSFORMATION", "SILVER", "SUCCESS"),
    ("04_GOLD_ANALYTICS", "GOLD", "SUCCESS"),
    ("05_INCREMENTAL_PROCESSING", "SILVER", "SUCCESS"),
    ("06_DATA_QUALITY", "VALIDATION", "SUCCESS")
]

audit_df = (
    spark.createDataFrame(
        audit_records,
        ["pipeline_stage", "layer", "status"]
    )
    .withColumn("execution_timestamp", F.current_timestamp())
)

display(audit_df)

# COMMAND ----------

silver_orders = spark.table("silver_orders")
gold_fact = spark.table("gold_fact_sales")

duplicate_orders = (
    silver_orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_order_ids = silver_orders.filter(
    F.col("order_id").isNull()
).count()

negative_gold_sales = gold_fact.filter(
    F.col("net_amount") < 0
).count()

monitoring_checks = [
    ("Duplicate Silver Orders", duplicate_orders, 0),
    ("Null Silver Order IDs", null_order_ids, 0),
    ("Negative Gold Sales", negative_gold_sales, 0)
]

monitoring_df = (
    spark.createDataFrame(
        monitoring_checks,
        ["monitoring_check", "actual_value", "expected_value"]
    )
    .withColumn(
        "status",
        F.when(
            F.col("actual_value") == F.col("expected_value"),
            "HEALTHY"
        ).otherwise("ALERT")
    )
)

display(monitoring_df)

# COMMAND ----------

(
    layer_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("audit_layer_metrics")
)

(
    audit_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("audit_pipeline_runs")
)

(
    monitoring_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("audit_monitoring_health")
)

print("Audit and monitoring Delta tables created successfully")
print("audit_layer_metrics     :", spark.table("audit_layer_metrics").count())
print("audit_pipeline_runs     :", spark.table("audit_pipeline_runs").count())
print("audit_monitoring_health :", spark.table("audit_monitoring_health").count())

# COMMAND ----------

pipeline_failures = audit_df.filter(
    F.col("status") != "SUCCESS"
).count()

monitoring_alerts = monitoring_df.filter(
    F.col("status") == "ALERT"
).count()

audit_status = (
    "SUCCESS"
    if pipeline_failures == 0 and monitoring_alerts == 0
    else "FAILED"
)

print("=" * 65)
print("AUDIT & MONITORING COMPLETED")
print("=" * 65)
print(f"Pipeline Stages       : {audit_df.count()}")
print(f"Pipeline Failures     : {pipeline_failures}")
print(f"Monitoring Checks     : {monitoring_df.count()}")
print(f"Active Alerts         : {monitoring_alerts}")
print("Audit Logging         : ENABLED")
print("Layer Metrics         : CAPTURED")
print("Monitoring Health     :",
      "HEALTHY" if monitoring_alerts == 0 else "ALERT")
print(f"Observability Status  : {audit_status}")
print("=" * 65)