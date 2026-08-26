# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

print("=" * 65)
print("ENTERPRISE SALES LAKEHOUSE")
print("NOTEBOOK 05 - INCREMENTAL SALES PROCESSING")
print("=" * 65)
print("Pattern : Incremental Load + Delta MERGE")
print("Status  : INITIALIZED")

# COMMAND ----------

current_orders = spark.table("silver_orders")

before_count = current_orders.count()

print(f"Current Silver Orders : {before_count}")
print(f"Distinct Order IDs    : {current_orders.select('order_id').distinct().count()}")

# COMMAND ----------

incremental_data = [
    (
        "ORD000501", "CUST0001", "STORE001",
        datetime(2025, 12, 31, 10, 30),
        "Completed", "UPI"
    ),
    (
        "ORD000502", "CUST0025", "STORE004",
        datetime(2025, 12, 31, 11, 15),
        "Completed", "Credit Card"
    ),
    (
        "ORD000503", "CUST0050", "STORE006",
        datetime(2025, 12, 31, 12, 45),
        "Pending", "Debit Card"
    ),
    (
        "ORD000010", "CUST0010", "STORE002",
        datetime(2025, 1, 15, 14, 30),
        "Completed", "UPI"
    )
]

incremental_schema = """
order_id STRING,
customer_id STRING,
store_id STRING,
order_timestamp TIMESTAMP,
order_status STRING,
payment_method STRING
"""

incremental_orders = spark.createDataFrame(
    incremental_data,
    incremental_schema
).withColumn(
    "order_date", F.to_date("order_timestamp")
).withColumn(
    "order_year", F.year("order_timestamp")
).withColumn(
    "order_month", F.month("order_timestamp")
).withColumn(
    "silver_processed_timestamp", F.current_timestamp()
)

display(incremental_orders)

# COMMAND ----------

target = DeltaTable.forName(spark, "silver_orders")

(
    target.alias("target")
    .merge(
        incremental_orders.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdate(set={
        "customer_id": "source.customer_id",
        "store_id": "source.store_id",
        "order_timestamp": "source.order_timestamp",
        "order_status": "source.order_status",
        "payment_method": "source.payment_method",
        "order_date": "source.order_date",
        "order_year": "source.order_year",
        "order_month": "source.order_month",
        "silver_processed_timestamp": "source.silver_processed_timestamp"
    })
    .whenNotMatchedInsert(values={
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "store_id": "source.store_id",
        "order_timestamp": "source.order_timestamp",
        "order_status": "source.order_status",
        "payment_method": "source.payment_method",
        "order_date": "source.order_date",
        "order_year": "source.order_year",
        "order_month": "source.order_month",
        "silver_processed_timestamp": "source.silver_processed_timestamp"
    })
    .execute()
)

print("Delta MERGE completed successfully")

# COMMAND ----------

after_orders = spark.table("silver_orders")

after_count = after_orders.count()
distinct_count = after_orders.select("order_id").distinct().count()

validation = spark.createDataFrame([
    ("Before MERGE", before_count),
    ("Incoming Batch", incremental_orders.count()),
    ("Expected New Records", 3),
    ("After MERGE", after_count),
    ("Distinct Order IDs", distinct_count)
], ["metric", "value"])

display(validation)

print(f"Expected final count : {before_count + 3}")
print(f"Actual final count   : {after_count}")

# COMMAND ----------

count_before_rerun = spark.table("silver_orders").count()

target = DeltaTable.forName(spark, "silver_orders")

(
    target.alias("target")
    .merge(
        incremental_orders.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdate(set={
        "customer_id": "source.customer_id",
        "store_id": "source.store_id",
        "order_timestamp": "source.order_timestamp",
        "order_status": "source.order_status",
        "payment_method": "source.payment_method",
        "order_date": "source.order_date",
        "order_year": "source.order_year",
        "order_month": "source.order_month",
        "silver_processed_timestamp": "source.silver_processed_timestamp"
    })
    .whenNotMatchedInsert(values={
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "store_id": "source.store_id",
        "order_timestamp": "source.order_timestamp",
        "order_status": "source.order_status",
        "payment_method": "source.payment_method",
        "order_date": "source.order_date",
        "order_year": "source.order_year",
        "order_month": "source.order_month",
        "silver_processed_timestamp": "source.silver_processed_timestamp"
    })
    .execute()
)

count_after_rerun = spark.table("silver_orders").count()

print("IDEMPOTENCY VALIDATION")
print("-" * 40)
print(f"Before Re-run : {count_before_rerun}")
print(f"After Re-run  : {count_after_rerun}")
print(f"Duplicates    : {count_after_rerun - count_before_rerun}")
print(
    "Status        :",
    "PASS" if count_before_rerun == count_after_rerun else "FAIL"
)

# COMMAND ----------

final_orders = spark.table("silver_orders")

duplicate_ids = (
    final_orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

expected_count = before_count + 3
actual_count = final_orders.count()

merge_status = (
    "SUCCESS"
    if actual_count == expected_count
    and duplicate_ids == 0
    and count_before_rerun == count_after_rerun
    else "FAILED"
)

print("=" * 65)
print("INCREMENTAL SALES PROCESSING COMPLETED")
print("=" * 65)
print(f"Initial Records      : {before_count}")
print(f"Incoming Records     : {incremental_orders.count()}")
print("New Records          : 3")
print("Updated Records      : 1")
print(f"Final Records        : {actual_count}")
print(f"Duplicate Order IDs  : {duplicate_ids}")
print("Processing Pattern   : DELTA MERGE")
print(
    "Idempotency          :",
    "PASSED" if count_before_rerun == count_after_rerun else "FAILED"
)
print(f"Incremental Status   : {merge_status}")
print("=" * 65)