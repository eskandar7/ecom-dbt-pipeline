"""
bronze_to_silver.py
===================
Real-world: Bronze -> Silver (curated)

- Reads raw Bronze JSONL events from GCS (partitioned by dt/hr in path)
- Adds dt/hr columns from file path
- Enforces basic types
- Writes Parquet to Silver as a curated dataset (still nested order + items)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "ecom-dev-pipeline"

BRONZE_PATH = f"gs://{BUCKET}/bronze/orders/dt=*/hr=*/part-*.jsonl"

# Curated silver dataset (still nested)
SILVER_CURATED_ORDERS = f"gs://{BUCKET}/silver/orders_curated"

spark = (
    SparkSession.builder
    .appName("bronze-to-silver-curated-orders")
    .getOrCreate()
)

print("📖 Reading Bronze JSONL...")
df = spark.read.json(BRONZE_PATH)

# Add dt/hr from file path
df = df.withColumn("_file", F.input_file_name())
df = df.withColumn("dt", F.regexp_extract(F.col("_file"), r"dt=([0-9\-]+)", 1))
df = df.withColumn("hr", F.regexp_extract(F.col("_file"), r"hr=([0-9]{2})", 1))

# Minimal "curation": keep the event + nested order as-is, but enforce types
curated = (
    df.select(
        F.col("event_ts").alias("event_ts"),
        F.col("event_type").alias("event_type"),

        # Keep nested order (including items) for later modeling
        F.col("order").alias("order"),

        F.col("dt").alias("dt"),
        F.col("hr").alias("hr"),
    )
    # Optional type guards (adjust if your schema differs)
    .withColumn("event_ts", F.to_timestamp("event_ts"))
)

print("💾 Writing curated orders to Silver (nested) ...")
curated.write.mode("append").partitionBy("dt", "hr").parquet(SILVER_CURATED_ORDERS)

print("✅ Done")
print(f"   Output: {SILVER_CURATED_ORDERS}")

spark.stop()
