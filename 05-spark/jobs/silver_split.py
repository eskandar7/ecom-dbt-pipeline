from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "ecom-dev-pipeline"

SILVER_CURATED_ORDERS = f"gs://{BUCKET}/silver/orders_curated/dt=*/hr=*"
SILVER_ORDERS_HEADER = f"gs://{BUCKET}/silver/orders_header"
SILVER_ORDER_ITEMS   = f"gs://{BUCKET}/silver/order_items"

spark = (
    SparkSession.builder
    .appName("silver-curated-to-silver-modeled")
    .getOrCreate()
)

print("📖 Reading curated Silver orders (nested) ...")
df = spark.read.parquet(SILVER_CURATED_ORDERS)

# ✅ Always re-extract dt/hr from file name (robust)
df = df.withColumn("_file", F.input_file_name())
df = df.withColumn("dt", F.regexp_extract(F.col("_file"), r"dt=([0-9\-]+)", 1))
df = df.withColumn("hr", F.regexp_extract(F.col("_file"), r"hr=([0-9]{2})", 1))

print("🔨 Building orders_header...")
orders_header = df.select(
    F.col("event_ts").alias("event_ts"),
    F.col("event_type").alias("event_type"),
    F.col("order.order_id").alias("order_id"),
    F.col("order.customer_id").alias("customer_id"),
    F.col("order.country").alias("country"),
    F.col("order.currency").alias("currency"),
    F.col("order.amount").alias("amount"),
    F.col("order.payment_method").alias("payment_method"),
    F.col("dt").alias("dt"),
    F.col("hr").alias("hr"),
)

print("🔨 Building order_items...")
order_items = (
    df.withColumn("item", F.explode_outer(F.col("order.items")))
      .select(
          F.col("order.order_id").alias("order_id"),
          F.col("item.sku").alias("sku"),
          F.col("item.qty").cast("long").alias("qty"),
          F.col("item.unit_price").alias("unit_price"),
          F.col("dt").alias("dt"),
          F.col("hr").alias("hr"),
      )
)

print("💾 Writing orders_header...")
orders_header.write.mode("append").partitionBy("dt", "hr").parquet(SILVER_ORDERS_HEADER)

print("💾 Writing order_items...")
order_items.write.mode("append").partitionBy("dt", "hr").parquet(SILVER_ORDER_ITEMS)

print("✅ Modeled Silver done")
spark.stop()
