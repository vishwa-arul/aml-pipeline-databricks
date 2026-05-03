# Databricks notebook source
from pyspark.sql.functions import upper, trim, to_date, date_format, current_timestamp, col

# ✅ Step 1: Read from Bronze (STREAMING)
df_raw = spark.readStream \
    .format("delta") \
    .load("/Volumes/workspace/aml_project/aml_v2/bronze/Transactions")

# ✅ Step 2: Deduplication
df_dedup = df_raw.dropDuplicates(["transaction_id"])

# ✅ Step 3: Remove nulls
df_nonull = df_dedup.dropna(
    subset=["transaction_id", "sender_id", "receiver_id", "amount", "timestamp"]
)

# ✅ Step 4: Fill nulls
df_fillnull = df_nonull.fillna({"country": "Unknown"})

# ✅ Step 5: Standardization
df_std = df_fillnull.withColumn("country", upper(col("country")))

# ✅ Step 6: Enrichment
df_enrich = df_std \
    .withColumn("Txn_date", to_date(col("timestamp"))) \
    .withColumn("Txn_time", date_format(col("timestamp"), 'HH:mm:ss')) \
    .withColumn("ing_date", to_date(col("ingestion_time"))) \
    .withColumn("ing_time", date_format(col("ingestion_time"), 'HH:mm:ss'))

# ✅ Step 7: Business filter
df_silver = df_enrich.filter("amount > 0")

# display(df_silver)  # optional streaming view

# ✅ Step 8: Write to Silver (STREAMING)
query = df_silver.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/workspace/aml_project/aml_v2/silver_checkpoint") \
    .trigger(availableNow=True) \
    .start("/Volumes/workspace/aml_project/aml_v2/silver/transaction_clean/")

query.awaitTermination()