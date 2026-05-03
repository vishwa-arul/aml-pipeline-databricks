# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ---------------- READ DATA ----------------
df_raw = spark.read.format("delta").load(
    "/Volumes/workspace/aml_project/aml_v2/silver/transaction_enriched/"
)

# ---------------- STEP 1: CREATE TIMESTAMP ----------------
df_raw = df_raw.withColumn(
    "txn_timestamp",
    to_timestamp(concat_ws(" ", col("Txn_date"), col("Txn_time")))
).withColumn(
    "txn_unix",
    unix_timestamp("txn_timestamp")
)

# ---------------- CONFIG ----------------
TXN_THRESHOLD = 10
FREQ_TXN_COUNT = 5
WINDOW_SECONDS = 90 * 60

# ---------------- 1. HIGH VOLUME TXN ----------------
df_velocity_1 = df_raw.groupBy("sender_id", "Txn_date") \
    .agg(
        count("*").alias("No_of_Txn"),
        collect_set("transaction_id").alias("txn_id_list")
    ) \
    .filter(col("No_of_Txn") >= TXN_THRESHOLD) \
    .withColumn("Suspicious_type", lit("High Volume Txn")) \
    .withColumnRenamed("sender_id", "customer_id")

# ---------------- 2. FREQUENT TXN (90 MIN WINDOW) ----------------
wnd_time = Window.partitionBy("sender_id") \
    .orderBy("txn_unix") \
    .rangeBetween(-WINDOW_SECONDS, 0)

df_velocity_2 = df_raw \
    .withColumn("txn_count_90min", count("*").over(wnd_time)) \
    .filter(col("txn_count_90min") >= FREQ_TXN_COUNT) \
    .groupBy("sender_id", "Txn_date") \
    .agg(
        count("*").alias("No_of_Txn"),
        collect_set("transaction_id").alias("txn_id_list")
    ) \
    .withColumn("Suspicious_type", lit("Frequent Txn (90 min)")) \
    .withColumnRenamed("sender_id", "customer_id")

# ---------------- 3. HIGH RISK COUNTRY ----------------
df_country = df_raw.filter(col("country_risk_level") == "HIGH") \
    .groupBy("sender_id", "Txn_date") \
    .agg(
        count("*").alias("No_of_Txn"),
        collect_set("transaction_id").alias("txn_id_list")
    ) \
    .withColumn("Suspicious_type", lit("High Risk Country")) \
    .withColumnRenamed("sender_id", "customer_id")

# ---------------- 4. SENDER HIGH RISK ----------------
df_risk_sender = df_raw.filter(col("sender_risk") == "HIGH") \
    .groupBy("sender_id", "Txn_date") \
    .agg(
        count("*").alias("No_of_Txn"),
        collect_set("transaction_id").alias("txn_id_list")
    ) \
    .withColumn("Suspicious_type", lit("Sender High Risk")) \
    .withColumnRenamed("sender_id", "customer_id")

# ---------------- 5. RECEIVER HIGH RISK ----------------
df_risk_receiver = df_raw.filter(col("receiver_risk") == "HIGH") \
    .groupBy("receiver_id", "Txn_date") \
    .agg(
        count("*").alias("No_of_Txn"),
        collect_set("transaction_id").alias("txn_id_list")
    ) \
    .withColumn("Suspicious_type", lit("Receiver High Risk")) \
    .withColumnRenamed("receiver_id", "customer_id")

# ---------------- 6. WATCHLIST MATCH ----------------
df_watchlist = df_raw.filter(col("sender_watchlist").isNotNull()) \
    .groupBy("sender_id", "Txn_date") \
    .agg(
        count("*").alias("No_of_Txn"),
        collect_set("transaction_id").alias("txn_id_list")
    ) \
    .withColumn("Suspicious_type", lit("Watchlist Match")) \
    .withColumnRenamed("sender_id", "customer_id")

# ---------------- FINAL UNION ----------------
df_str = df_velocity_1 \
    .unionByName(df_velocity_2) \
    .unionByName(df_country) \
    .unionByName(df_risk_sender) \
    .unionByName(df_risk_receiver) \
    .unionByName(df_watchlist)

# ---------------- ADD METADATA ----------------
df_str = df_str \
    .withColumn("run_date", current_date()) \
    .withColumn("risk_level", lit("HIGH")) \
    .withColumn("alert_id", monotonically_increasing_id())

# ---------------- OPTIONAL DEDUP ----------------
df_str = df_str.dropDuplicates([
    "customer_id", "Txn_date", "Suspicious_type"
])

# ---------------- WRITE OUTPUT ----------------
df_str.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("Txn_date") \
    .save("/Volumes/workspace/aml_project/aml_v2/gold/STR_Report/")