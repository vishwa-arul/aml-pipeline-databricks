# Databricks notebook source
from pyspark.sql.functions import col

# ✅ Step 1: Read from Silver (STREAMING)
df_raw = spark.readStream \
    .format("delta") \
    .load("/Volumes/workspace/aml_project/aml_v2/silver/transaction_clean/")

# ===============================
# ✅ Step 2: Create lookup tables
# ===============================

country_data = [
    ("IRAN", "HIGH", "IR"),
    ("NORTH KOREA", "HIGH", "KP"),
    ("USA", "LOW", "US"),
    ("INDIA", "MEDIUM", "IN"),
    ("PAKISTAN", "HIGH", "PK")
]
df_country = spark.createDataFrame(country_data, ["country", "country_risk_level", "country_code"])

watchlist_data = [
    ("CUST1001", "TERROR"),
    ("CUST2002", "SANCTION"),
    ("CUST1015", "TERROR"),
    ("CUST2014", "SANCTION"),
    ("CUST2015", "SANCTION"),
    ("CUST1016", "TERROR"),
    ("CUST2018", "SANCTION"),
    ("CUST2011", "TERROR"),
    ("CUST1020", "SANCTION")
]

df_watchlist = spark.createDataFrame(watchlist_data, ["customer_id", "watchlist_type"]).dropDuplicates()

customer_data = [
    ("CUST1001", "HIGH"),
    ("CUST1002", "LOW"),
    ("CUST2002", "MEDIUM"),
    ("CUST1015", "LOW"),
    ("CUST2014", "LOW"),
    ("CUST2015", "MEDIUM"),
    ("CUST1016", "HIGH"),
    ("CUST2018", "HIGH"),
    ("CUST2011", "HIGH"),
    ("CUST1020", "MEDIUM")
]

df_customer = spark.createDataFrame(customer_data, ["customer_id", "customer_risk"]).dropDuplicates()

# ===============================
# ✅ Step 3: Joins (STREAMING + STATIC)
# ===============================

df_enriched = df_raw \
    .join(df_customer.withColumnRenamed("customer_id", "sender_id")
                      .withColumnRenamed("customer_risk", "sender_risk"),
          on="sender_id", how="left") \
    .join(df_customer.withColumnRenamed("customer_id", "receiver_id")
                      .withColumnRenamed("customer_risk", "receiver_risk"),
          on="receiver_id", how="left") \
    .join(df_watchlist.withColumnRenamed("customer_id", "sender_id")
                      .withColumnRenamed("watchlist_type", "sender_watchlist"),
          on="sender_id", how="left") \
    .join(df_watchlist.withColumnRenamed("customer_id", "receiver_id")
                      .withColumnRenamed("watchlist_type", "receiver_watchlist"),
          on="receiver_id", how="left") \
    .join(df_country, on="country", how="left")

# ===============================
# ✅ Step 4: Write to Gold (STREAMING)
# ===============================

query = df_enriched.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/workspace/aml_project/aml_v2/silver_enrich_checkpoint") \
    .trigger(availableNow=True) \
    .start("/Volumes/workspace/aml_project/aml_v2/silver/transaction_enriched/")

query.awaitTermination()