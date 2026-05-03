# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, from_json

#  Step 1: Define schema (same as yours)
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("sender_id", StringType(), True),
    StructField("receiver_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

#  Step 2: Read from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "<KAFKA_SERVER>") \
    .option("subscribe", "Transactions") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='<API_KEY>' password='<SECRET_KEY>';") \
    .option("startingOffsets", "earliest") \
    .load()

#  Step 3: Convert binary → string
df_string = df_kafka.selectExpr("CAST(value AS STRING) as message")

#  Step 4: Parse JSON → columns
df_parsed = df_string.withColumn(
    "data",
    from_json(col("message"), schema)
)

#  Step 5: Flatten structure
df_bronze = df_parsed.select("data.*") \
    .withColumn("ingestion_time", current_timestamp())

# (Optional) add Kafka metadata
df_bronze = df_bronze.withColumn("kafka_offset", col("transaction_id")).drop(col("data"))

# display(df_bronze)  # streaming display (optional)

# Step 6: Write to Delta (STREAMING)
query = df_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/Volumes/workspace/aml_project/aml_v2/bronze_checkpoint") \
    .trigger(availableNow=True) \
    .start("/Volumes/workspace/aml_project/aml_v2/bronze/Transactions")

query.awaitTermination()