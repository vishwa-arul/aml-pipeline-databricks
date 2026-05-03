# Databricks notebook source
from pyspark.sql.functions import *
df_raw=spark.read.format("delta").load("/Volumes/workspace/aml_project/aml_v2/silver/transaction_enriched/")


df_lctr_single=df_raw.filter("amount >= 200000")


df_lctr_single_se=df_lctr_single.withColumn("No_of_Txn",lit(1))\
    .withColumn("transaction_id",array(col("transaction_id").cast("string")))\
    .withColumn("report_type1",lit("Single LCTR"))\
    .select("sender_id","Txn_date","transaction_id","No_of_Txn","amount", "report_type1")
# display(df_lctr_single_se)

# df_lctr_single_re=df_lctr_single.withColumn("No_of_Txn",lit(1))\
#     .withColumn("sender_id",array(col("sender_id").cast("string")))\
#     .withColumn("report_type",lit("Single Receiver LCTR"))\
#     .select("receiver_id","Txn_date","sender_id","No_of_Txn","amount", "report_type")
# display(df_lctr_single_re)


df_lctr_agg_se=df_raw.groupBy("sender_id","Txn_date")\
    .agg(collect_set("transaction_id").alias("transaction_id_list"),\
        count("transaction_id").alias("No_of_txn"),\
        sum("amount").alias("amount"))\
    .filter("amount > 500000").withColumn("report_type",lit("Aggergated Sender LCTR"))
# display(df_lctr_agg_se)

df_lctr_agg_re=df_raw.groupBy("receiver_id","Txn_date")\
    .agg(collect_set("transaction_id").alias("transaction_id_list"),\
        count("transaction_id").alias("No_of_txn"),\
        sum("amount").alias("amount"))\
    .filter("amount > 500000").withColumn("report_type",lit("Aggergated Receiver LCTR"))
# display(df_lctr_agg_re)

df_lctr=df_lctr_single_se.union(df_lctr_agg_se).union(df_lctr_agg_re).withColumnRenamed("sender_id","customer_id")
display(df_lctr)

df_lctr.write.format("delta").mode("overwrite")\
    .partitionBy("Txn_date")\
        .save("/Volumes/workspace/aml_project/aml_v2/gold/LCTR_Report")
