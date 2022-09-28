import pandas as pd
import numpy as np
import seaborn as sns
import geopandas as gpd
from datetime import datetime
import matplotlib.pyplot as plt

import geopandas as gpd
import folium
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.functions import countDistinct, col, date_format
import numpy as np
import pyspark.sql.functions as func
from pyspark.sql.types import (
    StringType,
    LongType,
    DoubleType,
    StructField,
    StructType,
    FloatType,
    BooleanType
)

import warnings
warnings.filterwarnings("ignore")

# Start Spark Session
spark = (
    SparkSession.builder.appName("MAST30034 Project 2 BNPL")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "8g")
    .getOrCreate()
)

# load all transactions datasets
paths=['./data/tables/transactions_20210228_20210827_snapshot',
       './data/tables/transactions_20210828_20220227_snapshot',
       './data/tables/transactions_20220228_20220828_snapshot']

first = 1
for path in paths:
    if first:
        transactions = spark.read.parquet(path)
        print(f'added {path.split("/")[3]}')
        first = 0
    else:
        append_transactions = spark.read.parquet(path)
        transactions = transactions.union(append_transactions)
        print(f'added {path.split("/")[3]}')

consumer = spark.read.csv("./data/tables/tbl_consumer.csv", header=True, sep="|")
details = spark.read.parquet("./data/tables/consumer_user_details.parquet")
merchants = spark.read.parquet("./data/tables/tbl_merchants.parquet")

# rename columns
merchants = merchants.withColumnRenamed('name', 'merchant_name')
consumer = consumer.withColumnRenamed('name', 'consumer_name')

# Join consumers with their respective details
consumer_detail = consumer.join(details, on="consumer_id")

# Join consumers with their respective transactions
consumer_trx = consumer_detail.join(transactions, on="user_id")

# Join transactions with the respective merchants
df_trx = consumer_trx.join(merchants, on="merchant_abn")

@F.udf(StringType())
def normalize_tags(col):
    return col.replace("(", "[").replace(")", "]")[1:-1]

@F.udf(StringType())
def clean_string(col):
    col = col.lower()
    return " ".join(col.split())

df_trx = df_trx.withColumn("tags", normalize_tags(F.col("tags")))
df_trx = df_trx.withColumn("categories", F.regexp_extract("tags", "(?<=\[)(.*?)(?=\])", 1))
df_trx = df_trx.withColumn("revenue_level", F.regexp_extract("tags", "(?<=,\s\[)([a-e]+?)(?=\],)", 1))
df_trx = df_trx.withColumn("take_rate", F.regexp_extract("tags", "(?<=\[take rate: )(.*?)(?=\])", 1))
df_trx = df_trx.withColumn("take_rate", F.col("take_rate").astype(FloatType()))
df_trx = df_trx.withColumn("categories", clean_string(F.col("categories")))
df_trx = df_trx.where(F.col("dollar_value") >= 35)

@F.udf(FloatType())
def get_z(dollar, mean, sdev):
    if sdev == 0.0:
        return 0.0
    if sdev == None:
        return None
    z = (dollar - mean) / sdev
    return abs(z)

@F.udf(BooleanType())
def possible_fraud(mean_trx, z_score, count_trx):
    if mean_trx > ("ENTER THRESHOLD HERE"):
        if z_score > ("ENTER THRESHOLD HERE"):
            pass
    else:
        if count_trx > ("ENTER THRESHOLD HERE"):
            pass

df_mean_sd = df_trx.groupby("merchant_abn").agg(F.mean("dollar_value").alias("mean_trx"), \
    F.stddev("dollar_value").alias("stddev_trx"))
df_grouped = df_trx.groupby(["merchant_abn", "order_datetime"]).agg(F.sum("dollar_value").alias("sum_trx"), \
    F.count("dollar_value").alias("count_trx"))
df_grouped = df_grouped.join(df_mean_sd, on="merchant_abn")
df_grouped = df_grouped.withColumn("z_score", get_z(F.col("sum_trx"), F.col("mean_trx"), F.col("stddev_trx")))
df_grouped = df_grouped.withColumn("possible_fraud", possible_fraud(F.col("mean_trx"), F.col("z_score"), F.col("count_trx")))

df_trx = df_trx.join(df_grouped, on=["merchant_abn", "order_datetime"], how="left")



#NEED TO KNOW REQUIREMENTS
