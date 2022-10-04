from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.functions import countDistinct, col, date_format
import numpy as np
import pyspark.sql.functions as func
from pyspark.sql.functions import sum, avg, count, lag, date_sub, split
from pyspark.sql.window import Window
import geopandas as gpd
from datetime import datetime

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.functions import countDistinct, col, date_format
import numpy as np
import pyspark.sql.functions as func
from pyspark.sql.functions import sum, avg, count, lag, date_sub, split
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType,
    LongType,
    DoubleType,
    StructField,
    StructType,
    FloatType
)

# Start Spark Session
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder.appName("MAST30034 Project 2 BNPL")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "8g")
    .getOrCreate()
)

# load data from ETL
df_trx_sa2 = spark.read.parquet('../data/curated/clean_df_trx_sa2.parquet')
df_trx_sa2_test = spark.read.parquet('../data/curated/clean_df_trx_sa2_test.parquet')

# remove fraud_probability which is above 5 from transaction
fraud_data_consumer = spark.read.csv("../data/tables/consumer_fraud_probability.csv", header=True)
fraud_data_merchant = spark.read.csv("../data/tables/merchant_fraud_probability.csv", header=True)

fraud_data_merchant = fraud_data_merchant.filter(F.col('fraud_probability') > 5)
fraud_data_consumer = fraud_data_consumer.filter(F.col('fraud_probability') > 5)

print("Dataframe size before:", df_trx_sa2.count())

df_trx_sa2.createOrReplaceTempView("TRX")
fraud_data_merchant.createOrReplaceTempView("MERFRAUD")
fraud_data_consumer.createOrReplaceTempView("CONFRAUD")

df_trx_sa2 = spark.sql("SELECT a.* FROM TRX a LEFT ANTI JOIN MERFRAUD b ON a.merchant_abn == b.merchant_abn AND a.order_datetime == b.order_datetime")

df_trx_sa2.createOrReplaceTempView("NOMERFRAUD")

df_trx_sa2 = spark.sql("SELECT a.* FROM NOMERFRAUD a LEFT ANTI JOIN CONFRAUD b ON a.user_id == b.user_id AND a.order_datetime == b.order_datetime")

print("Dataframe size after:", df_trx_sa2.count())

df_trx_sa2.write.parquet("../data/curated/fin_df_trx_sa2.parquet", mode="overwrite")
print("Successfully save fin_df_trx_sa2")
df_trx_sa2_test.write.parquet("../data/curated/fin_df_trx_sa2_test.parquet", mode="overwrite")
print("Successfully save fin_df_trx_sa2_test")