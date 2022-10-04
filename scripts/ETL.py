from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.functions import countDistinct, col, date_format
import numpy as np
import pyspark.sql.functions as func
from pyspark.sql.functions import sum, avg, count, lag, date_sub, split
from pyspark.sql.window import Window

# Start Spark Session
spark = (
    SparkSession.builder.appName("MAST30034 Project 2 BNPL")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

# load BNPL dataset
consumer = spark.read.csv("../data/tables/tbl_consumer.csv", header=True, sep="|")
details = spark.read.parquet("../data/tables/consumer_user_details.parquet")
merchants = spark.read.parquet("../data/tables/tbl_merchants.parquet")

# load all transactions datasets
paths=['../data/tables/transactions_20210228_20210827_snapshot',
       '../data/tables/transactions_20210828_20220227_snapshot']

first = 1
for path in paths:
    if first:
        transactions = spark.read.parquet(path)
        first = 0
    else:
        append_transactions = spark.read.parquet(path)
        transactions = transactions.union(append_transactions)

test_path = '../data/tables/transactions_20220228_20220828_snapshot'
test_trx = spark.read.parquet(test_path)

age = gpd.read_file("../data/abs/sa2_age.gml")
income = gpd.read_file("../data/abs/sa2_income.gml")

# load poa_to_sa2 dataset
poa_to_sa2 = spark.read.csv("../data/curated/poa_w_sa2.csv", header=True)

# rename columns
merchants = merchants.withColumnRenamed('name', 'merchant_name')
consumer = consumer.withColumnRenamed('name', 'consumer_name')

# Join consumers with their respective details
consumer_detail = consumer.join(details, on="consumer_id")

# Join consumers with their respective transactions
consumer_trx = consumer_detail.join(transactions, on="user_id")
consumer_trx_test = consumer_detail.join(test_trx, on="user_id")

# Join transactions with the respective merchants
df_trx = consumer_trx.join(merchants, on="merchant_abn")
df_trx_test = consumer_trx_test.join(merchants, on="merchant_abn")

# translate postcodes in transaction to sa2 codes
sa2_cols = ['poa_name_2016', 'sa2_maincode_2016', 'sa2_name_2016', 'geometry']
df_trx_sa2 = (df_trx \
                .join(poa_to_sa2[sa2_cols], 
                     on=[df_trx['postcode'] == poa_to_sa2['poa_name_2016']],
                     how='inner')
                .drop('poa_name_2016')
             )

df_trx_sa2_test = (df_trx_test \
                    .join(poa_to_sa2[sa2_cols], 
                         on=[df_trx_test['postcode'] == 
                             poa_to_sa2['poa_name_2016']],
                         how='inner')
                    .drop('poa_name_2016')
                  )


@F.udf(StringType())
def normalize_tags(col):
    return col.replace("(", "[").replace(")", "]")[1:-1]

df_trx_sa2 = df_trx_sa2.withColumn("tags", normalize_tags(F.col("tags")))
df_trx_sa2_test = df_trx_sa2_test.withColumn("tags", 
                                             normalize_tags(F.col("tags")))


def clean_tags(trx_sa2):
    '''
    Split elements in tags into:
    - categories
    - revenue_level
    - take_rate
    '''
    trx_sa2 = (trx_sa2
                  .withColumn("categories", 
                              F.regexp_extract("tags", "(?<=\[)(.*?)(?=\])", 1))
                  .withColumn("revenue_level", 
                              F.regexp_extract("tags", "(?<=,\s\[)([a-e]+?)(?=\],)", 1))
                  .withColumn("take_rate", 
                              F.regexp_extract("tags", "(?<=\[take rate: )(.*?)(?=\])", 1))
                  .withColumn("take_rate", 
                              F.col("take_rate").astype(FloatType()))
              )
    return trx_sa2

df_trx_sa2 = clean_tags(df_trx_sa2)
df_trx_sa2_test = clean_tags(df_trx_sa2_test)

# business rule applied: only transaction above $35 is applicable
df_trx_sa2 = df_trx_sa2[df_trx_sa2["dollar_value"] >= 35]
df_trx_sa2_test = df_trx_sa2_test[df_trx_sa2_test["dollar_value"] >= 35]


@F.udf(StringType())
def clean_string(col):
    '''
    Convert every character to lower case
    '''
    col = col.lower()
    return " ".join(col.split())

df_trx_sa2 = df_trx_sa2.withColumn("categories", clean_string(F.col("categories")))
df_trx_sa2_test = df_trx_sa2_test.withColumn("categories", clean_string(F.col("categories")))


@F.udf(FloatType())
def get_revenue(take_rate, dollar_value):
    '''
    Calculate revenue for the BNPL platform per transaction
    
    revenue = take_rate * dollar_value + 0.3
    ($0.3 flat transaction fee - Klarna/Afterpay) 
    '''
    return (take_rate / 100) * dollar_value + 0.3

df_trx_sa2 = df_trx_sa2.withColumn("revenue", get_revenue(F.col("take_rate"), F.col("dollar_value")))
df_trx_sa2_test = df_trx_sa2_test.withColumn("revenue", get_revenue(F.col("take_rate"), F.col("dollar_value")))

df_trx_sa2 = df_trx_sa2.drop('tags')
df_trx_sa2_test = df_trx_sa2_test.drop('tags')

# calculate population of age above 20


def get_age_pop(age_start, age_end):
    '''
    Create new columns that sum the age population that is in the range of
    age_start and age_end from the age dataframe.
    '''
    
    left_age = age_start
    right_age = left_age + 4
    
    age[f'males_age_{age_start}_{age_end}'] = age[f'males_age_{left_age}_{right_age}']
    age[f'females_age_{age_start}_{age_end}'] = age[f'females_age_{left_age}_{right_age}']
    
    while right_age < age_end + 1:
        left_age += 5
        right_age += 5
        
        age[f'males_age_{age_start}_{age_end}'] += age[f'males_age_{left_age}_{right_age}']
        age[f'females_age_{age_start}_{age_end}'] += age[f'females_age_{left_age}_{right_age}']
    
    return age
        
    
age = get_age_pop(20, 44)
age = get_age_pop(45, 60)

age = age[['sa2_main16', 'males_age_20_44', 'females_age_20_44', 
           'males_age_45_60', 'females_age_45_60']]

# change age gpd dataframe to sdf
age_sdf = spark.createDataFrame(age)

# change income gpd dataframe to sdf
cols = ['sa2_code', 'median_age_of_earners_years', 'median_aud', 
        'gini_coefficient_coef']
income_sdf = spark.createDataFrame(income[cols])

# create new column 'yyyy-mmm' from order datetime
df_trx_sa2 = (df_trx_sa2.withColumn("order_year_month", 
                                date_format(col("order_datetime"), 'yyyy-MM')))
df_trx_sa2_test = (df_trx_sa2_test.withColumn("order_year_month", 
                                date_format(col("order_datetime"), 'yyyy-MM')))

# remove fraud_probability above 5 from transaction
fraud_data_merchant = fraud_data_merchant.filter(F.col('fraud_probability') > 5)
fraud_data_consumer = fraud_data_consumer.filter(F.col('fraud_probability') > 5)

df_trx_sa2.createOrReplaceTempView("TRX")
fraud_data_merchant.createOrReplaceTempView("MERFRAUD")
fraud_data_consumer.createOrReplaceTempView("CONFRAUD")

df_trx_sa2 = spark.sql("SELECT a.* FROM TRX a LEFT ANTI JOIN MERFRAUD b ON a.merchant_abn == b.merchant_abn AND a.order_datetime == b.order_datetime")

df_trx_sa2.createOrReplaceTempView("NOMERFRAUD")

df_trx_sa2 = spark.sql("SELECT a.* FROM NOMERFRAUD a LEFT ANTI JOIN CONFRAUD b ON a.user_id == b.user_id AND a.order_datetime == b.order_datetime")


# save final sdf to curated data folder
df_trx_sa2.write.parquet("../data/curated/df_train_transaction.parquet", mode="overwrite")
df_trx_sa2_test.write.parquet("../data/curated/df_test_transaction.parquet", mode="overwrite")

