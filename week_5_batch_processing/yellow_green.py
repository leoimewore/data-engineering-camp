#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
from pyspark.sql import types
from pyspark.sql import functions as F
import argparse



spark = SparkSession.builder \
    .master("spark://de-zoomcamp.us-central1-c.c.shareprompt-412612.internal:7077") \
    .appName('test') \
    .getOrCreate()

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()


input_green = args.input_green
input_yellow = args.input_yellow
output = args.output



print(spark.sparkContext.uiWebUrl)



df_green = spark.read.parquet(input_green)




df_green.show()




df_green = df_green.withColumn("VendorID", col("VendorID").cast("int"))



df_green.printSchema()
df_green.show()



yellow_schema = yellow_schema = types.StructType([
    types.StructField("VendorID", types.StringType(), True),
    types.StructField("tpep_pickup_datetime", types.TimestampType(), True),
    types.StructField("tpep_dropoff_datetime", types.TimestampType(), True),
    types.StructField("passenger_count", types.IntegerType(), True),
    types.StructField("trip_distance", types.DoubleType(), True),
    types.StructField("RatecodeID", types.IntegerType(), True),
    types.StructField("store_and_fwd_flag", types.StringType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("payment_type", types.IntegerType(), True),
    types.StructField("fare_amount", types.DoubleType(), True),
    types.StructField("extra", types.DoubleType(), True),
    types.StructField("mta_tax", types.DoubleType(), True),
    types.StructField("tip_amount", types.DoubleType(), True),
    types.StructField("tolls_amount", types.DoubleType(), True),
    types.StructField("improvement_surcharge", types.DoubleType(), True),
    types.StructField("total_amount", types.DoubleType(), True),
    types.StructField("congestion_surcharge", types.DoubleType(), True)
])




df_yellow = spark.read.parquet(input_yellow)




df_yellow.show()




df_yellow = df_yellow.withColumn("VendorID", col("VendorID").cast("int"))




df_yellow.columns

df_green=df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


df_yellow=df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


columns = []
yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        columns.append(col)
        




df_green.select(columns).show()

df_green_sel =df_green.select(columns) \
    .withColumn('service_type', F.lit("green"))


df_yellow_sel =df_yellow.select(columns) \
    .withColumn('service_type', F.lit("yellow"))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)


df_trips_data.groupBy('service_type').count().show()


df_trips_data.registerTempTable('trips_data')





df_result = spark.sql("""
SELECT
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc("month", "pickup_datetime") AS revenue_month, 

    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance

    FROM
    trips_data
    GROUP BY 1,2,3
""")


df_result.write.parquet(output)

print(spark.sparkContext.uiWebUrl)






