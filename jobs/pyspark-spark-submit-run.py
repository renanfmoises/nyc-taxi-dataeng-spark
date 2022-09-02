import os
import argparse

import pyspark
# from pyspark.conf import SparkConf
# from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CREDS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SPARK_HOME = os.getenv("SPARK_HOME")

parser = argparse.ArgumentParser()

parser.add_argument("--input_green_path", type=str, required=True)
parser.add_argument("--input_yellow_path", type=str, required=True)
parser.add_argument("--output_path", type=str, required=True)

args = parser.parse_args()

input_green = args.input_green_path
input_yellow = args.input_yellow_path
output_path = args.output_path

# Create SparkConf
# conf = (
#     SparkConf()
#     .setMaster("spark://mpb-m1max.local:7077")
#     .setAppName("pyspark-local-standalone-master-gcs.sandbox")
#     .set("spark.jars", f"{SPARK_HOME}/jars/gcs-connector-hadoop3-latest.jar")
#     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
#     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", CREDS_PATH)
# )

# # Define SparkContext
# sc = SparkContext(conf = conf)
# hadoop_conf = sc._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
# hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
# hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", CREDS_PATH)
# hadoop_conf.set("fs.gs.auth.service.account.enable", "true")


# # Create SparkSession
# spark = (
#     SparkSession.builder
#     .config(conf = sc.getConf())
#     .getOrCreate()
#     )

# spark = (
#     SparkSession.builder.master("spark://mpb-m1max.local:7077")
#     .appName("pyspark-local-standalone-master-gcs.sandbox")
#     .getOrCreate()
# )

spark = (
    SparkSession.builder
    # .master("spark://mpb-m1max.local:7077")
    .appName("pyspark-local-standalone-master-gcs.sandbox")
    .getOrCreate()
)

# Read green taxi data
df_green = spark.read.parquet(input_green)
# df_green = spark.read.parquet("gs://data_lake_nyc-taxi-359621/part/green/*/*")


# Read yellow taxi data
df_yellow = spark.read.parquet(input_yellow)
# df_yellow = spark.read.parquet("gs://data_lake_nyc-taxi-359621/part/yellow/*/*")


# Rename columns to match in both datasets
# Green dataset
df_green = df_green.withColumnRenamed(
    "lpep_pickup_datetime", "pickup_datetime"
).withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

# Yellow dataset
df_yellow = df_yellow.withColumnRenamed(
    "tpep_pickup_datetime", "pickup_datetime"
).withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

# Select columns to match in both datasets
tripdata_columns = [col for col in df_green.columns if col in df_yellow.columns]

# Create a new column with the taxi type
df_green = df_green.select(*tripdata_columns).withColumn("service", F.lit("green"))
df_yellow = df_yellow.select(*tripdata_columns).withColumn("service", F.lit("yellow"))

# Combine both datasets
df_tripdata = df_green.unionAll(df_yellow)

# Create a TempView for sql queries
df_tripdata.createOrReplaceTempView("nyc_taxi_tripdata")

# Query
df_result = spark.sql(
    """
    SELECT
        -- Reveneue grouping
        DATE_TRUNC('month', pickup_datetime) AS revenue_month, 
        PULocationID AS revenue_zone,
        service,

        -- Revenue calculation
        ROUND(SUM(fare_amount), 2) AS revenue_monthly_fare,
        ROUND(SUM(extra), 2) AS revenue_monthly_extra,
        ROUND(SUM(mta_tax), 2) AS revenue_monthly_mta_tax,
        ROUND(SUM(tip_amount), 2) AS revenue_monthly_tip_amount,
        ROUND(SUM(tolls_amount), 2) AS revenue_monthly_tolls_amount,
        ROUND(SUM(improvement_surcharge), 2) AS revenue_monthly_improvement_surcharge,
        ROUND(SUM(total_amount), 2) AS revenue_monthly_total_amount,
        ROUND(SUM(congestion_surcharge), 2) AS revenue_monthly_congestion_surcharge,

        -- Additional calculations
        ROUND(AVG(passenger_count), 2) AS avg_montly_passenger_count,
        ROUND(AVG(trip_distance), 2) AS avg_montly_trip_distance
    FROM
        nyc_taxi_tripdata
    GROUP BY
        revenue_month, revenue_zone, service
"""
)

# df_result.coalesce(1).write.parquet(
#     "gs://data_lake_nyc-taxi-359621/reports/revenue/tripdata_all/",
#     mode="overwrite",
# )

df_result.coalesce(1).write.parquet(
    output_path,
    mode="overwrite",
)
