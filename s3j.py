from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()
from datetime import date

today = date.today().strftime("%Y-%m-%d")

print(today)

df = spark.read.load(f"s3://z43n/src/{today}")

aggdf = df.groupBy("username").agg(count("ip").alias("cnt"))

aggdf.show()

aggdf.write.format("parquet").mode("overwrite").save("s3://z43n/dest/ipCount")
