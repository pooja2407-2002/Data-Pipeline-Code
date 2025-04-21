from pyspark.sql import SparkSession
from pyspark import SparkConf

# Set up SparkSession for HDInsight
spark = SparkSession.builder \
    .appName("pyspark") \
    .master("yarn") \
    .getOrCreate()

from pyspark.sql.functions import *

print("====WORKING====")

import os
import urllib.request
import ssl
import subprocess

sc = spark.sparkContext
sc.setLogLevel("ERROR")

urldata = (
    urllib.request
    .urlopen(
        "https://randomuser.me/api/0.8/?results=1000",
        context=ssl._create_unverified_context()
    )
    .read()
    .decode('utf-8')
)


df = spark.read.json(spark.sparkContext.parallelize([urldata]))
df.show()

df.printSchema()



resultexp = df.withColumn("results", expr("explode(results)"))
resultexp.show()

finalflatten = resultexp.select(
    "nationality",
    "results.user.cell",
    "results.user.dob",
    "results.user.email",
    "results.user.gender",
    "results.user.location.city",
    "results.user.location.state",
    "results.user.location.street",
    "results.user.location.zip",
    "results.user.md5",
    "results.user.name.first",
    "results.user.name.last",
    "results.user.name.title",
    "results.user.password",
    "results.user.phone",
    "results.user.picture.large",
    "results.user.picture.medium",
    "results.user.picture.thumbnail",
    "results.user.registered",
    "results.user.salt",
    "results.user.sha1",
    "results.user.sha256",
    "results.user.username",
    "seed",
    "version"
)

seldf = finalflatten.select("username", "city", "state", "zip")


remdf = seldf.withColumn("username", regexp_replace(col("username"), "([0-9])", "") )


remdf.write.format("parquet").mode("overwrite").save("s3://z43n/dest/customerApi")
