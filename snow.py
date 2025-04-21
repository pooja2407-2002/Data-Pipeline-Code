from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

import os
secretjson = os.popen("aws secretsmanager get-secret-value --secret-id snowp").read()
secretrdd = sc.parallelize([secretjson])
secretdf  = spark.read.json(secretrdd)
secretstring = secretdf.select("SecretString")
finalpassword = secretstring.withColumn("password",expr("replace(replace(split(SecretString,':')[1],'\"',''),'}','')")).select("password")
password = finalpassword.rdd.collect()[0]["password"]





df = (
    spark
    .read
    .format("snowflake")
    .option("sfURL","https://aocjkpl-cr29100.snowflakecomputing.com")
    .option("sfAccount","aocjkpl")
    .option("sfUser","root")
    .option("sfPassword",password  )
    .option("sfDatabase","zeyodb")
    .option("sfSchema","zeyoschema")
    .option("sfRole","ACCOUNTADMIN")
    .option("sfWarehouse","COMPUTE_WH")
    .option("dbtable","srctab")
    .load()
)


df.show()

aggdf = df.groupBy("username").agg(collect_list("score").alias("cust_score"))

aggdf.write.format("parquet").mode("overwrite").save("s3://z43n/dest/custScores")
