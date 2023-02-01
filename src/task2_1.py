# Part 2: Spark Dataframe API
# Task 1
# Download the parquet data file from given URL
# and make it available to spark 
#  

import pyspark # apache spark
from pyspark.sql import SparkSession # apache spark
# initialize spark object
spark=SparkSession.builder.appName("parquetFile").getOrCreate()
# read table from file
parDF1=spark.read.parquet("/opt/spark-data/sf-airbnb-clean.parquet")
# print the table read above
parDF1.printSchema()
# print to console
parDF1.show(truncate=False)

'''
run this app as: 

/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task2_1.py

'''