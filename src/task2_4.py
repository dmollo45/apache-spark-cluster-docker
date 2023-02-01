# Part 2: Spark Dataframe API
# Task 4
# How many people can be accomodated by the property with the lowest price and highest rating?
#  

import pyspark # apache spark
from pyspark.sql import SparkSession # apache spark
# initialize spark object
spark=SparkSession.builder.appName("parquetFile").getOrCreate()
# read table from file
parDF1=spark.read.parquet("/opt/spark-data/sf-airbnb-clean.parquet")
# print table columns to console
parDF1.printSchema()
# base for sql queries
parDF1.createOrReplaceTempView("parquetTable")
# run sql statement to produce new table
maxRevSQL = spark.sql("select * from ParquetTable where review_scores_rating = (SELECT max(review_scores_rating) FROM ParquetTable)")
# another base table for sql queries
maxRevSQL.createOrReplaceTempView("maxRevTable")
# run sql statement to produce new table
maxRevSQL2 = spark.sql("select sum(accommodates) from maxRevTable where price = (select min(price) from maxRevTable)")
# print query result to console
maxRevSQL2.show(truncate=False)
# write to csv file
maxRevSQL2.write.options(header='True', delimiter=',').format("csv").mode('overwrite').csv("/opt/out/out_2_4.txt")


'''
run this app as: 

/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task2_4.py

'''
