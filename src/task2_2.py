# Part 2: Spark Dataframe API
# Task 2
# Create CSV output file under out/out_2_2.txt that lists the minimum price, 
# maximum price, and total row count from this data set
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
parkSQL = spark.sql("select min(price) as min_price, max(price) as max_price, count(price) as row_count from ParquetTable ")
# print query result to console
parkSQL.show(truncate=False)
# write csv file
parkSQL.write.options(header='True', delimiter=',').format("csv").mode('overwrite').csv("/opt/out/out_2_2.txt")


'''
run this app as:

/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task2_2.py

'''

