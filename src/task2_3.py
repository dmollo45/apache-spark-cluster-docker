# Part 2: Spark Dataframe API
# Task 3
# Calculate the average number of bathrooms and bedrooms across all 
# the properties listed in this data set with a price of > 5000 
# and a review score being exactly equalt to 10
#  
#  Write the results into a CSV file out/out_2_3.txt

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
parkSQL = spark.sql("select avg(bathrooms) as avg_bathrooms, avg(bedrooms) as avg_bedrooms from ParquetTable where price > 5000")
# print query result to console
parkSQL.show(truncate=False)
# write csv file
parkSQL.write.options(header='True', delimiter=',').format("csv").mode('overwrite').csv("/opt/out/out_2_3.txt")


'''
run this app as: 

/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task2_3.py

'''