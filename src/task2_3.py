import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("parquetFile").getOrCreate()
parDF1=spark.read.parquet("/opt/spark-data/sf-airbnb-clean.parquet")
parDF1.printSchema()
parDF1.createOrReplaceTempView("parquetTable")
parkSQL = spark.sql("select avg(bathrooms) as avg_bathrooms, avg(bedrooms) as avg_bedrooms from ParquetTable where price > 5000")
parkSQL.show(truncate=False)
parkSQL.write.options(header='True', delimiter=',').format("csv").mode('overwrite').csv("/opt/out/out_2_3.txt")


'''
/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task2_3.py

'''