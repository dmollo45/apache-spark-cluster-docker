import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("parquetFile").getOrCreate()
parDF1=spark.read.parquet("/opt/spark-data/sf-airbnb-clean.parquet")
parDF1.printSchema()
parDF1.show(truncate=False)

'''

/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task2_1.py

'''