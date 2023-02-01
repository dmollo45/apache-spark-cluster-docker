import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("parquetFile").getOrCreate()
parDF1=spark.read.parquet("/opt/spark-data/sf-airbnb-clean.parquet")
parDF1.printSchema()
parDF1.createOrReplaceTempView("parquetTable")
parkSQL = spark.sql("select max(price) as min_price, min(price) as max_price, count(price) as row_count from ParquetTable ")
parkSQL.show(truncate=False)
parkSQL.write.options(header='True', delimiter=',').format("csv").mode('overwrite').csv("/opt/out/out_2_2.txt")


'''

/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task2_2.py

'''

