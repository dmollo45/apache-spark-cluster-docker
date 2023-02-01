import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("parquetFile").getOrCreate()
parDF1=spark.read.parquet("/opt/spark-data/sf-airbnb-clean.parquet")
parDF1.printSchema()
parDF1.createOrReplaceTempView("parquetTable")
maxRevSQL = spark.sql("select * from ParquetTable where review_scores_rating = (SELECT max(review_scores_rating) FROM ParquetTable)")
maxRevSQL.createOrReplaceTempView("maxRevTable")
maxRevSQL2 = spark.sql("select sum(accommodates) from maxRevTable where price = (select min(price) from maxRevTable)")
maxRevSQL2.show(truncate=False)
maxRevSQL2.write.options(header='True', delimiter=',').format("csv").mode('overwrite').csv("/opt/out/out_2_4.txt")


'''

/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task2_4.py

'''
