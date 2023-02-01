from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer 
from pyspark.ml.feature import IndexToString


spark = SparkSession \
    .builder \
    .appName("churn") \
    .getOrCreate()

df = spark.read.csv('/opt/spark-data/iris.csv', inferSchema = True)
cols = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"]
df = df.toDF(*cols)
df.show(5)

class_indexer = StringIndexer(inputCol="class", outputCol="classIndex")
#Fits a model to the input dataset with optional parameters.
class_indexer_model = class_indexer.fit(df)
df1 = class_indexer_model.transform(df)
df1.show()

assembler = VectorAssembler(inputCols = ["sepal_length", "sepal_width", "petal_length", "petal_width"], outputCol='features')
output = assembler.transform(df1)

finalised_data = output.select('features', 'classIndex')
finalised_data.show()

lr = LogisticRegression(featuresCol='features',labelCol='classIndex')
# Fit the model
lrModel = lr.fit(finalised_data)
# lrModel.setFeaturesCol("class")

df_val = spark.createDataFrame(
 [(5.1, 3.5, 1.4, 0.2),(6.2, 3.4, 5.4, 2.3)],
 ["sepal_length", "sepal_width", "petal_length", "petal_width"] )

output2 = assembler.transform(df_val)
finalised_data2 = output2.select('features')
finalised_data2.show()

predicted_df = lrModel.transform(finalised_data2)

predicted_df.show()

print(class_indexer_model.labels)
idx_to_string = IndexToString(inputCol="prediction", outputCol="predictedValue")
idx_to_string.setLabels(class_indexer_model.labels)
idx_to_string.getLabels()

df_with_prediction = class_indexer_model.transform(predicted_df)

df2 = idx_to_string.transform(df_with_prediction)
df2.show()

rdd2=df2.rdd.map(lambda x: x.predictedValue) 

f = open("/opt/out/out_3_2.txt", "w")
f.write("class\n")
f.write(rdd2.collect()[0])
f.write("\n")
f.write(rdd2.collect()[1])
f.write("\n")
f.close()

'''
/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task3_2.py
'''