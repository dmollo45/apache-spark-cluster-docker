# Part 3: Spark Dataframe API
# Task 2
# Implement a Spark version of the above Python code and demonstrate 
# that you can correctly predict on the training data, similar
# to what was done in the preceding section.
#  

# apache spark imports
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer 
from pyspark.ml.feature import IndexToString

# initialize spark object
spark = SparkSession \
    .builder \
    .appName("churn") \
    .getOrCreate()
# read table from file
df = spark.read.csv('/opt/spark-data/iris.csv', inferSchema = True)
# this is the data schema
cols = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"]
df = df.toDF(*cols)
# print table to console
df.show(5)
# create indexter to convert column class strings to indices
class_indexer = StringIndexer(inputCol="class", outputCol="classIndex")
# create an indexter
class_indexer_model = class_indexer.fit(df)
# this will add extra column called classIndex
df1 = class_indexer_model.transform(df)
# print transformed table to console
df1.show()

# create assebler to combine all columns to simgle datum
assembler = VectorAssembler(inputCols = ["sepal_length", "sepal_width", "petal_length", "petal_width"], outputCol='features')
# perform the columns transformation
output = assembler.transform(df1)
# specify dependent and independent variable for model
finalised_data = output.select('features', 'classIndex')
# print table to console
finalised_data.show()

# specify regression model with input and output column
lr = LogisticRegression(featuresCol='features',labelCol='classIndex')
# Fit the model
lrModel = lr.fit(finalised_data)

# this is the test model
df_val = spark.createDataFrame(
 [(5.1, 3.5, 1.4, 0.2),(6.2, 3.4, 5.4, 2.3)],
 ["sepal_length", "sepal_width", "petal_length", "petal_width"] )
# we have to transform it first to be same shape as training data
output2 = assembler.transform(df_val)
finalised_data2 = output2.select('features')
# output to console
finalised_data2.show()
# perform prediction
predicted_df = lrModel.transform(finalised_data2)
# show predicted to console
predicted_df.show()
#print(class_indexer_model.labels)

# initialize index to string converter with column names
idx_to_string = IndexToString(inputCol="prediction", outputCol="predictedValue")
# give index to string converter labels
idx_to_string.setLabels(class_indexer_model.labels)
# print the labels to console
idx_to_string.getLabels()


df_with_prediction = class_indexer_model.transform(predicted_df)
# transform from indexed values to original strings
df2 = idx_to_string.transform(df_with_prediction)
# print to console
df2.show()

# we are only intrested in one column
rdd2=df2.rdd.map(lambda x: x.predictedValue) 

# open file for writing
f = open("/opt/out/out_3_2.txt", "w")
f.write("class\n")
# report first value
f.write(rdd2.collect()[0])
f.write("\n")
# report second value
f.write(rdd2.collect()[1])
f.write("\n")
f.close()

'''
run this app as: 

/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task3_2.py
'''