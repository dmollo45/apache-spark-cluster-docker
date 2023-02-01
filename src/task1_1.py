# Part 1: Spark RDD API
# Task 1 
# Download the data file from the above location and make it accessible to Spark.

import os # for path
import requests # for downloading content
from pyspark import SparkContext # get pyspark
sc = SparkContext("local", "count app") # initialize

# download file
def download(url: str, dest_folder: str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist

    filename = url.split('/')[-1].replace(" ", "_")  # be careful with file names
    file_path = os.path.join(dest_folder, filename)

    r = requests.get(url, stream=True) # make http request 
    if r.ok: # make http request  successful
        print("saving to", os.path.abspath(file_path))
        # open file and save
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:  # HTTP status code 4XX/5XX
        print("Download failed: status code {}\n{}".format(r.status_code, r.text))

# the data is contained in this url
url = "https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv"
# pull the data
download(url, dest_folder="/opt/spark-data")
# read as text file 
rdd = sc.textFile("/opt/spark-data/groceries.csv");
# split every line into its own data
rdd2 = rdd.map(lambda line: line.split("\n")) 
# print first 5 and compare to given result =  they are identical
print(rdd2.take(5))

'''
run this app as: 

/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task1_1.py

'''