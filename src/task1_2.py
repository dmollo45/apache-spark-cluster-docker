# Part 1: Spark RDD API
# Task 2
#  create a list and count of all (unique) products present in the transactions
#  report to textfiles: out/out_1_2a.txt and out/out_1_2b.txt and 

import os # to resolve path
import requests # make http request to download data
from pyspark import SparkContext # python's apache spark object
sc = SparkContext("local", "count app") # initialize apache spark

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
# read the data as string
rdd = sc.textFile("/opt/spark-data/groceries.csv");
# split the string along commas
# flatten the array structure
rdd2 = rdd.map(lambda line: line.split(",")).map(lambda x:x[0])
# open file for writting
f = open("/opt/out/out_1_2a.txt", "w")
# convert rdd to list and iterate over it
for i in rdd2.distinct().collect():
    print(i)
    f.write(i) # write item to file
    f.write("\n")# append new line
f.close() # close file

# open another file for writting
f = open("/opt/out/out_1_2b.txt", "w")
# report the number of unique items in data
f.write("Count: \n")
f.write(str(rdd2.distinct().count()))
f.write("\n")
f.close()

'''
run this app as: 

/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task1_2.py

'''