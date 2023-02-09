# Spark Cluster with Docker & docker-compose

## Pre requisites

* Docker installed

* Docker compose  installed

## Build the image


```sh
docker build -t cluster-apache-spark:3.0.2 .
```

## Run the docker-compose

The final step to create your test cluster will be to run the compose file:

```sh
docker-compose up -d
```
## start a worker shell

```sh
docker exec -it apache-spark-cluster-docker_spark-worker-a_1 bash
pip3 install requests

```

## from the shell run the apps as: 
```sh
/opt/spark/bin/spark-submit  \
   --master spark://spark-master:7077  \
   --jars /opt/spark-apps/postgresql-42.2.22.jar  \
   --driver-memory 1G  \
   --executor-memory 1G \
   /opt/spark-apps/task1_1.py

```
