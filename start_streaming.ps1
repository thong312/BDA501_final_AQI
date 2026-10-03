# Chạy job Spark Streaming (query A + B) trên cụm Docker. Tương đương `make streaming`.
$ErrorActionPreference = "Stop"
$Packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3"
docker exec -i spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 `
    --conf spark.jars.ivy=/opt/aq-ivy --packages $Packages /opt/aq/jobs/streaming/aq_streaming.py
