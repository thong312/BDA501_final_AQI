$ErrorActionPreference = "Stop"

Write-Host "`n[1/3] Checking and installing Python libraries (psycopg2, dateutil) for Spark cluster..." -ForegroundColor Yellow
Write-Host "Installing on spark-master..." -ForegroundColor Cyan
docker exec -u root spark-master pip install python-dateutil psycopg2-binary
Write-Host "Installing on spark-worker-1..." -ForegroundColor Cyan
docker exec -u root spark-worker-1 pip install python-dateutil psycopg2-binary
Write-Host "Installing on spark-worker-2..." -ForegroundColor Cyan
docker exec -u root spark-worker-2 pip install python-dateutil psycopg2-binary

Write-Host "`n[2/3] Starting Real-time Streaming from Kafka to S3 and Postgres..." -ForegroundColor Yellow
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2 /opt/spark/jobs/streaming/aq_streaming.py

Write-Host "`n[3/3] Streaming is running..." -ForegroundColor Green
