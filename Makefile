.PHONY: env build up down reset test test-spark streaming backfill batch clean-silver mapreduce gold validate analytics perf

DATE ?= $(shell date -d yesterday +%F 2>/dev/null || date -v-1d +%F)
PACKAGES = org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3
SUBMIT = docker exec -i spark-master /opt/spark/bin/spark-submit \
	--master spark://spark-master:7077 \
	--conf spark.jars.ivy=/opt/aq-ivy \
	--packages $(PACKAGES)

env:
	cp .env.example .env

build:
	docker compose build

up:
	docker compose up -d --build
	@echo "Kafka UI:      http://localhost:8080"
	@echo "Spark UI:      http://localhost:8082"
	@echo "MinIO console: http://localhost:9001"
	@echo "PostgreSQL:    localhost:5432 (db aq)"

down:
	docker compose down

# Xoá cả volume (MinIO, Postgres) để init lại DDL
reset:
	docker compose down -v

# Unit test thuần Python (không cần Spark/Java)
test:
	python -m pytest -q tests/test_aqi.py tests/test_alert_rules.py tests/test_poller.py tests/test_notifier.py tests/test_mapreduce.py

# Toàn bộ test, chạy trong container Spark (có Java + pyspark)
test-spark:
	docker exec -i -w /opt/aq spark-master python3 -m pytest -q tests --ignore=tests/test_dag.py

streaming:
	$(SUBMIT) /opt/aq/jobs/streaming/aq_streaming.py

backfill:
	$(SUBMIT) /opt/aq/jobs/batch/backfill_archive.py

# Batch hằng ngày M7 -> M10: clean -> (mapreduce || gold) -> validate -> analytics
batch: clean-silver mapreduce gold validate analytics

clean-silver:
	$(SUBMIT) /opt/aq/jobs/batch/clean_bronze_to_silver.py --date $(DATE)

mapreduce:
	docker exec -i -w /opt/aq spark-master python3 jobs/mapreduce/run_daily_stats.py --date $(DATE)

gold:
	$(SUBMIT) /opt/aq/jobs/batch/aggregate_silver_to_gold.py --date $(DATE)

validate:
	$(SUBMIT) /opt/aq/jobs/batch/validate_mr_vs_spark.py --date $(DATE)

analytics:
	$(SUBMIT) /opt/aq/jobs/batch/analytics.py

perf:
	$(SUBMIT) /opt/aq/jobs/batch/performance_test.py
