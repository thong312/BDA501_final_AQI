"""Batch hang ngay (ARCHITECTURE muc 3, 6.4): clean -> (mapreduce || aggregate) -> validate -> analytics.

Cac task goi `docker exec` vao container spark-master, nen worker Airflow can docker CLI va
quyen truy cap /var/run/docker.sock. Khong co Airflow thi dung `make batch DATE=...` hoac run_batch.ps1.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PACKAGES = ("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3")
SUBMIT = ("docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 "
          f"--conf spark.jars.ivy=/opt/aq-ivy --packages {PACKAGES} /opt/aq/jobs/batch")

default_args = {
    "owner": "aq_admin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "aq_daily_batch_pipeline",
    default_args=default_args,
    description="Bronze -> Silver -> Gold -> analytics, kem MapReduce doi chieu",
    schedule_interval="0 2 * * *",   # 02:00 xu ly ngay hom truoc ({{ ds }})
    start_date=datetime(2026, 9, 26),
    catchup=False,
    tags=["aq", "spark", "batch"],
) as dag:
    clean = BashOperator(task_id="clean_bronze_to_silver",
                         bash_command=f"{SUBMIT}/clean_bronze_to_silver.py --date {{{{ ds }}}}")
    mapreduce = BashOperator(task_id="mapreduce_daily_stats",
                             bash_command="docker exec -w /opt/aq spark-master python3 "
                                          "jobs/mapreduce/run_daily_stats.py --date {{ ds }}")
    aggregate = BashOperator(task_id="aggregate_silver_to_gold",
                             bash_command=f"{SUBMIT}/aggregate_silver_to_gold.py --date {{{{ ds }}}}")
    validate = BashOperator(task_id="validate_mr_vs_spark",
                            bash_command=f"{SUBMIT}/validate_mr_vs_spark.py --date {{{{ ds }}}}")
    analytics = BashOperator(task_id="analytics_ml_sql",
                             bash_command=f"{SUBMIT}/analytics.py")

    clean >> [mapreduce, aggregate] >> validate >> analytics
