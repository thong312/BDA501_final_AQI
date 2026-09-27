from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'aq_admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'aq_daily_batch_pipeline',
    default_args=default_args,
    description='Pipeline chạy quy trình Batch Data Lake từ M7 đến M10',
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 9, 26),
    catchup=False,
    tags=['aq', 'spark', 'batch'],
) as dag:

    clean_bronze_to_silver = BashOperator(
        task_id='clean_bronze_to_silver',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4 /opt/spark/jobs/batch/clean_bronze_to_silver.py --date {{ ds }}'
    )

    aggregate_silver_to_gold = BashOperator(
        task_id='aggregate_silver_to_gold',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4 /opt/spark/jobs/batch/aggregate_silver_to_gold.py --date {{ ds }}'
    )

    validate_mr_vs_spark = BashOperator(
        task_id='validate_mr_vs_spark',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4 /opt/spark/jobs/batch/validate_mr_vs_spark.py --date {{ ds }}'
    )

    analytics_ml_sql = BashOperator(
        task_id='analytics_ml_sql',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2 /opt/spark/jobs/batch/analytics.py'
    )

    clean_bronze_to_silver >> aggregate_silver_to_gold >> validate_mr_vs_spark >> analytics_ml_sql
