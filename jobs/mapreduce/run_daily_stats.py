"""Chạy DailyStatsMR cho một ngày: tải Bronze + sensor_lookup từ MinIO, chạy mrjob, ghi kết quả lên
s3a://aq-lake/validation/mr_daily_stats/date=YYYY-MM-DD/part-00000.

Mặc định dùng runner "local" của mrjob (chạy nhiều tiến trình map/reduce trên một máy, cùng mô hình
Hadoop Streaming). Có cụm Hadoop thì truyền --runner hadoop và input HDFS tương ứng.

Chạy: python jobs/mapreduce/run_daily_stats.py --date 2026-09-26
"""
import argparse
import json
import logging
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import boto3

from jobs.mapreduce.daily_stats import DailyStatsMR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("mapreduce")

BUCKET = "aq-lake"
BRONZE_PREFIX = "bronze/openaq/measurements/"
LOOKUP_KEY = "validation/sensor_lookup.json"
OUTPUT_PREFIX = "validation/mr_daily_stats/"


def s3_client():
    return boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
                        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
                        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadmin123"),
                        region_name="us-east-1")


def download_bronze(s3, day: date, workdir: Path):
    files = []
    for d in (day, day + timedelta(days=1)):  # ngày New York D nằm trong ingest_date D và D+1
        prefix = f"{BRONZE_PREFIX}ingest_date={d}/"
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json.gz") or obj["Key"].endswith(".json"):
                    local = workdir / "bronze" / obj["Key"].replace("/", "_")
                    local.parent.mkdir(parents=True, exist_ok=True)
                    s3.download_file(BUCKET, obj["Key"], str(local))
                    files.append(str(local))
    return files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--runner", default="local", choices=["inline", "local", "hadoop"])
    args = parser.parse_args()
    day = date.fromisoformat(args.date)
    s3 = s3_client()

    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        inputs = download_bronze(s3, day, workdir)
        if not inputs:
            logger.warning("Không có Bronze cho %s", day)
            return
        lookup = workdir / "sensor_lookup.json"
        s3.download_file(BUCKET, LOOKUP_KEY, str(lookup))
        logger.info("MapReduce %s trên %d file Bronze", day, len(inputs))

        job = DailyStatsMR(["-r", args.runner, "--lookup", str(lookup), "--date", str(day)] + inputs)
        lines = []
        with job.make_runner() as runner:
            runner.run()
            for key, value in job.parse_output(runner.cat_output()):
                lines.append(json.dumps(key) + "\t" + json.dumps(value))

    out_key = f"{OUTPUT_PREFIX}date={day}/part-00000"
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=("\n".join(lines) + "\n").encode("utf-8"))
    logger.info("Đã ghi %d khoá vào s3a://%s/%s", len(lines), BUCKET, out_key)


if __name__ == "__main__":
    main()
