"""Backfill một lần từ OpenAQ S3 archive vào Bronze (ARCHITECTURE 6.3).

Nguồn: s3://openaq-data-archive/records/csv.gz/locationid={id}/year={yyyy}/month={mm}/*.csv.gz
Cột:   location_id, sensors_id, location, datetime, lat, lon, parameter, units, value
       (datetime là giờ địa phương kèm offset, ví dụ 2024-07-01T10:00:00-04:00)

Ghi vào cùng thư mục Bronze, cùng dạng bản ghi Kafka của Query A (value = chuỗi JSON theo schema 5.1),
thêm parameter/units/location_name và source="openaq-archive".
ingest_date của backfill = ngày UTC của bản đo, để job làm sạch theo ngày đọc được đúng partition.

Chạy:
  make backfill                                        # dùng BACKFILL_FROM/BACKFILL_TO trong env
  spark-submit ... backfill_archive.py --from 2024-01-01 --to 2024-12-31 [--locations 2178,1234]
  spark-submit ... backfill_archive.py --input tests/fixtures/archive_sample.csv   # file cục bộ
"""
import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import BRONZE_PATH, DEFAULT_BBOX, TOPIC_MEASUREMENTS
from common.geo import parse_bbox
from common.spark_utils import create_spark_session, existing_paths, path_size, read_sensor_metadata

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("backfill")

ARCHIVE_BUCKET = "openaq-data-archive"
ARCHIVE_ROOT = f"s3a://{ARCHIVE_BUCKET}/records/csv.gz"
TS_FMT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

ARCHIVE_CONF = {
    # Bucket công khai trên AWS: truy cập ẩn danh, endpoint riêng cho bucket này (MinIO giữ nguyên)
    f"spark.hadoop.fs.s3a.bucket.{ARCHIVE_BUCKET}.endpoint": "s3.us-east-1.amazonaws.com",
    f"spark.hadoop.fs.s3a.bucket.{ARCHIVE_BUCKET}.endpoint.region": "us-east-1",
    f"spark.hadoop.fs.s3a.bucket.{ARCHIVE_BUCKET}.path.style.access": "false",
    f"spark.hadoop.fs.s3a.bucket.{ARCHIVE_BUCKET}.connection.ssl.enabled": "true",
    f"spark.hadoop.fs.s3a.bucket.{ARCHIVE_BUCKET}.aws.credentials.provider":
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
}


def month_range(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def archive_paths(location_ids, start: date, end: date):
    return [f"{ARCHIVE_ROOT}/locationid={loc}/year={y}/month={m:02d}/*.csv.gz"
            for loc in sorted(location_ids) for y, m in month_range(start, end)]


def transform_backfill(df: DataFrame, bbox_str: str, start: date = None, end: date = None,
                       ingested_at: str = None) -> DataFrame:
    """Hàm thuần: lọc bbox + khoảng ngày, map sang bản ghi Bronze."""
    min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox_str)
    ingested_at = ingested_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lat, lon = F.col("lat").cast("double"), F.col("lon").cast("double")
    event_ts = F.to_timestamp(F.col("datetime").cast("string"))  # offset trong chuỗi -> UTC
    df = (df.withColumn("event_ts", event_ts)
          .filter(lat.between(min_lat, max_lat) & lon.between(min_lon, max_lon))
          .filter(F.col("event_ts").isNotNull()))
    if start is not None:
        df = df.filter(F.to_date("event_ts") >= F.lit(start))
    if end is not None:
        df = df.filter(F.to_date("event_ts") <= F.lit(end))

    record = F.struct(
        F.col("sensors_id").cast("int").alias("sensor_id"),
        F.col("location_id").cast("int").alias("location_id"),
        F.col("value").cast("double").alias("value"),
        F.date_format("event_ts", TS_FMT).alias("datetime_utc"),
        F.col("datetime").cast("string").alias("datetime_local"),
        lat.alias("lat"),
        lon.alias("lon"),
        F.lit(ingested_at).alias("ingested_at"),
        F.lit("openaq-archive").alias("source"),
        F.col("parameter").cast("string").alias("parameter"),
        F.col("units").cast("string").alias("units"),
        F.col("location").cast("string").alias("location_name"),
    )
    return df.select(
        F.to_json(record).alias("value"),
        F.lit(TOPIC_MEASUREMENTS).alias("topic"),
        F.lit(-1).alias("partition"),       # -1: không đến từ Kafka
        F.lit(0).cast("long").alias("offset"),
        F.col("event_ts").alias("timestamp"),
        F.to_date("event_ts").alias("ingest_date"),
    )


def nyc_location_ids(spark, bootstrap: str):
    rows = read_sensor_metadata(spark, bootstrap).select("location_id").distinct().collect()
    return [r.location_id for r in rows if r.location_id is not None]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="date_from", default=os.getenv("BACKFILL_FROM"))
    parser.add_argument("--to", dest="date_to", default=os.getenv("BACKFILL_TO"))
    parser.add_argument("--locations", help="Danh sách location_id, mặc định lấy từ topic metadata")
    parser.add_argument("--input", help="Đọc file/glob CSV cụ thể thay cho archive (thử nghiệm)")
    args = parser.parse_args()

    bbox = os.getenv("NYC_BBOX", DEFAULT_BBOX)
    spark = create_spark_session("AQ_Backfill", conf=ARCHIVE_CONF)
    start = date.fromisoformat(args.date_from) if args.date_from else None
    end = date.fromisoformat(args.date_to) if args.date_to else None

    if args.input:
        paths, in_bytes = existing_paths(spark, [args.input])
    else:
        if not (start and end):
            parser.error("cần --from/--to (hoặc BACKFILL_FROM/BACKFILL_TO) khi đọc archive")
        if args.locations:
            locations = [int(x) for x in args.locations.split(",")]
        else:
            locations = nyc_location_ids(spark, os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
        logger.info("Backfill %s -> %s cho %d location NYC", start, end, len(locations))
        paths, in_bytes = existing_paths(spark, archive_paths(locations, start, end))
    if not paths:
        logger.warning("Không tìm thấy file archive nào, dừng.")
        return

    raw = spark.read.option("header", True).csv(paths)
    n_read = raw.count()
    bronze = transform_backfill(raw, bbox, start, end).cache()
    n_kept = bronze.count()
    logger.info("Đọc %d dòng (%.1f MB csv.gz), sau lọc bbox/ngày còn %d dòng",
                n_read, in_bytes / 1e6, n_kept)

    if n_kept == 0:
        logger.warning("Không có dòng nào sau lọc, không ghi.")
        return
    size_before = path_size(spark, BRONZE_PATH)
    (bronze.repartition("ingest_date").write
     .format("json").option("compression", "gzip")
     .partitionBy("ingest_date").mode("append").save(BRONZE_PATH))
    logger.info("Đã ghi %d dòng vào %s, dung lượng tăng %.1f MB",
                n_kept, BRONZE_PATH, (path_size(spark, BRONZE_PATH) - size_before) / 1e6)


if __name__ == "__main__":
    main()
