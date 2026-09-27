"""Tạo SparkSession dùng chung (S3A tới MinIO, session timezone UTC)."""
import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str, conf: dict = None) -> SparkSession:
    builder = (SparkSession.builder.appName(app_name)
               .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
               .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "minioadmin"))
               .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "minioadmin123"))
               .config("spark.hadoop.fs.s3a.path.style.access", "true")
               .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
               .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                       str(os.getenv("S3_ENDPOINT", "http://").startswith("https")).lower())
               # Mọi timestamp xử lý ở UTC; giờ New York tính tường minh bằng from_utc_timestamp
               .config("spark.sql.session.timeZone", "UTC"))
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
