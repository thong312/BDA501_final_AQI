"""Tao SparkSession dung chung (S3A toi MinIO, session timezone UTC)."""
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
               # Moi timestamp xu ly o UTC; gio New York tinh tuong minh bang from_utc_timestamp
               .config("spark.sql.session.timeZone", "UTC"))
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def explain_string(df, mode: str = "formatted") -> str:
    """Noi dung df.explain(mode) duoi dang chuoi de luu vao reports/plans/."""
    return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)


def _fs_path(spark, path):
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    return jpath.getFileSystem(spark._jsc.hadoopConfiguration()), jpath


def existing_paths(spark, paths):
    """Loc cac path/glob co du lieu. Tra (danh sach path, tong bytes)."""
    found, total = [], 0
    for p in paths:
        fs, jpath = _fs_path(spark, p)
        statuses = fs.globStatus(jpath)
        if statuses:
            found.append(p)
            for st in statuses:
                total += fs.getContentSummary(st.getPath()).getLength()
    return found, total


def path_size(spark, path) -> int:
    fs, jpath = _fs_path(spark, path)
    return fs.getContentSummary(jpath).getLength() if fs.exists(jpath) else 0


def write_text(spark, path: str, text: str):
    """Ghi mot file text duy nhat (khong phai thu muc part-*) len S3/HDFS."""
    fs, jpath = _fs_path(spark, path)
    out = fs.create(jpath, True)
    try:
        out.write(bytearray(text.encode("utf-8")))
    finally:
        out.close()


def read_sensor_metadata(spark, bootstrap: str):
    """Topic compacted aq.openaq.sensors.v1 -> ban moi nhat moi sensor (theo offset), bo tombstone."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from common.config import TOPIC_SENSORS
    from common.schemas import SENSOR_STRUCT

    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", TOPIC_SENSORS)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())
    return (raw.withColumn("rn", F.row_number().over(
                Window.partitionBy("key").orderBy(F.col("partition").desc(), F.col("offset").desc())))
            .filter("rn = 1")
            .filter(F.col("value").isNotNull())
            .select(F.from_json(F.col("value").cast("string"), SENSOR_STRUCT).alias("m"))
            .select("m.*")
            .filter(F.col("sensor_id").isNotNull()))
