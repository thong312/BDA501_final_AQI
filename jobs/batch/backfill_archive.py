import os
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, struct, to_json, expr

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_backfill(df, bbox_str):
    """
    Hàm thuần xử lý DataFrame backfill:
    - Lọc dữ liệu trong Bounding Box của NYC.
    - Ép kiểu và đổi tên các cột sang chuẩn schema 5.1.
    - Thêm thông tin tĩnh: parameter, units, location_name, source="openaq-archive".
    - Đóng gói dữ liệu về dạng chuỗi JSON `value` giống cấu trúc của topic Kafka ở M4 để ghi vào chung thư mục Bronze.
    """
    parts = bbox_str.split(',')
    min_lon, min_lat, max_lon, max_lat = float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])
    
    # 1. Lọc bbox
    df_filtered = df.filter(
        (col("lat") >= min_lat) & (col("lat") <= max_lat) &
        (col("lon") >= min_lon) & (col("lon") <= max_lon)
    )
    
    # 2. Chuẩn hoá schema
    df_mapped = df_filtered.select(
        col("sensors_id").cast("int").alias("sensor_id"),
        col("location_id").cast("int").alias("location_id"),
        col("value").cast("double").alias("value"),
        col("datetime").cast("string").alias("datetime_utc"),
        col("datetime").cast("string").alias("datetime_local"),
        col("lat").cast("double").alias("lat"),
        col("lon").cast("double").alias("lon"),
        col("datetime").cast("string").alias("ingested_at"),
        lit("openaq-archive").alias("source"),
        col("parameter").cast("string").alias("parameter"),
        col("units").cast("string").alias("units"),
        col("location").cast("string").alias("location_name")
    )
    
    # 3. Tạo ingest_date để partition thư mục
    df_mapped = df_mapped.withColumn("ingest_date", to_date(col("datetime_utc")))
    
    # 4. Gói dữ liệu thô vào JSON bên trong cột `value` giống cấu trúc M4
    df_bronze = df_mapped.select(
        to_json(struct(
            col("sensor_id"), col("location_id"), col("value").alias("val_tmp"), # rename tạm để tránh lặp tên `value` ngoài
            col("datetime_utc"), col("datetime_local"),
            col("lat"), col("lon"), col("ingested_at"), col("source"),
            col("parameter"), col("units"), col("location_name")
        )).alias("value"),
        lit("aq.openaq.measurements.v1").alias("topic"),
        lit(0).alias("partition"),
        lit(0).cast("long").alias("offset"),
        col("datetime_utc").cast("timestamp").alias("timestamp"),
        col("ingest_date")
    )
    
    # Thay thế chuỗi JSON val_tmp về value cho đúng cấu trúc
    df_bronze = df_bronze.withColumn("value", expr("regexp_replace(value, '\"val_tmp\"', '\"value\"')"))
    
    return df_bronze

def create_spark_session():
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "test")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "test")
    
    return (SparkSession.builder
            .appName("AQ_Backfill")
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True, help="Đường dẫn file CSV input từ S3/local")
    args = parser.parse_args()

    BBOX = os.getenv("NYC_BBOX", "-74.26,40.49,-73.70,40.92")
    OUTPUT_PATH = "s3a://aq-lake/bronze/openaq/measurements/"
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Bắt đầu đọc dữ liệu từ: {args.input}")
    
    df_raw = spark.read.csv(args.input, header=True, inferSchema=True)
    count_raw = df_raw.count()
    logger.info(f"Tổng số dòng đọc ban đầu (trước lọc): {count_raw}")
    
    df_transformed = transform_backfill(df_raw, BBOX)
    df_transformed.cache()
    
    count_filtered = df_transformed.count()
    logger.info(f"Tổng số dòng sau khi lọc Bounding Box NYC: {count_filtered}")
    
    if count_filtered > 0:
        logger.info(f"Đang ghi xuống Bronze Data Lake tại {OUTPUT_PATH} ...")
        df_transformed.write \
            .format("json") \
            .option("compression", "gzip") \
            .partitionBy("ingest_date") \
            .mode("append") \
            .save(OUTPUT_PATH)
        logger.info("Hoàn thành quá trình Backfill!")
    else:
        logger.warning("Không có dữ liệu nào khớp với Bounding Box, không ghi.")
