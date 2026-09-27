import os
import argparse
import logging
import json
import yaml
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date, hour, year, month, expr, broadcast, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window

import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from common.geo import get_borough

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

measurements_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("value", DoubleType(), True),
    StructField("datetime_utc", StringType(), True),
    StructField("datetime_local", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("source", StringType(), True),
    # Có thể có sẵn từ backfill
    StructField("parameter", StringType(), True),
    StructField("units", StringType(), True),
    StructField("location_name", StringType(), True)
])

# UDF để tính toán Borough
borough_udf = udf(lambda lat, lon: get_borough(lat, lon), StringType())

def build_quality_flag_expr(rules_dict):
    """ Tạo câu lệnh SQL CASE WHEN động dựa trên bộ luật quality_rules """
    # Mặc định
    sql_expr = "CASE WHEN parameter IS NULL THEN 'NO_METADATA' "
    sql_expr += "WHEN value IS NULL OR value < 0 THEN 'NEGATIVE' "
    
    # Stale: ingested_at - datetime_utc > 3 hours
    sql_expr += "WHEN (unix_timestamp(to_timestamp(ingested_at)) - unix_timestamp(to_timestamp(datetime_utc))) > 10800 THEN 'STALE' "
    
    # Out of range cho từng chất
    for param, limits in rules_dict.items():
        min_v = limits.get('min', -999)
        max_v = limits.get('max', 99999)
        sql_expr += f"WHEN parameter = '{param}' AND (value < {min_v} OR value > {max_v}) THEN 'OUT_OF_RANGE' "
        
    sql_expr += "ELSE 'OK' END"
    return sql_expr

def transform_silver(df_bronze, df_meta, rules_dict, bbox_str):
    """
    Hàm thuần xử lý Data:
    - Parse JSON.
    - Join Metadata.
    - Lọc BBOX.
    - Gắn Quality Flag.
    - Dedupe.
    - Thêm date/hour/borough/year/month.
    """
    # 1. Parse JSON
    df_parsed = df_bronze.select(from_json(col("value"), measurements_schema).alias("data")).select("data.*")
    
    # 2. Join Meta
    df_joined = df_parsed.join(broadcast(df_meta), "sensor_id", "left")
    df_enriched = df_joined.withColumn("parameter", expr("coalesce(parameter, meta_parameter)")) \
                           .withColumn("units", expr("coalesce(units, meta_units)")) \
                           .withColumn("location_name", expr("coalesce(location_name, meta_location_name)"))
                           
    # 3. Lọc BBOX
    parts = bbox_str.split(',')
    min_lon, min_lat, max_lon, max_lat = float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])
    df_bbox = df_enriched.filter(
        (col("lat") >= min_lat) & (col("lat") <= max_lat) &
        (col("lon") >= min_lon) & (col("lon") <= max_lon)
    )
    
    # 4. Gắn Quality Flag
    q_expr = build_quality_flag_expr(rules_dict)
    df_flagged = df_bbox.withColumn("quality_flag", expr(q_expr))
    
    # Lọc chỉ lấy OK
    df_ok = df_flagged.filter(col("quality_flag") == "OK")
    
    # 5. Dedupe
    windowSpec = Window.partitionBy("sensor_id", "datetime_utc").orderBy(col("ingested_at").desc())
    df_deduped = df_ok.withColumn("rn", expr("row_number() over (partition by sensor_id, datetime_utc order by ingested_at desc)")) \
                      .filter(col("rn") == 1).drop("rn")
                      
    # 6. Thêm các cột phục vụ partition và aggregate
    df_final = df_deduped.withColumn("ts_local", to_timestamp(col("datetime_local"))) \
                         .withColumn("date_local", to_date(col("ts_local"))) \
                         .withColumn("hour_local", hour(col("ts_local"))) \
                         .withColumn("year", year(col("date_local"))) \
                         .withColumn("month", month(col("date_local"))) \
                         .withColumn("borough", borough_udf(col("lat"), col("lon")))
                         
    return df_final, df_parsed, df_ok

def create_spark_session():
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "test")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "test")
    
    return (SparkSession.builder
            .appName("AQ_Clean_Bronze_to_Silver")
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="Ngày cần xử lý (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    BBOX = os.getenv("NYC_BBOX", "-74.26,40.49,-73.70,40.92")
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    
    config_path = Path(__file__).parent.parent.parent / "config" / "quality_rules.yaml"
    try:
        with open(config_path, "r") as f:
            RULES = yaml.safe_load(f)
    except:
        RULES = {}

    # Đọc Metadata từ Kafka (Topic lưu vĩnh viễn)
    try:
        logger.info("Đọc metadata từ Kafka...")
        df_kafka = (spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", "aq.openaq.sensors.v1")
            .option("startingOffsets", "earliest")
            .load())
        # Parse metadata
        meta_schema = StructType([
            StructField("sensor_id", IntegerType(), True),
            StructField("parameter", StringType(), True),
            StructField("units", StringType(), True),
            StructField("location_name", StringType(), True)
        ])
        df_meta_raw = df_kafka.select(from_json(col("value").cast("string"), meta_schema).alias("m")).select("m.*")
        
        # Dedupe metadata (lấy bản cập nhật mới nhất)
        meta_window = Window.partitionBy("sensor_id").orderBy(col("sensor_id")) # Giản lược lấy 1 dòng
        df_meta = df_meta_raw.withColumn("rn", expr("row_number() over (partition by sensor_id order by sensor_id)"))\
                             .filter(col("rn") == 1)\
                             .select(col("sensor_id"), 
                                     col("parameter").alias("meta_parameter"),
                                     col("units").alias("meta_units"),
                                     col("location_name").alias("meta_location_name"))
    except Exception as e:
        logger.warning(f"Không thể đọc metadata từ Kafka, sẽ bỏ qua: {e}")
        df_meta = spark.createDataFrame([], StructType([StructField("sensor_id", IntegerType())]))
        
    input_path = f"s3a://aq-lake/bronze/openaq/measurements/ingest_date={args.date}/"
    output_path = "s3a://aq-lake/silver/measurements/"
    
    logger.info(f"Đọc dữ liệu Bronze tại: {input_path}")
    try:
        df_bronze = spark.read.json(input_path)
    except Exception as e:
        logger.error(f"Lỗi đọc thư mục Bronze (có thể không có dữ liệu cho ngày này): {e}")
        sys.exit(0)
        
    df_final, df_parsed, df_ok = transform_silver(df_bronze, df_meta, RULES, BBOX)
    
    df_final.cache()
    
    count_raw = df_parsed.count()
    count_ok = df_ok.count()
    count_final = df_final.count()
    
    logger.info(f"--- BÁO CÁO CHẤT LƯỢNG DỮ LIỆU ({args.date}) ---")
    logger.info(f"Số dòng đọc từ Bronze: {count_raw}")
    logger.info(f"Số dòng thoả mãn Quality Flag = OK: {count_ok}")
    logger.info(f"Số dòng sau khi Dedupe: {count_final}")
    
    if count_final > 0:
        logger.info(f"Ghi xuống Silver tại: {output_path}")
        df_final.coalesce(4).write \
            .format("parquet") \
            .partitionBy("year", "month") \
            .mode("append") \
            .save(output_path)
            
        # Xuất sensor_lookup.json
        lookup_path = "s3a://aq-lake/validation/sensor_lookup.json"
        logger.info(f"Xuất file từ điển {lookup_path}")
        df_final.select("sensor_id", "parameter").distinct().coalesce(1)\
                .write.format("json").mode("overwrite").save(lookup_path)
        
        logger.info("Hoàn tất Spark Làm Sạch!")
    else:
        logger.warning("Không có dữ liệu hợp lệ để ghi xuống Silver.")
