import os
import time
import csv
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "test")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "test")
    return (SparkSession.builder
            .appName("AQ_Performance_Test")
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Truy xuất dữ liệu (cần chạy M7, M9 trước)
    silver_path = "s3a://aq-lake/silver/measurements/"
    dim_path = "s3a://aq-lake/gold/dim_station/"
    
    logger.info("Đang đọc dữ liệu cho thí nghiệm...")
    try:
        df_silver = spark.read.parquet(silver_path)
        df_dim = spark.read.parquet(dim_path)
    except Exception as e:
        logger.error("Dữ liệu Silver/Gold chưa có sẵn để đo hiệu năng. Vui lòng chạy luồng Data Pipeline trước.")
        import sys; sys.exit(0)

    results = []

    # BÀI TEST 1: KHÔNG DÙNG BROADCAST (SHUFFLE JOIN)
    logger.info("Test 1: Shuffle Join (Tắt Broadcast)...")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    start = time.time()
    df_join_1 = df_silver.join(df_dim, "location_id")
    c1 = df_join_1.count() # Ép Spark thực thi
    end = time.time()
    results.append({"experiment": "No_Broadcast_Shuffle", "time_seconds": round(end - start, 2), "records": c1})

    # BÀI TEST 2: DÙNG BROADCAST JOIN
    logger.info("Test 2: Broadcast Hash Join...")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") # 10MB
    start = time.time()
    df_join_2 = df_silver.join(broadcast(df_dim), "location_id")
    c2 = df_join_2.count()
    end = time.time()
    results.append({"experiment": "With_Broadcast", "time_seconds": round(end - start, 2), "records": c2})

    # BÀI TEST 3: REPARTITION LÊN 20 PHÂN VÙNG
    logger.info("Test 3: Tăng Partition lên 20 + Broadcast...")
    df_silver_rep = df_silver.repartition(20)
    start = time.time()
    df_join_3 = df_silver_rep.join(broadcast(df_dim), "location_id")
    c3 = df_join_3.count()
    end = time.time()
    results.append({"experiment": "Broadcast_20_Partitions", "time_seconds": round(end - start, 2), "records": c3})

    # LƯU KẾT QUẢ VÀO FILE BÁO CÁO
    report_dir = Path(__file__).parent.parent.parent / "reports" / "perf"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_file = report_dir / "perf_results.csv"
    
    with open(report_file, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["experiment", "time_seconds", "records"])
        writer.writeheader()
        writer.writerows(results)
        
    logger.info(f"Hoàn thành thí nghiệm hiệu năng. Báo cáo được lưu tại: {report_file}")
    for r in results:
        print(r)
