import os
import argparse
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, max as _max, avg, when, expr
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "test")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "test")
    return (SparkSession.builder
            .appName("AQ_Validate_MR")
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
    
    year = args.date.split('-')[0]
    month = str(int(args.date.split('-')[1]))
    
    silver_path = f"s3a://aq-lake/silver/measurements/year={year}/month={month}/"
    mr_path = f"s3a://aq-lake/validation/mr_daily_stats/date={args.date}/*"
    
    logger.info(f"Đọc dữ liệu Silver đã làm sạch tại: {silver_path}")
    try:
        df_silver = spark.read.parquet(silver_path).filter(col("date_local") == args.date)
    except Exception as e:
        logger.error(f"Lỗi đọc Silver: {e}")
        sys.exit(0)
        
    logger.info("Tính toán thống kê hằng ngày bằng PySpark...")
    df_sp_metrics = df_silver.groupBy("location_id", "parameter", "date_local").agg(
        count("value").alias("sp_count"),
        _sum("value").alias("sp_sum"),
        _max("value").alias("sp_max"),
        avg("value").alias("sp_avg"),
        _sum(when(col("value") > 100, 1).otherwise(0)).alias("sp_hours_over_100")
    )
    
    logger.info(f"Đọc báo cáo MapReduce tại: {mr_path}")
    try:
        df_mr_raw = spark.read.text(mr_path)
        def parse_mr_line(line):
            try:
                k_str, v_str = line.split('\t', 1)
                k = json.loads(k_str)
                v = json.loads(v_str)
                return (int(k[0]), k[1], k[2], v['count'], v['sum'], v['max'], v['avg'], v['hours_over_100'])
            except:
                return None
        rdd_mr = df_mr_raw.rdd.map(lambda r: parse_mr_line(r.value)).filter(lambda x: x is not None)
        df_mr_metrics = spark.createDataFrame(rdd_mr, ["location_id", "parameter", "date_local", "mr_count", "mr_sum", "mr_max", "mr_avg", "mr_hours_over_100"])
    except Exception as e:
        logger.error(f"Lỗi đọc MR: {e}")
        sys.exit(0)
        
    logger.info("Bắt đầu đối chiếu Full Outer Join...")
    df_join = df_sp_metrics.join(df_mr_metrics, ["location_id", "parameter", "date_local"], "full_outer")
    df_join.cache()
    
    total_keys = df_join.count()
    sp_only = df_join.filter(col("mr_count").isNull()).count()
    mr_only = df_join.filter(col("sp_count").isNull()).count()
    matched = df_join.filter(col("mr_count").isNotNull() & col("sp_count").isNotNull())
    num_matched = matched.count()
    
    diffs = matched.withColumn("diff_avg", expr("abs(sp_avg - mr_avg)")).withColumn("diff_max", expr("abs(sp_max - mr_max)"))
    max_diff_avg = diffs.agg(_max("diff_avg")).collect()[0][0] or 0.0
    max_diff_max = diffs.agg(_max("diff_max")).collect()[0][0] or 0.0
    
    status = "PASS" if (sp_only == 0 and mr_only == 0 and max_diff_avg < 0.01 and max_diff_max < 0.01) else "FAIL"
    
    report = {
        "date": args.date,
        "total_keys": total_keys,
        "matched_keys": num_matched,
        "spark_only": sp_only,
        "mr_only": mr_only,
        "max_diff_avg": max_diff_avg,
        "max_diff_max": max_diff_max,
        "status": status
    }
    
    report_dir = Path(__file__).parent.parent.parent / "reports" / "validation"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_file = report_dir / f"{args.date}.json"
    
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2)
        
    logger.info(f"Kết quả đối chiếu: {status}. Báo cáo được xuất ra tại: {report_file}")
    print(json.dumps(report, indent=2))
