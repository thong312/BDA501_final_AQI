import os
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as _max, count, sum as _sum, collect_set, when, broadcast, struct, hour, to_timestamp, first, udf, dayofweek, month
from pyspark.sql.types import IntegerType, StringType
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))
from common.aqi import compute_aqi

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Đăng ký UDF
compute_aqi_udf = udf(lambda param, val, units: compute_aqi(param, val, units), IntegerType())

def get_epa_level(aqi):
    if aqi is None: return "UNKNOWN"
    if aqi <= 50: return "Good"
    elif aqi <= 100: return "Moderate"
    elif aqi <= 150: return "USG"
    elif aqi <= 200: return "Unhealthy"
    elif aqi <= 300: return "Very Unhealthy"
    else: return "Hazardous"
level_udf = udf(get_epa_level, StringType())

def build_dim_station(df_silver):
    return df_silver.groupBy("location_id", "location_name", "lat", "lon", "borough") \
                    .agg(collect_set("parameter").alias("parameters"))

def build_fact_hourly(df_silver):
    df = df_silver.withColumn("hour_utc", hour(to_timestamp(col("datetime_utc"))))
    df_agg = df.groupBy("location_id", "parameter", "date_local", "hour_utc", "hour_local") \
               .agg(avg("value").alias("hourly_value"), 
                    first("units", ignorenulls=True).alias("units"))
    return df_agg.withColumn("aqi_hourly", compute_aqi_udf(col("parameter"), col("hourly_value"), col("units")))

def build_fact_daily_aqi(df_fact_hourly):
    # Tính đặc thù từng chất (PM2.5: trung bình, O3: max - xấp xỉ cho trung bình trượt 8h)
    df_daily_param = df_fact_hourly.groupBy("location_id", "date_local", "parameter") \
                                   .agg(avg("hourly_value").alias("daily_avg"),
                                        _max("hourly_value").alias("daily_max"),
                                        first("units").alias("units"))
                                        
    df_param_aqi = df_daily_param.withColumn(
        "value_for_aqi",
        when(col("parameter") == 'o3', col("daily_max")).otherwise(col("daily_avg"))
    ).withColumn(
        "param_aqi", compute_aqi_udf(col("parameter"), col("value_for_aqi"), col("units"))
    )
    
    # Tính AQI tổng của ngày (max các chất)
    df_max = df_param_aqi.withColumn("aqi_struct", struct(col("param_aqi"), col("parameter"))) \
                         .groupBy("location_id", "date_local") \
                         .agg(_max("aqi_struct").alias("max_struct"))
                         
    df_daily_aqi = df_max.select(
        col("location_id"),
        col("date_local"),
        col("max_struct.param_aqi").alias("aqi_daily"),
        col("max_struct.parameter").alias("dominant_pollutant")
    ).withColumn("level", level_udf(col("aqi_daily")))
    
    return df_daily_aqi

def build_mart_region_daily(df_fact_daily_aqi, df_dim_station):
    df_joined = df_fact_daily_aqi.join(broadcast(df_dim_station.select("location_id", "borough")), "location_id")
    return df_joined.groupBy("borough", "date_local").agg(
        _max("aqi_daily").alias("aqi_max"),
        avg("aqi_daily").alias("aqi_avg"),
        count("location_id").alias("n_stations"),
        _sum(when(col("aqi_daily") >= 101, 1).otherwise(0)).alias("n_stations_usg")
    )

def build_mart_temporal(df_fact_hourly, df_dim_station):
    df_joined = df_fact_hourly.join(broadcast(df_dim_station.select("location_id", "borough")), "location_id")
    df_temporal = df_joined.withColumn("dow", dayofweek(to_timestamp(col("date_local")))) \
                           .withColumn("month", month(to_timestamp(col("date_local"))))
    return df_temporal.groupBy("borough", "hour_local", "dow", "month").agg(
        avg("aqi_hourly").alias("aqi_avg"),
        _sum(when(col("aqi_hourly") > 100, 1).otherwise(0)).alias("hours_over_100")
    )

def build_cluster_features(df_fact_hourly):
    # Lọc những ngày có >= 18 giờ đo
    df_hours_count = df_fact_hourly.groupBy("location_id", "date_local").agg(count("hour_utc").alias("n_hours"))
    df_valid_days = df_hours_count.filter(col("n_hours") >= 18)
    
    df_valid_hourly = df_fact_hourly.join(df_valid_days.select("location_id", "date_local"), ["location_id", "date_local"])
    
    # Tính các feature
    return df_valid_hourly.groupBy("location_id", "date_local").agg(
        avg("aqi_hourly").alias("aqi_mean"),
        _max("aqi_hourly").alias("aqi_max"),
        _sum(when(col("aqi_hourly") > 100, 1).otherwise(0)).alias("hours_over_100")
    )

def create_spark_session():
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "test")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "test")
    return (SparkSession.builder
            .appName("AQ_Aggregate_Silver_to_Gold")
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
    
    year_str = args.date.split('-')[0]
    month_str = str(int(args.date.split('-')[1]))
    
    silver_path = f"s3a://aq-lake/silver/measurements/year={year_str}/month={month_str}/"
    gold_path = "s3a://aq-lake/gold/"
    
    logger.info(f"Đọc dữ liệu Silver tại: {silver_path}")
    try:
        df_silver = spark.read.parquet(silver_path).filter(col("date_local") == args.date)
        df_silver.cache()
        if df_silver.count() == 0:
            logger.warning("Không có dữ liệu cho ngày này.")
            sys.exit(0)
    except Exception as e:
        logger.error(f"Lỗi đọc Silver: {e}")
        sys.exit(0)
        
    logger.info("Xây dựng các bảng Gold...")
    
    # 1. Dim Station
    df_dim = build_dim_station(df_silver)
    df_dim.cache()
    df_dim.write.format("parquet").mode("overwrite").save(f"{gold_path}dim_station/date_local={args.date}")
    
    # 2. Fact Hourly
    df_fact_hourly = build_fact_hourly(df_silver)
    df_fact_hourly.cache()
    df_fact_hourly.write.format("parquet").mode("overwrite").save(f"{gold_path}fact_hourly/date_local={args.date}")
    
    # Lưu Explain Plan cho Broadcast Join
    report_dir = Path(__file__).parent.parent.parent / "reports" / "plans"
    report_dir.mkdir(parents=True, exist_ok=True)
    
    # 3. Fact Daily AQI
    df_fact_daily = build_fact_daily_aqi(df_fact_hourly)
    df_fact_daily.write.format("parquet").mode("overwrite").save(f"{gold_path}fact_daily_aqi/date_local={args.date}")
    
    # 4. Mart Region Daily
    df_mart_region = build_mart_region_daily(df_fact_daily, df_dim)
    with open(report_dir / f"mart_region_plan_{args.date}.txt", "w") as f:
        f.write(df_mart_region._jdf.queryExecution().simpleString()) # Lưu query plan (Explain)
    df_mart_region.write.format("parquet").mode("overwrite").save(f"{gold_path}mart_region_daily/date_local={args.date}")
    
    # 5. Mart Temporal
    df_mart_temp = build_mart_temporal(df_fact_hourly, df_dim)
    with open(report_dir / f"mart_temporal_plan_{args.date}.txt", "w") as f:
        f.write(df_mart_temp._jdf.queryExecution().simpleString())
    df_mart_temp.write.format("parquet").mode("overwrite").save(f"{gold_path}mart_temporal/date_local={args.date}")
    
    # 6. Cluster Features
    df_cluster = build_cluster_features(df_fact_hourly)
    df_cluster.write.format("parquet").mode("overwrite").save(f"{gold_path}cluster_features/date_local={args.date}")
    
    logger.info(f"Hoàn thành tổng hợp Gold Data Lake tại {gold_path}!")
