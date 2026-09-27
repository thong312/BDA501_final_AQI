"""Silver -> Gold (ARCHITECTURE 6.6).

fact_hourly       (location_id, parameter, hour_utc)  nồng độ trung bình giờ, aqi_hourly
fact_daily_aqi    (location_id, date_local)           AQI ngày chuẩn EPA:
                     PM2.5/PM10: trung bình 24h (>= 18 giờ có dữ liệu)
                     O3: max trung bình trượt 8h (>= 6 giờ trong cửa sổ), so với max 1h (bảng 1h)
                     CO: max trung bình trượt 8h; NO2/SO2: max 1h
                     aqi_daily = max các chất, dominant_pollutant, level
dim_station       (location_id)                       tên, lat, lon, borough, danh sách chất đo
mart_region_daily (borough, date_local)
mart_temporal     (borough, grain, bucket)            grain ∈ hour_local / dow / month, trên toàn lịch sử
cluster_features  (location_id, date_local)           chỉ ngày có >= 18 giờ dữ liệu

fact_* và mart_region_daily, cluster_features partition theo date_local, ghi đè đúng các ngày xử lý.
Join fact_* với dim_station bằng broadcast; plan lưu ở reports/plans/.
"""
import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType

from common.aqi import compute_aqi, get_level, get_level_name
from common.config import GOLD_PATH, REPORTS_DIR, SILVER_PATH, TIMEZONE
from common.spark_utils import create_spark_session, existing_paths, explain_string

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("gold")

MIN_HOURS_DAILY = 18
MIN_HOURS_8H = 6


def _aqi_series(parameter, value, units, averaging):
    out = [compute_aqi(p, v, u, averaging=a) for p, v, u, a in zip(parameter, value, units, averaging)]
    return pd.Series([float("nan") if x is None else float(x) for x in out])


@pandas_udf(DoubleType())
def aqi_udf(parameter: pd.Series, value: pd.Series, units: pd.Series, averaging: pd.Series) -> pd.Series:
    return _aqi_series(parameter, value, units, averaging)


@pandas_udf(StringType())
def level_name_udf(aqi: pd.Series) -> pd.Series:
    return pd.Series([None if pd.isna(a) else get_level_name(get_level(a)) for a in aqi])


def aqi_col(parameter, value, units, averaging: str):
    return aqi_udf(parameter, value, units, F.lit(averaging)).cast("int")


# ------------------------------------------------------------------ builders (hàm thuần)

def build_fact_hourly(df_silver: DataFrame) -> DataFrame:
    hourly = (df_silver
              .withColumn("hour_utc", F.date_trunc("hour", F.col("event_time")))
              .groupBy("location_id", "parameter", "hour_utc")
              .agg(F.avg("value").alias("value_avg"),
                   F.count("value").alias("n_readings"),
                   F.first("units", ignorenulls=True).alias("units")))
    ts_local = F.from_utc_timestamp("hour_utc", TIMEZONE)
    return (hourly.withColumn("date_local", F.to_date(ts_local))
            .withColumn("hour_local", F.hour(ts_local))
            .withColumn("aqi_hourly", aqi_col(F.col("parameter"), F.col("value_avg"), F.col("units"), "instant")))


def _rolling_8h(df_hourly: DataFrame) -> DataFrame:
    """Trung bình trượt 8h kết thúc tại mỗi giờ (cần >= 6 giờ có dữ liệu trong cửa sổ)."""
    w = (Window.partitionBy("location_id", "parameter")
         .orderBy(F.col("hour_utc").cast("long")).rangeBetween(-7 * 3600, 0))
    return (df_hourly.withColumn("avg_8h", F.avg("value_avg").over(w))
            .withColumn("n_8h", F.count("value_avg").over(w))
            .withColumn("avg_8h", F.when(F.col("n_8h") >= MIN_HOURS_8H, F.col("avg_8h"))))


def build_fact_daily_aqi(df_fact_hourly: DataFrame) -> DataFrame:
    """AQI ngày chuẩn EPA. df_fact_hourly nên gồm cả ngày trước để có cửa sổ 8h đầu ngày."""
    rolled = _rolling_8h(df_fact_hourly)
    per_param = (rolled.groupBy("location_id", "date_local", "parameter")
                 .agg(F.avg("value_avg").alias("avg_24h"),
                      F.count("value_avg").alias("n_hours"),
                      F.max("value_avg").alias("max_1h"),
                      F.max("avg_8h").alias("max_8h"),
                      F.first("units", ignorenulls=True).alias("units")))
    p, u = F.col("parameter"), F.col("units")
    aqi_24h = aqi_col(p, F.col("avg_24h"), u, "24h")
    aqi_8h = aqi_col(p, F.col("max_8h"), u, "8h")
    aqi_1h_o3 = aqi_col(p, F.col("max_1h"), u, "1h")
    aqi_1h = aqi_col(p, F.col("max_1h"), u, "instant")
    param_aqi = (F.when(p.isin("pm25", "pm10"), F.when(F.col("n_hours") >= MIN_HOURS_DAILY, aqi_24h))
                 .when(p == "o3", F.greatest(aqi_8h, aqi_1h_o3))
                 .when(p == "co", aqi_8h)
                 .when(p.isin("no2", "so2"), aqi_1h))
    per_param = per_param.withColumn("param_aqi", param_aqi).filter(F.col("param_aqi").isNotNull())
    return (per_param.groupBy("location_id", "date_local")
            .agg(F.max_by("parameter", "param_aqi").alias("dominant_pollutant"),
                 F.max("param_aqi").alias("aqi_daily"))
            .withColumn("level", level_name_udf("aqi_daily")))


def build_dim_station(df_silver: DataFrame, df_existing: DataFrame = None) -> DataFrame:
    new = (df_silver.groupBy("location_id")
           .agg(F.max_by("location_name", "event_time").alias("name"),
                F.max_by("lat", "event_time").alias("lat"),
                F.max_by("lon", "event_time").alias("lon"),
                F.max_by("borough", "event_time").alias("borough"),
                F.sort_array(F.collect_set("parameter")).alias("parameters")))
    if df_existing is None:
        return new
    # Gộp với dim cũ: ưu tiên thông tin mới, hợp danh sách chất đo
    both = (new.withColumn("prio", F.lit(0))
            .unionByName(df_existing.select(new.columns).withColumn("prio", F.lit(1))))
    return both.groupBy("location_id").agg(
        *[F.min_by(c, "prio").alias(c) for c in ("name", "lat", "lon", "borough")],
        F.sort_array(F.array_distinct(F.flatten(F.collect_list("parameters")))).alias("parameters"))


def build_mart_region_daily(df_fact_daily: DataFrame, df_dim: DataFrame) -> DataFrame:
    return (df_fact_daily.join(F.broadcast(df_dim.select("location_id", "borough")), "location_id")
            .filter(F.col("borough").isNotNull())
            .groupBy("borough", "date_local")
            .agg(F.max("aqi_daily").alias("aqi_max"),
                 F.avg("aqi_daily").alias("aqi_avg"),
                 F.countDistinct("location_id").alias("n_stations"),
                 F.sum(F.when(F.col("aqi_daily") >= 101, 1).otherwise(0)).alias("n_stations_usg")))


def station_hourly_aqi(df_fact_hourly: DataFrame) -> DataFrame:
    """AQI trạm theo giờ = max các chất trong giờ đó."""
    return (df_fact_hourly.filter(F.col("aqi_hourly").isNotNull())
            .groupBy("location_id", "hour_utc", "date_local", "hour_local")
            .agg(F.max("aqi_hourly").alias("aqi"),
                 F.max(F.when(F.col("parameter") == "pm25", F.col("aqi_hourly"))).alias("aqi_pm25"),
                 F.max(F.when(F.col("parameter") == "o3", F.col("aqi_hourly"))).alias("aqi_o3")))


def build_mart_temporal(df_fact_hourly: DataFrame, df_dim: DataFrame) -> DataFrame:
    hourly = (station_hourly_aqi(df_fact_hourly)
              .join(F.broadcast(df_dim.select("location_id", "borough")), "location_id")
              .filter(F.col("borough").isNotNull())
              .withColumn("dow", F.dayofweek("date_local"))
              .withColumn("month", F.month("date_local")))
    parts = []
    for grain in ("hour_local", "dow", "month"):
        parts.append(hourly.groupBy("borough", F.col(grain).alias("bucket"))
                     .agg(F.avg("aqi").alias("aqi_avg"),
                          F.sum(F.when(F.col("aqi") > 100, 1).otherwise(0)).alias("hours_over_100"),
                          F.count("aqi").alias("n_station_hours"))
                     .withColumn("grain", F.lit(grain)))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.select("borough", "grain", "bucket", "aqi_avg", "hours_over_100", "n_station_hours")


def build_cluster_features(df_fact_hourly: DataFrame) -> DataFrame:
    hourly = station_hourly_aqi(df_fact_hourly)
    peak = Window.partitionBy("location_id", "date_local").orderBy(F.col("aqi").desc(), F.col("hour_local"))
    peak_hour = (hourly.withColumn("rn", F.row_number().over(peak)).filter("rn = 1")
                 .select("location_id", "date_local", F.col("hour_local").alias("peak_hour")))
    feats = (hourly.groupBy("location_id", "date_local")
             .agg(F.count("aqi").alias("n_hours"),
                  F.avg("aqi").alias("aqi_mean"),
                  F.max("aqi").alias("aqi_max"),
                  F.sum(F.when(F.col("aqi") > 100, 1).otherwise(0)).alias("hours_over_100"),
                  F.avg("aqi_pm25").alias("pm25_mean"),
                  F.avg("aqi_o3").alias("o3_mean"))
             .filter(F.col("n_hours") >= MIN_HOURS_DAILY))
    # Tỉ trọng PM2.5 trong (PM2.5 + O3) theo AQI trung bình: 1 = thuần bụi mịn, 0 = thuần ozone
    pm, o3 = F.coalesce("pm25_mean", F.lit(0.0)), F.coalesce("o3_mean", F.lit(0.0))
    ratio = F.when((pm + o3) > 0, pm / (pm + o3))
    return (feats.join(peak_hour, ["location_id", "date_local"])
            .withColumn("pm25_o3_ratio", ratio)
            .filter(F.col("pm25_o3_ratio").isNotNull())
            .select("location_id", "date_local", "aqi_mean", "aqi_max", "hours_over_100", "peak_hour",
                    "pm25_o3_ratio", "n_hours"))


# ------------------------------------------------------------------ main

def save_plan(df: DataFrame, name: str):
    plan_dir = REPORTS_DIR / "plans"
    plan_dir.mkdir(parents=True, exist_ok=True)
    (plan_dir / f"{name}.txt").write_text(explain_string(df, "formatted"), encoding="utf-8")


def read_silver(spark, date_from: date, date_to: date):
    days = [date_from - timedelta(days=1) + timedelta(days=i) for i in range((date_to - date_from).days + 2)]
    paths, _ = existing_paths(spark, [f"{SILVER_PATH}year={d.year}/month={d.month}/date_local={d}" for d in days])
    if not paths:
        return None
    return spark.read.option("basePath", SILVER_PATH).parquet(*paths)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date")
    parser.add_argument("--from", dest="date_from")
    parser.add_argument("--to", dest="date_to")
    args = parser.parse_args()
    if args.date:
        date_from = date_to = date.fromisoformat(args.date)
    elif args.date_from and args.date_to:
        date_from, date_to = date.fromisoformat(args.date_from), date.fromisoformat(args.date_to)
    else:
        parser.error("cần --date hoặc --from/--to")

    spark = create_spark_session("AQ_Aggregate_Silver_to_Gold",
                                 conf={"spark.sql.sources.partitionOverwriteMode": "dynamic"})
    df_silver = read_silver(spark, date_from, date_to)
    if df_silver is None:
        logger.warning("Không có Silver cho %s..%s", date_from, date_to)
        return
    df_silver = df_silver.cache()
    in_range = F.col("date_local").between(F.lit(date_from), F.lit(date_to))

    # dim_station: gộp với bản cũ rồi ghi đè toàn bộ (bảng nhỏ, materialize trước khi ghi đè)
    existing, _ = existing_paths(spark, [f"{GOLD_PATH}dim_station"])
    df_dim_old = spark.read.parquet(f"{GOLD_PATH}dim_station") if existing else None
    dim_rows = build_dim_station(df_silver, df_dim_old).collect()
    df_dim = spark.createDataFrame(dim_rows, "location_id int, name string, lat double, lon double, "
                                              "borough string, parameters array<string>").cache()
    df_dim.write.mode("overwrite").parquet(f"{GOLD_PATH}dim_station")

    # fact_hourly (tính cả ngày trước để có cửa sổ 8h, chỉ ghi các ngày trong khoảng)
    df_hourly_all = build_fact_hourly(df_silver).cache()
    df_hourly = df_hourly_all.filter(in_range)
    df_hourly.write.mode("overwrite").partitionBy("date_local").parquet(f"{GOLD_PATH}fact_hourly")

    df_daily = build_fact_daily_aqi(df_hourly_all).filter(in_range).cache()
    df_daily.write.mode("overwrite").partitionBy("date_local").parquet(f"{GOLD_PATH}fact_daily_aqi")

    df_region = build_mart_region_daily(df_daily, df_dim)
    save_plan(df_region, "mart_region_daily")
    df_region.write.mode("overwrite").partitionBy("date_local").parquet(f"{GOLD_PATH}mart_region_daily")

    df_cluster = build_cluster_features(df_hourly)
    df_cluster.write.mode("overwrite").partitionBy("date_local").parquet(f"{GOLD_PATH}cluster_features")

    # mart_temporal trên toàn bộ lịch sử fact_hourly (đọc lại sau khi đã ghi các ngày mới)
    df_temporal = build_mart_temporal(spark.read.parquet(f"{GOLD_PATH}fact_hourly"), df_dim)
    save_plan(df_temporal, "mart_temporal")
    rows = df_temporal.collect()
    spark.createDataFrame(rows, df_temporal.schema).write.mode("overwrite").parquet(f"{GOLD_PATH}mart_temporal")

    logger.info("Gold %s..%s: dim=%d daily=%d", date_from, date_to, len(dim_rows), df_daily.count())


if __name__ == "__main__":
    main()
