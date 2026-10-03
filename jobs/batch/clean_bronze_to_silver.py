"""Bronze -> Silver (ARCHITECTURE 6.5).

Mot ngay New York D gom cac ban do co datetime_utc trong [D 04:00Z/05:00Z, D+1 04:00Z/05:00Z), nen
doc Bronze ingest_date D va D+1 roi loc theo date_local. Ghi de dung partition date_local
(dynamic overwrite) nen chay lai cung ngay cho ket qua nhu nhau.

Chay:  clean_bronze_to_silver.py --date 2026-09-26
       clean_bronze_to_silver.py --from 2024-01-01 --to 2024-12-31   (xu ly lai lich su)
"""
import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField, StructType)

from common.config import (BRONZE_PATH, DEFAULT_BBOX, REPORTS_DIR, SILVER_PATH, TIMEZONE,
                           VALIDATION_PATH)
from common.geo import get_borough, parse_bbox
from common.quality import BATCH_DROP_FLAGS, spark_quality_flag
from common.schemas import MEASUREMENT_STRUCT
from common.spark_utils import create_spark_session, existing_paths, read_sensor_metadata, write_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("clean")

BRONZE_STRUCT = StructType([
    StructField("value", StringType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("timestamp", StringType()),
])

SILVER_COLUMNS = ["sensor_id", "location_id", "location_name", "parameter", "units", "value",
                  "lat", "lon", "datetime_utc", "event_time", "datetime_local", "date_local", "hour_local",
                  "borough", "quality_flag", "ingested_at", "source", "year", "month"]


def parse_bronze(df_bronze: DataFrame) -> DataFrame:
    offset = F.col("offset") if "offset" in df_bronze.columns else F.lit(0).cast("long")
    return (df_bronze
            .select(F.from_json(F.col("value"), MEASUREMENT_STRUCT).alias("m"), offset.alias("kafka_offset"))
            .select("m.*", "kafka_offset")
            .filter(F.col("sensor_id").isNotNull()))


def prepare_metadata(df_meta: DataFrame) -> DataFrame:
    return df_meta.select(
        "sensor_id",
        F.col("parameter").alias("meta_parameter"),
        F.col("units").alias("meta_units"),
        F.col("location_name").alias("meta_location_name"),
        F.col("lat").alias("meta_lat"),
        F.col("lon").alias("meta_lon"))


def borough_lookup(df: DataFrame) -> DataFrame:
    """(location_id, lat, lon) it -> gan borough tren driver bang shapely, tra DataFrame nho de broadcast."""
    spark = df.sparkSession
    rows = df.select("location_id", "lat", "lon").distinct().collect()
    data = [(r.location_id, r.lat, r.lon, get_borough(r.lat, r.lon)) for r in rows]
    schema = "location_id int, lat double, lon double, borough string"
    return spark.createDataFrame(data, schema)


def transform_silver(df_parsed: DataFrame, df_meta: DataFrame, bbox_str: str,
                     date_from: date = None, date_to: date = None, stats: dict = None):
    """Ham thuan: join metadata, loc bbox, quality_flag, dedupe, them cot thoi gian dia phuong + borough."""
    stats = stats if stats is not None else {}
    min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox_str)

    # 1. Metadata cho ban ghi streaming (backfill da co san parameter/units/location_name)
    df = (df_parsed.join(F.broadcast(df_meta), "sensor_id", "left")
          .withColumn("parameter", F.coalesce("parameter", "meta_parameter"))
          .withColumn("units", F.coalesce("units", "meta_units"))
          .withColumn("location_name", F.coalesce("location_name", "meta_location_name"))
          .withColumn("lat", F.coalesce("lat", "meta_lat"))
          .withColumn("lon", F.coalesce("lon", "meta_lon"))
          .drop("meta_parameter", "meta_units", "meta_location_name", "meta_lat", "meta_lon"))

    # 2. Thoi gian: UTC chuan hoa + gio New York tuong minh
    df = (df.withColumn("event_time", F.to_timestamp("datetime_utc"))
          .filter(F.col("event_time").isNotNull())
          .withColumn("ts_local", F.from_utc_timestamp("event_time", TIMEZONE))
          .withColumn("date_local", F.to_date("ts_local"))
          .withColumn("hour_local", F.hour("ts_local"))
          .withColumn("datetime_utc", F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'")))
    if date_from is not None:
        df = df.filter(F.col("date_local").between(F.lit(date_from), F.lit(date_to or date_from)))

    # 3. Loc bbox NYC
    df = df.filter(F.col("lat").between(min_lat, max_lat) & F.col("lon").between(min_lon, max_lon))

    # 4. Quality flag (cung luat voi Streaming/MapReduce); batch giu STALE.
    #    STALE chi co nghia voi du lieu realtime: ingested_at cua backfill la luc chay backfill.
    reference_ts = F.when(F.col("source") != "openaq-archive", F.to_timestamp("ingested_at"))
    df = df.withColumn("quality_flag", spark_quality_flag(
        F.col("parameter"), F.col("units"), F.col("value"), F.col("event_time"), reference_ts))
    stats["flagged"] = df
    df = df.filter(~F.col("quality_flag").isin(*BATCH_DROP_FLAGS))

    # 5. Dedupe (sensor_id, datetime_utc), giu ban ingested_at moi nhat
    w = Window.partitionBy("sensor_id", "event_time").orderBy(
        F.col("ingested_at").desc_nulls_last(), F.col("kafka_offset").desc(), F.col("value").desc())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    # 6. Borough + cot partition
    boroughs = borough_lookup(df)
    df = (df.join(F.broadcast(boroughs), ["location_id", "lat", "lon"], "left")
          .withColumn("year", F.year("date_local"))
          .withColumn("month", F.month("date_local")))
    return df.select(*SILVER_COLUMNS)


def bronze_paths(date_from: date, date_to: date):
    days = (date_to - date_from).days + 2  # them D+1
    return [f"{BRONZE_PATH}ingest_date={date_from + timedelta(days=i)}" for i in range(days)]


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
        parser.error("can --date hoac --from/--to")

    spark = create_spark_session("AQ_Clean_Bronze_to_Silver",
                                 conf={"spark.sql.sources.partitionOverwriteMode": "dynamic"})
    bbox = os.getenv("NYC_BBOX", DEFAULT_BBOX)

    try:
        df_meta = prepare_metadata(read_sensor_metadata(spark, os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))).cache()
        logger.info("Metadata: %d sensors", df_meta.count())
    except Exception as e:  # Kafka khong san sang: ban ghi streaming se bi NO_METADATA
        logger.warning("Khong doc duoc metadata tu Kafka (%s); tiep tuc khong co metadata", e)
        df_meta = prepare_metadata(spark.createDataFrame(
            [], "sensor_id int, parameter string, units string, location_name string, lat double, lon double"))

    paths, _ = existing_paths(spark, bronze_paths(date_from, date_to))
    if not paths:
        logger.warning("Khong co Bronze cho %s..%s", date_from, date_to)
        return
    df_bronze = spark.read.schema(BRONZE_STRUCT).json(paths)
    df_parsed = parse_bronze(df_bronze).cache()

    stats = {}
    df_silver = transform_silver(df_parsed, df_meta, bbox, date_from, date_to, stats).cache()

    # Bao cao data quality: so dong truoc/sau tung buoc
    report = {"date_from": str(date_from), "date_to": str(date_to),
              "bronze_rows": df_bronze.count(), "parsed_rows": df_parsed.count()}
    flag_counts = {r["quality_flag"]: r["count"] for r in stats["flagged"].groupBy("quality_flag").count().collect()}
    report["in_date_range_and_bbox"] = sum(flag_counts.values())
    report["quality_flags"] = flag_counts
    report["after_quality_filter"] = sum(v for k, v in flag_counts.items() if k not in BATCH_DROP_FLAGS)
    report["silver_rows_after_dedupe"] = df_silver.count()
    logger.info("DATA QUALITY %s", json.dumps(report))
    report_dir = REPORTS_DIR / "data_quality"
    report_dir.mkdir(parents=True, exist_ok=True)
    name = str(date_from) if date_from == date_to else f"{date_from}_{date_to}"
    (report_dir / f"{name}.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    if report["silver_rows_after_dedupe"] == 0:
        logger.warning("Khong co du lieu hop le de ghi Silver.")
        return

    # Compact: moi partition ngay mot file; chi ghi de cac ngay co trong lan chay nay
    (df_silver.repartition("year", "month", "date_local").write
     .mode("overwrite").partitionBy("year", "month", "date_local").parquet(SILVER_PATH))
    logger.info("Da ghi Silver: %s", SILVER_PATH)

    # Bang tra sensor -> parameter/units cho MapReduce (mot file JSON Lines)
    lookup = (df_meta.select("sensor_id", F.col("meta_parameter").alias("parameter"),
                             F.col("meta_units").alias("units"))
              .unionByName(df_silver.select("sensor_id", "parameter", "units"))
              .filter(F.col("parameter").isNotNull())
              .dropDuplicates(["sensor_id"]).collect())
    write_text(spark, f"{VALIDATION_PATH}sensor_lookup.json",
               "".join(json.dumps(r.asDict(), ensure_ascii=False) + "\n" for r in lookup))
    logger.info("Da xuat %d sensor vao %ssensor_lookup.json", len(lookup), VALIDATION_PATH)


if __name__ == "__main__":
    main()
