"""Doi chieu MapReduce voi Spark (ARCHITECTURE 6.7).

Tinh lai tu Silver dung cac chi so cua MapReduce theo cung khoa (location_id, parameter, date_local),
full outer join voi output MR, ghi reports/validation/YYYY-MM-DD.json. Exit code 1 neu co lech.
"""
import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import REPORTS_DIR, SILVER_PATH, VALIDATION_PATH
from common.quality import spark_threshold
from common.spark_utils import create_spark_session, existing_paths

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("validate")

KEYS = ["location_id", "parameter", "date_local"]
METRICS = ["count", "sum", "max", "avg", "hours_over_threshold"]
TOLERANCE = 1e-6


def spark_daily_stats(df_silver: DataFrame) -> DataFrame:
    thr = spark_threshold(F.col("parameter"), F.col("units"))
    return (df_silver.withColumn("date_local", F.col("date_local").cast("string"))
            .groupBy(*KEYS).agg(
                F.count("value").alias("sp_count"),
                F.sum("value").alias("sp_sum"),
                F.max("value").alias("sp_max"),
                F.avg("value").alias("sp_avg"),
                F.sum(F.when(thr.isNotNull() & (F.col("value") > thr), 1).otherwise(0))
                 .alias("sp_hours_over_threshold")))


def parse_mr_output(spark, lines_df: DataFrame) -> DataFrame:
    def parse(line):
        k, v = line.split("\t", 1)
        k, v = json.loads(k), json.loads(v)
        return (int(k[0]), k[1], k[2], int(v["count"]), float(v["sum"]), float(v["max"]), float(v["avg"]),
                int(v["hours_over_threshold"]))
    rdd = lines_df.rdd.map(lambda r: r.value).filter(lambda s: s.strip()).map(parse)
    schema = ("location_id int, parameter string, date_local string, mr_count long, mr_sum double, "
              "mr_max double, mr_avg double, mr_hours_over_threshold long")
    return spark.createDataFrame(rdd, schema)


def compare(df_sp: DataFrame, df_mr: DataFrame) -> dict:
    joined = df_sp.join(df_mr, KEYS, "full_outer").cache()
    both = joined.filter(F.col("sp_count").isNotNull() & F.col("mr_count").isNotNull())
    diffs = both.select(*KEYS, *[F.abs(F.col(f"sp_{m}") - F.col(f"mr_{m}")).alias(f"diff_{m}") for m in METRICS])
    max_diff = diffs.agg(*[F.max(f"diff_{m}").alias(m) for m in METRICS]).collect()[0].asDict()
    bad = diffs.filter(" OR ".join(f"diff_{m} > {TOLERANCE}" for m in METRICS))
    report = {
        "total_keys": joined.count(),
        "matched_keys": both.count(),
        "spark_only": joined.filter(F.col("mr_count").isNull()).count(),
        "mr_only": joined.filter(F.col("sp_count").isNull()).count(),
        "value_mismatches": bad.count(),
        "max_abs_diff": {m: (max_diff[m] or 0.0) for m in METRICS},
        "sample_mismatches": [r.asDict() for r in bad.limit(20).collect()],
        "sample_one_sided": [r.asDict() for r in joined.filter(F.col("mr_count").isNull() | F.col("sp_count").isNull())
                             .select(*KEYS, "sp_count", "mr_count").limit(20).collect()],
    }
    ok = report["spark_only"] == 0 and report["mr_only"] == 0 and report["value_mismatches"] == 0
    report["status"] = "PASS" if ok else "FAIL"
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    day = date.fromisoformat(args.date)
    spark = create_spark_session("AQ_Validate_MR_vs_Spark")

    silver_part = f"{SILVER_PATH}year={day.year}/month={day.month}/date_local={day}"
    mr_path = f"{VALIDATION_PATH}mr_daily_stats/date={day}/"
    found, _ = existing_paths(spark, [silver_part, mr_path])
    if len(found) < 2:
        logger.error("Thieu du lieu de doi chieu (tim thay: %s)", found)
        sys.exit(1)

    df_silver = (spark.read.option("basePath", SILVER_PATH).parquet(silver_part))
    report = {"date": str(day), **compare(spark_daily_stats(df_silver), parse_mr_output(spark, spark.read.text(mr_path)))}

    out_dir = REPORTS_DIR / "validation"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{day}.json"
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info("Doi chieu %s: %s (%s)", day, report["status"], out)
    if report["status"] != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
