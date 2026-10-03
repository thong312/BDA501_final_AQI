"""Spark SQL + KMeans, ghi schema analytics trong PostgreSQL (ARCHITECTURE 6.8).

Spark SQL (ket qua -> reports/sql/*.csv, EXPLAIN FORMATTED -> reports/plans/*.txt):
  q1_borough_bad_days_rank   xep hang borough theo so ngay aqi_daily > 100, theo nam
  q2_borough_hour            AQI trung binh theo borough × hour_local (xu huong trong ngay)
  q2_borough_month           AQI trung binh theo borough × month (mua)
  q3_widespread_days         ngay co >= 50% so tram cung >= USG

KMeans: VectorAssembler -> StandardScaler -> KMeans(k, seed=42), thu k = 2..7, chon theo silhouette
(hoac --k). Model luu s3a://aq-lake/models/kmeans/vN/.
"""
import argparse
import csv
import json
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common.config import GOLD_PATH, LAKE, REPORTS_DIR, load_yaml
from common.spark_utils import create_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("analytics")

FEATURES = ["aqi_mean", "aqi_max", "hours_over_100", "peak_hour", "pm25_o3_ratio"]
MODELS_PATH = f"{LAKE}/models/kmeans"

SQL = {
    "q1_borough_bad_days_rank": """
        WITH bad AS (
            SELECT s.borough, year(d.date_local) AS year, COUNT(DISTINCT d.date_local) AS bad_days
            FROM fact_daily_aqi d JOIN dim_station s ON d.location_id = s.location_id
            WHERE d.aqi_daily > 100 AND s.borough IS NOT NULL
            GROUP BY s.borough, year(d.date_local))
        SELECT year, borough, bad_days, RANK() OVER (PARTITION BY year ORDER BY bad_days DESC) AS rank
        FROM bad ORDER BY year, rank""",
    "q2_borough_hour": """
        WITH station_hour AS (
            SELECT location_id, hour_utc, hour_local, MAX(aqi_hourly) AS aqi
            FROM fact_hourly WHERE aqi_hourly IS NOT NULL
            GROUP BY location_id, hour_utc, hour_local)
        SELECT s.borough, h.hour_local, ROUND(AVG(h.aqi), 1) AS aqi_avg, COUNT(*) AS n_station_hours
        FROM station_hour h JOIN dim_station s ON h.location_id = s.location_id
        WHERE s.borough IS NOT NULL
        GROUP BY s.borough, h.hour_local ORDER BY s.borough, h.hour_local""",
    "q2_borough_month": """
        SELECT s.borough, month(d.date_local) AS month, ROUND(AVG(d.aqi_daily), 1) AS aqi_avg,
               SUM(CASE WHEN d.aqi_daily > 100 THEN 1 ELSE 0 END) AS station_days_over_100
        FROM fact_daily_aqi d JOIN dim_station s ON d.location_id = s.location_id
        WHERE s.borough IS NOT NULL
        GROUP BY s.borough, month(d.date_local) ORDER BY s.borough, month""",
    "q3_widespread_days": """
        WITH per_day AS (
            SELECT d.date_local, COUNT(*) AS n_stations,
                   SUM(CASE WHEN d.aqi_daily >= 101 THEN 1 ELSE 0 END) AS n_usg
            FROM fact_daily_aqi d JOIN dim_station s ON d.location_id = s.location_id
            WHERE s.borough IS NOT NULL
            GROUP BY d.date_local)
        SELECT date_local, n_stations, n_usg, ROUND(n_usg / n_stations, 3) AS share_usg
        FROM per_day WHERE n_usg > 0 AND n_usg >= 0.5 * n_stations ORDER BY date_local""",
}


# ------------------------------------------------------------------ KMeans (ham thuan)

def train_and_evaluate_kmeans(df_features: DataFrame, feature_cols, k_range=range(2, 8), fixed_k=None):
    """Thu tung k, tra dict: best_k, model (PipelineModel), scores, centers (thang goc)."""
    n = df_features.count()
    ks = [k for k in k_range if k < n]
    if not ks:
        raise ValueError(f"Khong du du lieu cho KMeans: {n} dong")
    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette")
    scores, models = [], {}
    for k in ks:
        pipeline = Pipeline(stages=[
            VectorAssembler(inputCols=list(feature_cols), outputCol="raw_features"),
            StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
            KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=42),
        ])
        model = pipeline.fit(df_features)
        score = evaluator.evaluate(model.transform(df_features))
        scores.append({"k": k, "silhouette": score})
        models[k] = model
        logger.info("k=%d silhouette=%.4f", k, score)
    best_k = fixed_k if fixed_k in models else max(scores, key=lambda s: s["silhouette"])["k"]
    model = models[best_k]
    scaler, kmeans = model.stages[1], model.stages[2]
    mean, std = scaler.mean.toArray(), scaler.std.toArray()
    centers = [[float(c[i] * std[i] + mean[i]) for i in range(len(c))] for c in kmeans.clusterCenters()]
    return {"best_k": best_k, "model": model, "scores": scores, "centers": centers}


def name_clusters(centers, feature_cols, names_cfg):
    """Gan ten theo thu hang aqi_mean cua tam cum. Tra {cluster: (name, description)}."""
    idx = list(feature_cols).index("aqi_mean")
    order = sorted(range(len(centers)), key=lambda c: centers[c][idx])
    names = (names_cfg.get("names_by_k") or {}).get(len(centers), [])
    out = {}
    for rank, cluster in enumerate(order):
        entry = names[rank] if rank < len(names) else {}
        out[cluster] = (entry.get("name", f"Cum {cluster}"), entry.get("description", ""))
    return out


# ------------------------------------------------------------------ bang analytics

def build_region_stats(df_summary: DataFrame) -> DataFrame:
    month = F.month("date_local")
    season = (F.when(month.isin(12, 1, 2), "winter").when(month.isin(3, 4, 5), "spring")
              .when(month.isin(6, 7, 8), "summer").otherwise("autumn"))
    base = (df_summary.filter(F.col("borough").isNotNull())
            .withColumn("p_year", F.date_format("date_local", "yyyy"))
            .withColumn("p_month", F.date_format("date_local", "yyyy-MM"))
            .withColumn("p_season", F.concat_ws("-", F.date_format("date_local", "yyyy"), season)))
    out = None
    for ptype in ("year", "season", "month"):
        b = base.withColumn("period_type", F.lit(ptype)).withColumn("period", F.col(f"p_{ptype}"))
        keys = ["borough", "period_type", "period"]
        stats = b.groupBy(*keys).agg(
            F.count("*").alias("n_station_days"),
            F.avg("aqi_daily").alias("aqi_avg"),
            F.max("aqi_daily").alias("aqi_max"),
            F.countDistinct(F.when(F.col("aqi_daily") > 100, F.col("date_local"))).alias("days_over_100"))
        dist = (b.filter(F.col("cluster_name").isNotNull())
                .groupBy(*keys, "cluster_name").count()
                .withColumn("share", F.round(F.col("count") / F.sum("count").over(Window.partitionBy(*keys)), 4))
                .groupBy(*keys)
                .agg(F.to_json(F.map_from_entries(F.collect_list(F.struct("cluster_name", "share"))))
                     .alias("cluster_distribution")))
        part = stats.join(dist, keys, "left")
        out = part if out is None else out.unionByName(part)
    return out.select("borough", "period_type", "period", F.col("n_station_days").cast("int"), "aqi_avg",
                      F.col("aqi_max").cast("int"), F.col("days_over_100").cast("int"), "cluster_distribution")


def next_model_version(spark) -> str:
    jpath = spark._jvm.org.apache.hadoop.fs.Path(MODELS_PATH)
    fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    versions = [0]
    if fs.exists(jpath):
        for st in fs.listStatus(jpath):
            m = re.fullmatch(r"v(\d+)", st.getPath().getName())
            if m:
                versions.append(int(m.group(1)))
    return f"{MODELS_PATH}/v{max(versions) + 1}/"


def write_pg(df: DataFrame, table: str):
    """Ghi de noi dung nhung giu nguyen DDL (TRUNCATE thay vi DROP)."""
    (df.write.format("jdbc")
     .option("url", os.getenv("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/aq"))
     .option("dbtable", table)
     .option("user", os.getenv("PG_USER", "aq_user"))
     .option("password", os.getenv("PG_PASSWORD", "aq_password"))
     .option("driver", "org.postgresql.Driver")
     .option("truncate", "true")
     .mode("overwrite").save())
    logger.info("Da ghi %s", table)


def run_sql(spark):
    sql_dir, plan_dir = REPORTS_DIR / "sql", REPORTS_DIR / "plans"
    sql_dir.mkdir(parents=True, exist_ok=True)
    plan_dir.mkdir(parents=True, exist_ok=True)
    for name, query in SQL.items():
        plan = spark.sql("EXPLAIN FORMATTED " + query).collect()[0][0]
        (plan_dir / f"{name}.txt").write_text(plan, encoding="utf-8")
        spark.sql(query).toPandas().to_csv(sql_dir / f"{name}.csv", index=False)
        logger.info("SQL %s -> reports/sql/%s.csv", name, name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--k", type=int, help="Ep chon k (sau khi xem silhouette + dien giai)")
    args = parser.parse_args()
    spark = create_spark_session("AQ_Analytics_ML")

    df_daily = spark.read.parquet(f"{GOLD_PATH}fact_daily_aqi")
    df_hourly = spark.read.parquet(f"{GOLD_PATH}fact_hourly")
    df_dim = spark.read.parquet(f"{GOLD_PATH}dim_station").cache()
    df_feats = spark.read.parquet(f"{GOLD_PATH}cluster_features").dropna(subset=FEATURES).cache()
    for name, df in (("fact_daily_aqi", df_daily), ("fact_hourly", df_hourly), ("dim_station", df_dim)):
        df.createOrReplaceTempView(name)

    run_sql(spark)

    # KMeans
    ml_dir = REPORTS_DIR / "ml"
    ml_dir.mkdir(parents=True, exist_ok=True)
    names_cfg = load_yaml("cluster_names.yaml")
    try:
        res = train_and_evaluate_kmeans(df_feats, FEATURES, fixed_k=args.k)
    except ValueError as e:
        logger.warning("Bo qua KMeans: %s", e)
        res = None

    if res:
        with open(ml_dir / "k_selection.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["k", "silhouette", "chosen"])
            w.writeheader()
            for s in res["scores"]:
                w.writerow({**s, "chosen": s["k"] == res["best_k"]})
        names = name_clusters(res["centers"], FEATURES, names_cfg)
        preds = res["model"].transform(df_feats).select("location_id", "date_local", "cluster").cache()
        counts = {r["cluster"]: r["count"] for r in preds.groupBy("cluster").count().collect()}
        total = sum(counts.values())
        profiles = [(c, names[c][0], names[c][1], counts.get(c, 0) / total, counts.get(c, 0),
                     *res["centers"][c]) for c in range(res["best_k"])]
        df_profiles = spark.createDataFrame(profiles, "cluster int, cluster_name string, description string, "
                                            "share_of_days double, n_days int, " +
                                            ", ".join(f"{f} double" for f in FEATURES))
        df_profiles.toPandas().to_csv(ml_dir / "cluster_centers.csv", index=False)
        model_path = next_model_version(spark)
        res["model"].write().overwrite().save(model_path)
        (ml_dir / "model_version.json").write_text(json.dumps({"path": model_path, "k": res["best_k"]}))
        logger.info("KMeans k=%d luu tai %s", res["best_k"], model_path)
        df_names = spark.createDataFrame([(c, n[0]) for c, n in names.items()], "cluster int, cluster_name string")
        preds = preds.join(F.broadcast(df_names), "cluster", "left")
    else:
        preds = spark.createDataFrame([], "location_id int, date_local date, cluster int, cluster_name string")
        df_profiles = None

    df_summary = (df_daily.join(preds, ["location_id", "date_local"], "left")
                  .join(F.broadcast(df_dim.select("location_id", "borough")), "location_id", "left")
                  .select("location_id", "date_local", "borough", "aqi_daily", "dominant_pollutant", "level",
                          "cluster", "cluster_name")).cache()

    write_pg(df_summary, "analytics.daily_summary")
    write_pg(build_region_stats(df_summary), "analytics.region_stats")
    if df_profiles is not None:
        write_pg(df_profiles, "analytics.cluster_profiles")
    logger.info("Hoan tat analytics: %d station-days", df_summary.count())


if __name__ == "__main__":
    main()
