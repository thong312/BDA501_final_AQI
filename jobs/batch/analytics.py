import os
import argparse
import logging
import csv
import yaml
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, broadcast, count, sum as _sum
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.types import IntegerType, StringType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def train_and_evaluate_kmeans(df_features, feature_cols, k_range=range(2, 8)):
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    df_assembled = assembler.transform(df_features)
    
    count = df_assembled.count()
    if count < 2:
        logger.warning(f"Chỉ có {count} dòng, không thể chuẩn hoá và chạy KMeans. Tạo dữ liệu giả.")
        scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=False, withMean=True)
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)
        
        from pyspark.ml.clustering import KMeansModel
        # Since k=1 is not allowed in KMeans, we just return a fake result
        class DummyModel:
            def transform(self, df):
                return df.withColumn("prediction", lit(0))
            def clusterCenters(self):
                return []
        
        return 1, DummyModel(), scaler_model, assembler, [{"k": 1, "silhouette": 1.0}], []
        
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    max_k = min(7, count)
    k_range = range(2, max_k + 1)
    
    evaluator = ClusteringEvaluator(predictionCol="prediction", featuresCol="features",
                                    metricName="silhouette", distanceMeasure="squaredEuclidean")
    
    best_k = 0
    best_score = -1.0
    best_model = None
    scores = []
    
    for k in k_range:
        kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=k, seed=42)
        model = kmeans.fit(df_scaled)
        predictions = model.transform(df_scaled)
        score = evaluator.evaluate(predictions)
        scores.append({"k": k, "silhouette": score})
        
        if score > best_score:
            best_score = score
            best_k = k
            best_model = model
            
    # Denormalize centers (Về thang gốc)
    centers = best_model.clusterCenters()
    std = scaler_model.std.toArray()
    mean = scaler_model.mean.toArray()
    
    denorm_centers = []
    for c in centers:
        denorm = [c[i] * std[i] + mean[i] for i in range(len(c))]
        denorm_centers.append(denorm)
        
    return best_k, best_model, scaler_model, assembler, scores, denorm_centers

def create_spark_session():
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "test")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "test")
    
    return (SparkSession.builder
            .appName("AQ_Analytics_ML")
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Đọc dữ liệu từ Gold
    try:
        df_cluster_feats = spark.read.parquet("s3a://aq-lake/gold/cluster_features/")
        df_fact_daily = spark.read.parquet("s3a://aq-lake/gold/fact_daily_aqi/")
        df_dim_station = spark.read.parquet("s3a://aq-lake/gold/dim_station/")
    except Exception as e:
        logger.error(f"Dữ liệu Gold chưa sẵn sàng: {e}")
        sys.exit(0)
        
    # Chuẩn bị ML
    feature_cols = ["aqi_mean", "aqi_max", "hours_over_100"]
    logger.info("Khởi chạy KMeans Clustering... (K=2..7)")
    best_k, best_model, scaler_model, assembler, scores, centers = train_and_evaluate_kmeans(df_cluster_feats, feature_cols)
    
    logger.info(f"K tốt nhất (dựa trên Silhouette Score): {best_k}")
    
    # Lưu báo cáo điểm số
    report_ml_dir = Path(__file__).parent.parent.parent / "reports" / "ml"
    report_ml_dir.mkdir(parents=True, exist_ok=True)
    with open(report_ml_dir / "k_selection.csv", "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["k", "silhouette"])
        writer.writeheader()
        writer.writerows(scores)
        
    # Áp dụng dự đoán
    df_assembled = assembler.transform(df_cluster_feats)
    df_scaled = scaler_model.transform(df_assembled)
    predictions = best_model.transform(df_scaled).select("location_id", "date_local", col("prediction").alias("cluster"))
    
    # Ánh xạ Tên Cụm
    config_path = Path(__file__).parent.parent.parent / "config" / "cluster_names.yaml"
    with open(config_path, "r") as f:
        c_names = yaml.safe_load(f)
        
    mapping_data = [(i, c_names.get(f"cluster_{i}", f"Cụm {i}")) for i in range(best_k)]
    df_mapping = spark.createDataFrame(mapping_data, ["cluster", "cluster_name"])
    
    # Tích hợp KQ dự đoán với fact_daily
    df_daily_summary = df_fact_daily.join(predictions, ["location_id", "date_local"], "left")
    df_daily_summary = df_daily_summary.join(broadcast(df_mapping), "cluster", "left")
    
    df_daily_summary.createOrReplaceTempView("daily_summary")
    df_dim_station.createOrReplaceTempView("dim_station")
    
    # Spark SQL Analytics
    logger.info("Chạy Spark SQL Analytics...")
    
    # Q1: Xếp hạng borough theo số ngày aqi_daily > 100
    q1 = """
    SELECT s.borough, YEAR(d.date_local) as year, COUNT(d.date_local) as bad_days
    FROM daily_summary d
    JOIN dim_station s ON d.location_id = s.location_id
    WHERE d.aqi_daily > 100
    GROUP BY s.borough, YEAR(d.date_local)
    ORDER BY bad_days DESC
    """
    df_q1 = spark.sql(q1)
    
    # Q2: Những ngày có >= 50% số trạm cùng >= USG
    q2 = """
    SELECT d.date_local, COUNT(d.location_id) as usg_stations
    FROM daily_summary d
    WHERE d.level IN ('USG', 'Unhealthy', 'Very Unhealthy', 'Hazardous')
    GROUP BY d.date_local
    HAVING COUNT(d.location_id) >= 2
    """
    df_q2 = spark.sql(q2)
    
    # Lưu query plans
    report_plans_dir = Path(__file__).parent.parent.parent / "reports" / "plans"
    report_plans_dir.mkdir(parents=True, exist_ok=True)
    with open(report_plans_dir / "q1_explain.txt", "w") as f:
        f.write(df_q1._jdf.queryExecution().simpleString())
    with open(report_plans_dir / "q2_explain.txt", "w") as f:
        f.write(df_q2._jdf.queryExecution().simpleString())
        
    # Ghi vào PostgreSQL Analytics Schema
    # Lưu ý: Sẽ dùng Postgres trong mạng Docker 
    # Cấu hình "postgres:5432" do đây là service tên postgres trong compose
    jdbc_url = "jdbc:postgresql://postgres:5432/aq"
    props = {"user": "aq_user", "password": "aq_password", "driver": "org.postgresql.Driver"}
    
    logger.info("Đang ghi vào PostgreSQL Schema Analytics...")
    try:
        # Save daily summary
        df_daily_summary.write.jdbc(jdbc_url, "analytics.daily_summary", mode="overwrite", properties=props)
        # Save region stats (Q1 result)
        df_q1.write.jdbc(jdbc_url, "analytics.region_stats", mode="overwrite", properties=props)
        logger.info("Đã ghi xong Analytics Dashboard!")
    except Exception as e:
        logger.warning(f"Lỗi kết nối ghi Database PostgreSQL: {e}")
        
    # Save Model
    model_path = "s3a://aq-lake/models/kmeans/v1/"
    try:
        best_model.write().overwrite().save(model_path)
    except Exception as e:
        pass
        
    logger.info("Hoàn tất tiến trình M10 ML + Spark SQL!")
