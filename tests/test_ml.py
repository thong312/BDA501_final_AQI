import pytest
from pyspark.sql import SparkSession
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from jobs.batch.analytics import train_and_evaluate_kmeans

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder.master("local[1]").appName("pytest-spark").getOrCreate())

def test_kmeans_training(spark):
    data = [
        (101, "2026-09-26", 45.0, 60.0, 0),
        (101, "2026-09-27", 150.0, 200.0, 10),
        (102, "2026-09-26", 30.0, 45.0, 0),
        (102, "2026-09-27", 160.0, 210.0, 12)
    ]
    schema = ["location_id", "date_local", "aqi_mean", "aqi_max", "hours_over_100"]
    df = spark.createDataFrame(data, schema)
    
    feature_cols = ["aqi_mean", "aqi_max", "hours_over_100"]
    
    # Chỉ test K=2 để Unit test chạy nhanh
    best_k, best_model, scaler, assem, scores, centers = train_and_evaluate_kmeans(df, feature_cols, k_range=[2])
    
    assert best_k == 2
    assert len(centers) == 2
    assert len(scores) == 1
    
    # Đảm bảo Silhouette score được trả về (dù K bé dataset ít có thể ra điểm kém hoặc bằng 1)
    assert "silhouette" in scores[0]
