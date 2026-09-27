import pytest
from pyspark.sql import SparkSession
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from jobs.batch.clean_bronze_to_silver import transform_silver

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder.master("local[1]").appName("pytest-spark").getOrCreate())

def test_transform_silver(spark):
    bronze_data = [
        # Bản ghi chuẩn
        ('{"sensor_id": 1, "value": 20.0, "datetime_utc": "2026-09-26T14:00:00Z", "datetime_local": "2026-09-26T10:00:00-04:00", "lat": 40.7, "lon": -73.9, "ingested_at": "2026-09-26T14:05:00Z"}',),
        # Bản ghi trùng (sẽ bị dedupe bỏ, giữ dòng ingested_at mới nhất)
        ('{"sensor_id": 1, "value": 25.0, "datetime_utc": "2026-09-26T14:00:00Z", "datetime_local": "2026-09-26T10:00:00-04:00", "lat": 40.7, "lon": -73.9, "ingested_at": "2026-09-26T14:10:00Z"}',),
        # Bản ghi lỗi âm
        ('{"sensor_id": 1, "value": -5.0, "datetime_utc": "2026-09-26T15:00:00Z", "datetime_local": "2026-09-26T11:00:00-04:00", "lat": 40.7, "lon": -73.9, "ingested_at": "2026-09-26T15:05:00Z"}',)
    ]
    df_bronze = spark.createDataFrame(bronze_data, ["value"])
    
    meta_data = [(1, "pm25", "µg/m³", "Test Station")]
    df_meta = spark.createDataFrame(meta_data, ["sensor_id", "meta_parameter", "meta_units", "meta_location_name"])
    
    rules = {"pm25": {"min": 0.0, "max": 1000.0}}
    bbox = "-74.26,40.49,-73.70,40.92"
    
    df_final, df_parsed, df_ok = transform_silver(df_bronze, df_meta, rules, bbox)
    
    results = df_final.collect()
    
    # Assert
    assert len(results) == 1 # 1 trùng bị loại, 1 lỗi âm bị loại
    row = results[0]
    
    assert row["value"] == 25.0 # Lấy bản update ingested_at mới hơn
    assert row["parameter"] == "pm25" # Lấy từ join metadata
    assert row["quality_flag"] == "OK"
    assert "borough" in df_final.columns
