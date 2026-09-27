import pytest
from pyspark.sql import SparkSession
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from jobs.batch.backfill_archive import transform_backfill

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder.master("local[1]").appName("pytest-spark").getOrCreate())

def test_transform_backfill(spark):
    # Dữ liệu giả lập 1 trạm trong NYC và 1 trạm ngoài NYC
    data = [
        # Trạm trong NYC
        (1, 101, "Queens College", "2026-09-26T14:00:00Z", 40.73, -73.82, "pm25", "µg/m³", 20.5),
        # Trạm ngoài NYC (ví dụ Cali)
        (2, 102, "California", "2026-09-26T14:00:00Z", 34.05, -118.24, "pm25", "µg/m³", 10.0)
    ]
    schema = ["location_id", "sensors_id", "location", "datetime", "lat", "lon", "parameter", "units", "value"]
    df = spark.createDataFrame(data, schema)
    
    nyc_bbox = "-74.26,40.49,-73.70,40.92"
    result_df = transform_backfill(df, nyc_bbox)
    
    results = result_df.collect()
    
    # Assert
    assert len(results) == 1 # Chỉ còn trạm NYC
    
    row = results[0]
    assert row["topic"] == "aq.openaq.measurements.v1"
    assert row["partition"] == 0
    assert row["offset"] == 0
    assert "2026-09-26" in str(row["ingest_date"])
    
    # Kiểm tra json string value
    val_json = row["value"]
    assert "openaq-archive" in val_json
    assert "Queens College" in val_json
    assert "pm25" in val_json
