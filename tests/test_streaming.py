import pytest
from pyspark.sql import SparkSession
from datetime import datetime
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from jobs.streaming.aq_streaming import build_query_a_df

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local[1]")
            .appName("pytest-spark")
            .getOrCreate())

def test_build_query_a_df(spark):
    # Dữ liệu giả lập đầu vào từ Kafka (cột value là binary)
    data = [
        (b'{"sensor_id": 1, "value": 20.0}', "aq.openaq.measurements.v1", 0, 100, datetime(2026, 9, 26, 14, 0, 0))
    ]
    schema = ["value", "topic", "partition", "offset", "timestamp"]
    df = spark.createDataFrame(data, schema)
    
    # Thực thi hàm thuần
    result_df = build_query_a_df(df)
    result = result_df.collect()[0]
    
    # Kiểm tra cast kiểu và logic tạo cột mới
    assert isinstance(result["value"], str)
    assert '{"sensor_id": 1' in result["value"]
    assert result["topic"] == "aq.openaq.measurements.v1"
    assert result["partition"] == 0
    assert result["offset"] == 100
    assert "ingest_date" in result_df.columns
    assert str(result["ingest_date"]) == "2026-09-26"
