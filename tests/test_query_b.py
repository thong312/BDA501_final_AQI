import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from jobs.streaming.aq_streaming import build_query_b_df

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder.master("local[1]").appName("pytest-spark").getOrCreate())

def test_build_query_b_df(spark):
    data = [
        (b'{"sensor_id": 1, "value": 20.0, "datetime_utc": "2026-09-26T14:00:00Z"}', "topic", 0, 100, datetime.now())
    ]
    schema = ["value", "topic", "partition", "offset", "timestamp"]
    df = spark.createDataFrame(data, schema)
    
    result_df = build_query_b_df(df)
    
    # Assert
    assert "datetime_utc_ts" in result_df.columns
    assert "sensor_id" in result_df.columns
    assert "value" in result_df.columns
