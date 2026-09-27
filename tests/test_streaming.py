"""Test Spark của Query A (chạy trong container: make test-spark)."""
import sys
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent))
from jobs.streaming.aq_streaming import build_query_a_df


@pytest.fixture(scope="module")
def spark():
    return (SparkSession.builder.master("local[1]").appName("test-query-a")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate())


def test_build_query_a_df(spark):
    data = [(b'{"sensor_id": 1, "value": 20.0}', "aq.openaq.measurements.v1", 0, 100, datetime(2026, 9, 26, 14))]
    df = spark.createDataFrame(data, ["value", "topic", "partition", "offset", "timestamp"])
    row = build_query_a_df(df).collect()[0]
    assert row["value"] == '{"sensor_id": 1, "value": 20.0}'
    assert (row["topic"], row["partition"], row["offset"]) == ("aq.openaq.measurements.v1", 0, 100)
    assert str(row["ingest_date"]) == "2026-09-26"
