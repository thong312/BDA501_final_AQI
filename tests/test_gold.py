import pytest
from pyspark.sql import SparkSession
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from jobs.batch.aggregate_silver_to_gold import build_dim_station, build_fact_hourly, build_fact_daily_aqi

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder.master("local[1]").appName("pytest-spark").getOrCreate())

def test_gold_aggregation(spark):
    data = [
        (101, "Test Station", 40.7, -73.9, "Queens", "pm25", 20.0, "µg/m³", "2026-09-26T14:00:00Z", "2026-09-26", 10),
        (101, "Test Station", 40.7, -73.9, "Queens", "pm25", 40.0, "µg/m³", "2026-09-26T15:00:00Z", "2026-09-26", 11),
        (101, "Test Station", 40.7, -73.9, "Queens", "o3", 0.05, "ppm", "2026-09-26T14:30:00Z", "2026-09-26", 10)
    ]
    schema = ["location_id", "location_name", "lat", "lon", "borough", "parameter", "value", "units", "datetime_utc", "date_local", "hour_local"]
    df_silver = spark.createDataFrame(data, schema)
    
    # Test Dim Station
    df_dim = build_dim_station(df_silver)
    res_dim = df_dim.collect()[0]
    assert "pm25" in res_dim["parameters"]
    assert "o3" in res_dim["parameters"]
    
    # Test Fact Hourly
    df_hourly = build_fact_hourly(df_silver)
    res_hourly = df_hourly.filter(df_hourly.parameter == "pm25").orderBy("hour_utc").collect()
    assert len(res_hourly) == 2
    assert res_hourly[0]["hourly_value"] == 20.0
    
    # Test Fact Daily AQI
    df_daily = build_fact_daily_aqi(df_hourly)
    res_daily = df_daily.collect()[0]
    assert res_daily["dominant_pollutant"] == "pm25" # Do nồng độ pm25 quy ra AQI cao hơn o3
    assert res_daily["level"] in ["Good", "Moderate", "USG", "Unhealthy", "Very Unhealthy", "Hazardous"]
