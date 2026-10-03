from datetime import date, datetime, timedelta

import pytest

pytest.importorskip("pyspark")
from jobs.batch.aggregate_silver_to_gold import (build_cluster_features, build_dim_station,  # noqa: E402
                                                  build_fact_daily_aqi, build_fact_hourly,
                                                  build_mart_region_daily)

SCHEMA = ("location_id int, location_name string, lat double, lon double, borough string, "
          "parameter string, units string, value double, event_time timestamp")


def silver(spark, hours=20):
    # 2026-09-26 04:00Z = 00:00 New York; mỗi giờ PM2.5 = 20.0 và O3 = 0.060 ppm
    start = datetime(2026, 9, 26, 4)
    rows = []
    for h in range(hours):
        t = start + timedelta(hours=h)
        rows.append((101, "A", 40.73, -73.82, "Queens", "pm25", "µg/m³", 20.0, t))
        rows.append((101, "A", 40.73, -73.82, "Queens", "o3", "ppm", 0.060, t))
    rows.append((101, "A", 40.73, -73.82, "Queens", "pm25", "µg/m³", 22.0, start + timedelta(minutes=30)))
    return spark.createDataFrame(rows, SCHEMA)


def test_fact_hourly(spark):
    hourly = build_fact_hourly(silver(spark)).filter("parameter = 'pm25'").orderBy("hour_utc").collect()
    assert len(hourly) == 20
    assert hourly[0]["value_avg"] == 21.0 and hourly[0]["n_readings"] == 2
    assert str(hourly[0]["date_local"]) == "2026-09-26" and hourly[0]["hour_local"] == 0


def test_fact_daily_aqi_epa(spark):
    daily = build_fact_daily_aqi(build_fact_hourly(silver(spark))).collect()
    assert len(daily) == 1
    d = daily[0]
    # PM2.5 trung bình 24h ≈ 20.05 -> 71; O3 max trung bình 8h = 0.060 -> 67
    assert d["aqi_daily"] == 71 and d["dominant_pollutant"] == "pm25" and d["level"] == "MODERATE"


def test_pm25_needs_18_hours(spark):
    daily = build_fact_daily_aqi(build_fact_hourly(silver(spark, hours=10))).collect()
    assert daily[0]["dominant_pollutant"] == "o3"  # PM2.5 không đủ giờ nên không tính


def test_dim_region_and_cluster_features(spark):
    df = silver(spark)
    dim = build_dim_station(df)
    d = dim.collect()[0]
    assert d["parameters"] == ["o3", "pm25"] and d["borough"] == "Queens"
    hourly = build_fact_hourly(df)
    region = build_mart_region_daily(build_fact_daily_aqi(hourly), dim).collect()[0]
    assert region["n_stations"] == 1 and region["aqi_max"] == 71
    feats = build_cluster_features(hourly).collect()[0]
    assert feats["n_hours"] == 20 and feats["hours_over_100"] == 0
    assert 0.0 < feats["pm25_o3_ratio"] < 1.0
    assert feats["date_local"] == date(2026, 9, 26)
