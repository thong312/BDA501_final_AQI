"""Test Spark của Query B (chạy trong container: make test-spark)."""
import sys
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).parent.parent))
from jobs.streaming.aq_streaming import (META_SCHEMA, aqi_instant_udf, enrich_and_flag, parse_measurements,
                                         station_aqi)



def kafka_rows(spark, payloads):
    data = [(p.encode(), "aq.openaq.measurements.v1", 0, i, datetime(2026, 9, 26, 14)) for i, p in enumerate(payloads)]
    return spark.createDataFrame(data, ["value", "topic", "partition", "offset", "timestamp"])


def msg(sensor, loc, value, ts="2026-09-26T14:00:00Z"):
    return (f'{{"sensor_id": {sensor}, "location_id": {loc}, "value": {value}, '
            f'"datetime_utc": "{ts}", "ingested_at": "2026-09-26T14:05:00Z"}}')


def test_parse_measurements_drops_unparseable(spark):
    df = parse_measurements(kafka_rows(spark, [msg(1, 10, 20.0), "not json"]))
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["event_time"] == datetime(2026, 9, 26, 14)


def test_join_flag_aqi_and_station_max(spark):
    meta = spark.createDataFrame([
        (1, 10, "A", "pm25", "µg/m³", 40.73, -73.82, "Queens"),
        (2, 10, "A", "o3", "ppm", 40.73, -73.82, "Queens"),
    ], META_SCHEMA).drop("meta_location_id")
    batch = parse_measurements(kafka_rows(spark, [
        msg(1, 10, 60.0), msg(2, 10, 0.03), msg(1, 10, -1.0, "2026-09-26T13:00:00Z"), msg(99, 10, 5.0)]))
    flagged = enrich_and_flag(batch, meta)
    flags = sorted(r["quality_flag"] for r in flagged.collect())
    # event_time 2026-09-26 cũ hơn now > 3h: hai bản đo hợp lệ bị STALE, trạm lạ -> NO_METADATA
    assert flags.count("NO_METADATA") == 1 and flags.count("NEGATIVE") == 1

    ok = (flagged.filter("quality_flag IN ('OK', 'STALE')")
          .withColumn("aqi_instant", aqi_instant_udf("parameter", "value", "units").cast("int")))
    res = station_aqi(ok).collect()
    assert len(res) == 1
    assert res[0]["station_aqi"] == 154 and res[0]["dominant_pollutant"] == "pm25"
