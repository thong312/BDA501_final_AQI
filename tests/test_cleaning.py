import json
from datetime import date

import pytest

pytest.importorskip("pyspark")
from jobs.batch.clean_bronze_to_silver import parse_bronze, prepare_metadata, transform_silver  # noqa: E402

BBOX = "-74.26,40.49,-73.70,40.92"


def bronze(spark, records):
    rows = [(json.dumps(r), i) for i, r in enumerate(records)]
    return spark.createDataFrame(rows, "value string, offset long")


def rec(**over):
    r = {"sensor_id": 1, "location_id": 101, "value": 20.0, "datetime_utc": "2026-09-27T02:00:00Z",
         "lat": 40.73, "lon": -73.82, "ingested_at": "2026-09-27T02:05:00Z", "source": "openaq-api"}
    r.update(over)
    return r


def test_transform_silver(spark):
    df = parse_bronze(bronze(spark, [
        rec(),
        rec(value=25.0, ingested_at="2026-09-27T02:10:00Z"),        # trung, ban moi hon -> giu
        rec(value=-5.0, datetime_utc="2026-09-27T03:00:00Z"),       # NEGATIVE
        rec(value=5000.0, datetime_utc="2026-09-27T01:00:00Z"),     # OUT_OF_RANGE
        rec(sensor_id=2, datetime_utc="2026-09-27T01:00:00Z"),      # NO_METADATA
        rec(datetime_utc="2026-09-26T12:00:00Z", ingested_at="2026-09-26T18:00:00Z"),  # STALE: giu
        rec(datetime_utc="2026-09-27T05:00:00Z"),                   # 01:00 ngay 27 NY -> ngoai ngay
        rec(lat=34.05, lon=-118.24, datetime_utc="2026-09-26T20:00:00Z"),  # ngoai bbox
    ]))
    meta = prepare_metadata(spark.createDataFrame(
        [(1, "pm25", "µg/m³", "Queens College", 40.73, -73.82)],
        "sensor_id int, parameter string, units string, location_name string, lat double, lon double"))
    rows = transform_silver(df, meta, BBOX, date(2026, 9, 26), date(2026, 9, 26)) \
        .orderBy("event_time").collect()

    assert [(r["value"], r["quality_flag"]) for r in rows] == [(20.0, "STALE"), (25.0, "OK")]
    ok = rows[1]
    assert str(ok["date_local"]) == "2026-09-26" and ok["hour_local"] == 22  # gio New York
    assert ok["parameter"] == "pm25" and ok["location_name"] == "Queens College"
    assert ok["borough"] == "Queens"
    assert (ok["year"], ok["month"]) == (2026, 9)
