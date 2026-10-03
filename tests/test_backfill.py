import json
from datetime import date

import pytest

pytest.importorskip("pyspark")
from jobs.batch.backfill_archive import archive_paths, month_range, transform_backfill  # noqa: E402

COLS = ["location_id", "sensors_id", "location", "datetime", "lat", "lon", "parameter", "units", "value"]


def test_month_range_and_paths():
    assert list(month_range(date(2024, 11, 15), date(2025, 1, 2))) == [(2024, 11), (2024, 12), (2025, 1)]
    assert archive_paths([2178], date(2024, 1, 1), date(2024, 1, 31)) == [
        "s3a://openaq-data-archive/records/csv.gz/locationid=2178/year=2024/month=01/*.csv.gz"]


def test_transform_backfill(spark):
    df = spark.createDataFrame([
        ("2178", "3916", "Queens College", "2024-07-01T22:00:00-04:00", "40.73", "-73.82", "pm25", "µg/m³", "20.5"),
        ("9", "9", "Los Angeles", "2024-07-01T10:00:00-07:00", "34.05", "-118.24", "pm25", "µg/m³", "10.0"),
        ("2178", "3916", "Queens College", "2023-01-01T00:00:00-05:00", "40.73", "-73.82", "pm25", "µg/m³", "5"),
    ], COLS)
    rows = transform_backfill(df, "-74.26,40.49,-73.70,40.92", date(2024, 1, 1), date(2024, 12, 31),
                              ingested_at="2026-09-27T00:00:00Z").collect()
    assert len(rows) == 1  # ngoai bbox va ngoai khoang ngay bi loai
    row = rows[0]
    assert row["topic"] == "aq.openaq.measurements.v1"
    assert str(row["ingest_date"]) == "2024-07-02"  # ngay UTC cua ban do
    rec = json.loads(row["value"])
    assert rec["datetime_utc"] == "2024-07-02T02:00:00Z"
    assert rec["datetime_local"] == "2024-07-01T22:00:00-04:00"
    assert (rec["sensor_id"], rec["location_id"], rec["value"]) == (3916, 2178, 20.5)
    assert (rec["parameter"], rec["units"], rec["location_name"]) == ("pm25", "µg/m³", "Queens College")
    assert rec["source"] == "openaq-archive" and rec["ingested_at"] == "2026-09-27T00:00:00Z"
