import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.mapreduce.daily_stats import map_record, reduce_values

BBOX = "-74.26,40.49,-73.70,40.92"
LOOKUP = {1: {"sensor_id": 1, "parameter": "pm25", "units": "µg/m³"}}


def bronze_line(offset=0, **data):
    record = {"sensor_id": 1, "location_id": 101, "lat": 40.7, "lon": -73.9, "value": 40.0,
              "datetime_utc": "2026-09-27T02:00:00Z", "ingested_at": "2026-09-27T02:05:00Z"}
    record.update(data)
    return json.dumps({"value": json.dumps(record), "offset": offset})


def test_mapper_uses_new_york_date_and_lookup():
    key, value = map_record(bronze_line(), LOOKUP, BBOX)
    # 02:00Z ngày 27 = 22:00 ngày 26 ở New York (EDT)
    assert key == [101, "pm25", "2026-09-26"]
    assert value == [1, "2026-09-27T02:00:00Z", "2026-09-27T02:05:00Z", 0, 40.0, "µg/m³"]


def test_mapper_filters_same_rules_as_spark():
    assert map_record(bronze_line(value=-1.0), LOOKUP, BBOX) is None              # NEGATIVE
    assert map_record(bronze_line(value=5000.0), LOOKUP, BBOX) is None            # OUT_OF_RANGE
    assert map_record(bronze_line(value="20"), LOOKUP, BBOX) is None              # không phải số
    assert map_record(bronze_line(sensor_id=2), LOOKUP, BBOX) is None             # NO_METADATA
    assert map_record(bronze_line(lat=34.05, lon=-118.2), LOOKUP, BBOX) is None   # ngoài bbox
    assert map_record(bronze_line(), LOOKUP, BBOX, target_date="2026-09-27") is None
    assert map_record("not json", LOOKUP, BBOX) is None


def test_backfill_record_has_own_parameter():
    key, _ = map_record(bronze_line(sensor_id=9, parameter="o3", units="ppm", value=0.05), {}, BBOX)
    assert key[1] == "o3"


def test_reducer_dedupes_keeping_latest_ingest():
    key = [101, "pm25", "2026-09-26"]
    vals = [
        [1, "2026-09-27T02:00:00Z", "2026-09-27T02:05:00Z", 5, 40.0, "µg/m³"],
        [1, "2026-09-27T02:00:00Z", "2026-09-27T02:15:00Z", 9, 42.0, "µg/m³"],  # bản mới hơn
        [1, "2026-09-27T03:00:00Z", "2026-09-27T03:05:00Z", 7, 10.0, "µg/m³"],
    ]
    stats = reduce_values(key, vals)
    assert stats == {"count": 2, "sum": 52.0, "max": 42.0, "avg": 26.0, "hours_over_threshold": 1}
