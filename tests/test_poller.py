import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from services.poller.parser import parse_measurements, parse_sensor_metadata

NOW = "2026-09-26T14:07:12Z"


def latest_item(**over):
    item = {
        "datetime": {"utc": "2026-09-26T14:00:00Z", "local": "2026-09-26T10:00:00-04:00"},
        "value": 20.0,
        "coordinates": {"latitude": 40.73, "longitude": -73.82},
        "sensorsId": 3916,
        "locationsId": 2178,
    }
    item.update(over)
    return item


def test_valid_latest_item():
    ok, dlq = parse_measurements([latest_item()], 2178, 40.73, -73.82, "/v3/locations/2178/latest", now=NOW)
    assert dlq == []
    m = ok[0]
    assert (m.sensor_id, m.location_id, m.value) == (3916, 2178, 20.0)
    assert m.datetime_utc == "2026-09-26T14:00:00Z"
    assert m.datetime_local == "2026-09-26T10:00:00-04:00"
    assert m.ingested_at == NOW and m.source == "openaq-api"


def test_negative_value_is_not_filtered_by_poller():
    ok, dlq = parse_measurements([latest_item(value=-3.0)], 2178, None, None, "/x", now=NOW)
    assert len(ok) == 1 and ok[0].value == -3.0


def test_invalid_items_go_to_dlq_with_error_code():
    items = [
        latest_item(value="invalid"),
        latest_item(sensorsId=None),
        latest_item(datetime={}),
        latest_item(datetime={"utc": "not-a-date"}),
        "garbage",
    ]
    ok, dlq = parse_measurements(items, 2178, None, None, "/v3/locations/2178/latest", now=NOW)
    assert ok == []
    assert [d.error for d in dlq] == [
        "value_not_numeric", "missing_sensor_id", "missing_datetime_utc",
        "invalid_datetime_utc", "not_an_object"]
    assert dlq[0].endpoint == "/v3/locations/2178/latest"
    assert '"invalid"' in dlq[0].raw_payload


def test_sensor_metadata_flattened():
    loc = {
        "id": 2178, "name": "Queens College",
        "coordinates": {"latitude": 40.73, "longitude": -73.82},
        "sensors": [
            {"id": 3916, "parameter": {"name": "pm25", "units": "µg/m³"}},
            {"id": 3917, "parameter": {"name": "o3", "units": "ppm"}},
            {"id": None, "parameter": {}},
        ],
    }
    sensors = parse_sensor_metadata(loc, now=NOW)
    assert [(s.sensor_id, s.parameter) for s in sensors] == [(3916, "pm25"), (3917, "o3")]
    assert sensors[0].location_name == "Queens College" and sensors[0].updated_at == NOW
