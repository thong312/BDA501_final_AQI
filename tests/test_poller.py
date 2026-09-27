import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from services.poller.parser import parse_measurements

def test_parse_measurements_valid():
    raw_results = [{
        "sensorsId": 123,
        "value": 15.5,
        "period": {"datetimeTo": {"utc": "2026-09-26T14:00:00Z"}}
    }]
    
    ok, dlq = parse_measurements(raw_results, location_id=1, lat=40.7, lon=-73.9, endpoint="/test")
    assert len(ok) == 1
    assert len(dlq) == 0
    assert ok[0].sensor_id == 123
    assert ok[0].value == 15.5
    assert ok[0].datetime_utc == "2026-09-26T14:00:00Z"

def test_parse_measurements_invalid():
    raw_results = [{
        "sensorsId": "not_an_int",
        "value": "invalid_value",
        "period": {}
    }]
    
    ok, dlq = parse_measurements(raw_results, location_id=1, lat=40.7, lon=-73.9, endpoint="/test")
    assert len(ok) == 0
    assert len(dlq) == 1
    assert "parse_error" in dlq[0].error.lower()
