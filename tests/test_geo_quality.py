import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.geo import get_borough, in_bbox
from common.quality import quality_flag, threshold


def test_borough_assignment():
    assert get_borough(40.73, -73.82) == "Queens"
    assert get_borough(40.78, -73.97) == "Manhattan"
    assert get_borough(40.84, -73.88) == "Bronx"
    assert get_borough(40.65, -73.95) == "Brooklyn"
    assert get_borough(40.58, -74.15) == "Staten Island"
    assert get_borough(40.74, -74.17) is None  # Newark, NJ: trong bbox nhưng ngoài NYC


def test_bbox():
    assert in_bbox(40.73, -73.82)
    assert not in_bbox(34.05, -118.24)


def test_quality_flags():
    now = datetime(2026, 9, 26, 14, tzinfo=timezone.utc)
    assert quality_flag(None, "ppm", 1.0) == "NO_METADATA"
    assert quality_flag("pm25", "µg/m³", -1) == "NEGATIVE"
    assert quality_flag("pm25", "µg/m³", 5000) == "OUT_OF_RANGE"
    assert quality_flag("o3", "ppm", 0.05, now - timedelta(hours=4), now) == "STALE"
    assert quality_flag("o3", "ppm", 0.05, now - timedelta(hours=1), now) == "OK"
    assert quality_flag("bc", "µg/m³", 3.0) == "OK"  # không có luật -> chỉ kiểm tra âm


def test_threshold():
    assert threshold("pm25", "µg/m³") == 35.4
    assert threshold("bc", "µg/m³") is None
