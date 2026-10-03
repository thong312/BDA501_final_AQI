import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
pytest.importorskip("requests")
pytest.importorskip("confluent_kafka")
from services.notifier.main import SeenAlerts, format_alert_message  # noqa: E402


def alert(**over):
    a = {"alert_id": "Queens_2026092614_UNHEALTHY", "scope": "region", "borough": "Queens",
         "trigger_location_id": 2178, "aqi": 156, "level": "UNHEALTHY", "prev_level": "USG",
         "type": "ESCALATE", "dominant_pollutant": "pm25", "event_time": "2026-09-26T14:00:00Z",
         "created_at": "2026-09-26T14:08:30Z"}
    a.update(over)
    return a


def test_format_escalate():
    msg = format_alert_message(alert())
    for part in ("Queens", "156", "UNHEALTHY", "ESCALATE", "pm25"):
        assert part in msg


def test_format_unknown_without_aqi():
    msg = format_alert_message(alert(type="UNKNOWN", level="UNKNOWN", aqi=None, dominant_pollutant=None))
    assert "UNKNOWN" in msg and "None" not in msg


def test_seen_alerts_dedupes_and_is_bounded():
    seen = SeenAlerts(capacity=2)
    assert seen.check_and_add("a") and not seen.check_and_add("a")
    assert seen.check_and_add("b") and seen.check_and_add("c")
    assert seen.check_and_add("a")  # "a" da bi day ra khoi bo nho
