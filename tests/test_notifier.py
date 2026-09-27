import pytest
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from services.notifier.main import format_alert_message

def test_format_alert_message():
    alert = {
        "alert_id": "Queens_2026092614_UNHEALTHY",
        "scope": "region",
        "borough": "Queens",
        "trigger_location_id": 2178,
        "aqi": 156,
        "level": "UNHEALTHY",
        "prev_level": "USG",
        "type": "ESCALATE",
        "dominant_pollutant": "pm25",
        "event_time": "2026-09-26T14:00:00Z",
        "created_at": "2026-09-26T14:08:30Z"
    }
    msg = format_alert_message(alert)
    
    assert "🚨" in msg
    assert "Queens" in msg
    assert "156" in msg
    assert "UNHEALTHY" in msg
    assert "ESCALATE" in msg
    assert "pm25" in msg
