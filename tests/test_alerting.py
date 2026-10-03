import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.alerting import alert_to_message, build_alert_record, region_alert_id, region_observation

NOW = datetime(2026, 9, 26, 14, 30, tzinfo=timezone.utc)


def st(loc, level, aqi, minutes_ago=10, pollutant="pm25"):
    return {"location_id": loc, "alerted_level": level, "station_aqi": aqi,
            "dominant_pollutant": pollutant, "last_event_time": NOW - timedelta(minutes=minutes_ago)}


def test_region_alert_id_is_deterministic():
    assert region_alert_id("Queens", "2026-09-26T14:00:00Z", "UNHEALTHY") == "Queens_2026092614_UNHEALTHY"
    assert region_alert_id("Staten Island", "2026-09-26T14:59:59Z", "USG") == "StatenIsland_2026092614_USG"


def test_build_alert_record_matches_topic_schema():
    alert = {"type": "ESCALATE", "level": "UNHEALTHY", "prev_level": "USG", "aqi": 156,
             "event_time": "2026-09-26T14:00:00Z"}
    rec = build_alert_record(alert, "region", "Queens", 2178, "pm25",
                             created_at=datetime(2026, 9, 26, 14, 8, 30, tzinfo=timezone.utc))
    msg = alert_to_message(rec)
    assert msg == {
        "alert_id": "Queens_2026092614_UNHEALTHY", "scope": "region", "borough": "Queens",
        "trigger_location_id": 2178, "aqi": 156, "level": "UNHEALTHY", "prev_level": "USG",
        "type": "ESCALATE", "dominant_pollutant": "pm25",
        "event_time": "2026-09-26T14:00:00Z", "created_at": "2026-09-26T14:08:30Z"}


def test_region_level_is_max_station_level():
    obs = region_observation([st(1, 1, 80), st(2, 3, 160, pollutant="o3"), st(3, 3, 155)], 0, NOW)
    assert obs["level"] == 3 and obs["trigger_location_id"] == 2 and obs["dominant_pollutant"] == "o3"
    assert obs["event_time"] == NOW - timedelta(minutes=10)
    assert not obs["blocked_down"]


def test_region_ignores_stale_and_blocks_drop_caused_by_missing_data():
    stations = [st(1, 1, 80), st(2, -1, 160, minutes_ago=300)]
    obs = region_observation(stations, 3, NOW)
    assert obs["level"] == 1 and obs["blocked_down"]


def test_region_without_fresh_stations():
    assert region_observation([st(1, 2, 120, minutes_ago=400)], 2, NOW) is None
