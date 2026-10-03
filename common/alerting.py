"""Phan thuan cua Streaming query B: dung ban ghi alert va quan sat cap vung."""
from datetime import datetime, timezone
from typing import Iterable, Optional

from common.alert_rules import CONFIG, to_datetime
from common.aqi import UNKNOWN_LEVEL


def region_alert_id(borough: str, event_time, level_name: str) -> str:
    """Tat dinh tu borough + gio event + level (ARCHITECTURE 5.1), vi du Queens_2026092614_UNHEALTHY."""
    ev = to_datetime(event_time).astimezone(timezone.utc)
    return f"{borough.replace(' ', '')}_{ev:%Y%m%d%H}_{level_name}"


def station_alert_id(location_id: int, event_time, level_name: str) -> str:
    ev = to_datetime(event_time).astimezone(timezone.utc)
    return f"station{location_id}_{ev:%Y%m%d%H}_{level_name}"


def build_alert_record(alert: dict, scope: str, borough: Optional[str], location_id: Optional[int],
                       dominant_pollutant: Optional[str], created_at: Optional[datetime] = None) -> dict:
    """Alert tu decide()/check_timeout() -> ban ghi theo schema aq.alerts.level-changed.v1."""
    ev = to_datetime(alert["event_time"])
    alert_id = (region_alert_id(borough, ev, alert["level"]) if scope == "region"
                else station_alert_id(location_id, ev, alert["level"]))
    return {
        "alert_id": alert_id,
        "scope": scope,
        "borough": borough,
        "trigger_location_id": location_id,
        "aqi": alert.get("aqi"),
        "level": alert["level"],
        "prev_level": alert["prev_level"],
        "type": alert["type"],
        "dominant_pollutant": dominant_pollutant,
        "event_time": ev,
        "created_at": created_at or datetime.now(timezone.utc),
    }


def alert_to_message(record: dict) -> dict:
    """Ban ghi alert -> JSON message (datetime thanh chuoi ISO UTC)."""
    msg = dict(record)
    for k in ("event_time", "created_at"):
        msg[k] = to_datetime(msg[k]).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return msg


def region_observation(stations: Iterable[dict], region_level: int, now, cfg: Optional[dict] = None):
    """Quan sat cap vung tu state cac tram cung borough.

    stations: dict co location_id, alerted_level, station_aqi, dominant_pollutant, last_event_time.
    Muc vung = max alerted_level cac tram con du lieu (khong stale, khong UNKNOWN).
    Tra ve dict (level, aqi, trigger_location_id, dominant_pollutant, event_time, blocked_down)
    hoac None neu vung khong con tram nao co du lieu.

    blocked_down = True khi co tram da mat du lieu (UNKNOWN/stale): vung khong duoc ha muc
    vi ly do mat du lieu (luat 7 — khong gui RECOVERED khi mat du lieu).
    """
    cfg = cfg or CONFIG
    now = to_datetime(now)
    fresh, lost = [], False
    for st in stations:
        last = to_datetime(st.get("last_event_time"))
        stale = last is None or (now - last).total_seconds() > cfg["STALE_TIMEOUT_SEC"]
        if st.get("alerted_level") == UNKNOWN_LEVEL or stale:
            lost = True
            continue
        fresh.append(st)
    if not fresh:
        return None
    trigger = max(fresh, key=lambda s: (s["alerted_level"], s.get("station_aqi") or -1))
    level = trigger["alerted_level"]
    return {
        "level": level,
        "aqi": trigger.get("station_aqi"),
        "trigger_location_id": trigger["location_id"],
        "dominant_pollutant": trigger.get("dominant_pollutant"),
        "event_time": max(to_datetime(s["last_event_time"]) for s in fresh),
        "blocked_down": lost and region_level is not None and level < region_level,
    }
