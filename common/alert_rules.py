"""Bo luat canh bao (ARCHITECTURE muc 8) — ham thuan, khong phu thuoc Spark.

Dung cho ca cap tram va cap vung. State la dict voi cac khoa:
    alerted_level    muc da bao gan nhat (0..5, -1 = UNKNOWN)
    candidate_level  muc dang cho xac nhan (None neu khong co)
    candidate_count  so lan do lien tiep thoa candidate
    last_event_time  datetime cua ban do moi nhat da xet
    last_alert_at    event_time cua alert gan nhat
    last_alert_level muc cua alert gan nhat (phuc vu cooldown)

Alert tra ve la dict: type, level, prev_level, aqi, event_time (datetime).
Caller bo sung alert_id, scope, borough, trigger_location_id, dominant_pollutant.
"""
from datetime import datetime, timezone
from typing import Optional, Tuple

from dateutil import parser as dtparser

from common.aqi import LEVEL_LOWER_AQI, UNKNOWN_LEVEL, get_level, get_level_name
from common.config import load_yaml

CONFIG = load_yaml("alert_rules.yaml")


def to_datetime(value) -> Optional[datetime]:
    if value is None:
        return None
    dt = value if isinstance(value, datetime) else dtparser.isoparse(str(value))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _alert(alert_type, level, prev_level, aqi, event_time):
    return {
        "type": alert_type,
        "level": get_level_name(level),
        "prev_level": get_level_name(prev_level),
        "aqi": aqi,
        "event_time": event_time,
    }


def _in_cooldown(state, level, event_time, cfg) -> bool:
    """Luat 6: khong phat lai cung muc trong COOLDOWN_SEC."""
    last_at = to_datetime(state.get("last_alert_at"))
    if last_at is None or state.get("last_alert_level") != level:
        return False
    return (event_time - last_at).total_seconds() < cfg["COOLDOWN_SEC"]


def _emit(state, alert_type, level, prev_level, aqi, event_time, cfg):
    """Tao alert (neu khong bi cooldown chan) va ghi nhan vao state."""
    if _in_cooldown(state, level, event_time, cfg):
        return None
    state["last_alert_at"] = event_time
    state["last_alert_level"] = level
    return _alert(alert_type, level, prev_level, aqi, event_time)


def _reset_candidate(state):
    state["candidate_level"] = None
    state["candidate_count"] = 0


def decide(state: dict, aqi, event_time, level: Optional[int] = None,
           cfg: Optional[dict] = None) -> Tuple[dict, Optional[dict]]:
    """Ap luat 1–6 cho mot quan sat moi.

    aqi:   AQI quan sat (cap tram: station_aqi).
    level: muc quan sat neu da biet truoc (cap vung: max alerted_level cac tram).
           Khi truyen level, muc do da qua hysteresis o cap tram nen luat 5 chi con
           yeu cau xac nhan CONFIRM_N lan.
    """
    cfg = cfg or CONFIG
    if aqi is None and level is None:
        return state, None
    ev = to_datetime(event_time)

    # Luat 1: chi xet du lieu moi hon ban do cuoi
    last = to_datetime(state.get("last_event_time"))
    if last is not None and ev <= last:
        return state, None

    s = dict(state)
    s["last_event_time"] = ev
    s.setdefault("alerted_level", 0)
    s.setdefault("candidate_level", None)
    s.setdefault("candidate_count", 0)
    if s["alerted_level"] is None:
        s["alerted_level"] = 0
    if s["candidate_count"] is None:
        s["candidate_count"] = 0

    obs = level if level is not None else get_level(aqi)
    if obs == UNKNOWN_LEVEL:
        return s, None
    cur = s["alerted_level"]
    was_unknown = cur == UNKNOWN_LEVEL
    base = 0 if was_unknown else cur
    alert_min, urgent_min, confirm_n = cfg["ALERT_MIN_LEVEL"], cfg["URGENT_MIN_LEVEL"], cfg["CONFIRM_N"]

    # Co du lieu tro lai sau UNKNOWN ma muc duoi nguong: cap nhat lang le (luat 7: khong RECOVERED)
    if was_unknown and obs < alert_min:
        s["alerted_level"] = obs
        _reset_candidate(s)
        return s, None

    if obs > base or (was_unknown and obs >= alert_min):
        if obs < alert_min:
            # Luat 2: Good -> Moderate chi cap nhat state
            s["alerted_level"] = obs
            _reset_candidate(s)
            return s, None
        if obs >= urgent_min:
            # Luat 4: >= Unhealthy bao ngay, nhay nhieu muc bao muc cao nhat
            s["alerted_level"] = obs
            _reset_candidate(s)
            return s, _emit(s, "ESCALATE", obs, cur, aqi, ev, cfg)
        # Luat 3: len USG can CONFIRM_N lan do lien tiep
        if s["candidate_level"] == obs:
            s["candidate_count"] += 1
        else:
            s["candidate_level"], s["candidate_count"] = obs, 1
        if s["candidate_count"] >= confirm_n:
            s["alerted_level"] = obs
            _reset_candidate(s)
            return s, _emit(s, "ESCALATE", obs, cur, aqi, ev, cfg)
        return s, None

    if obs < cur:
        if cur < alert_min:
            # Moderate -> Good: chi cap nhat state
            s["alerted_level"] = obs
            _reset_candidate(s)
            return s, None
        # Luat 5: hysteresis — AQI phai <= nguong duoi cua muc hien tai − HYST
        passed = True if level is not None else aqi <= LEVEL_LOWER_AQI[cur] - cfg["HYST"]
        if not passed:
            _reset_candidate(s)
            return s, None
        cand = s["candidate_level"]
        if cand is not None and cand < cur:
            # Chuoi giam lien tiep: ha ve muc cao nhat da thay trong chuoi (than trong)
            s["candidate_level"] = max(cand, obs)
            s["candidate_count"] += 1
        else:
            s["candidate_level"], s["candidate_count"] = obs, 1
        if s["candidate_count"] >= confirm_n:
            new_level = s["candidate_level"]
            s["alerted_level"] = new_level
            _reset_candidate(s)
            if new_level < alert_min:
                return s, _emit(s, "RECOVERED", new_level, cur, aqi, ev, cfg)
            # Giam giua cac muc >= USG: chi cap nhat state
        return s, None

    # Cung muc voi muc da bao: chuoi candidate bi ngat
    _reset_candidate(s)
    return s, None


def check_timeout(state: dict, now, cfg: Optional[dict] = None) -> Tuple[dict, Optional[dict]]:
    """Luat 7: dang >= USG ma qua STALE_TIMEOUT_SEC khong co du lieu -> UNKNOWN, khong RECOVERED.

    event_time cua alert = last_event_time de alert_id tat dinh khi replay.
    """
    cfg = cfg or CONFIG
    cur = state.get("alerted_level")
    last = to_datetime(state.get("last_event_time"))
    if cur is None or cur < cfg["ALERT_MIN_LEVEL"] or last is None:
        return state, None
    if (to_datetime(now) - last).total_seconds() <= cfg["STALE_TIMEOUT_SEC"]:
        return state, None
    s = dict(state)
    s["alerted_level"] = UNKNOWN_LEVEL
    _reset_candidate(s)
    s["last_alert_at"] = last
    s["last_alert_level"] = UNKNOWN_LEVEL
    return s, _alert("UNKNOWN", UNKNOWN_LEVEL, cur, None, last)
