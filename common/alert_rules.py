"""Bộ luật cảnh báo (ARCHITECTURE mục 8) — hàm thuần, không phụ thuộc Spark.

Dùng cho cả cấp trạm và cấp vùng. State là dict với các khoá:
    alerted_level    mức đã báo gần nhất (0..5, -1 = UNKNOWN)
    candidate_level  mức đang chờ xác nhận (None nếu không có)
    candidate_count  số lần đo liên tiếp thoả candidate
    last_event_time  datetime của bản đo mới nhất đã xét
    last_alert_at    event_time của alert gần nhất
    last_alert_level mức của alert gần nhất (phục vụ cooldown)

Alert trả về là dict: type, level, prev_level, aqi, event_time (datetime).
Caller bổ sung alert_id, scope, borough, trigger_location_id, dominant_pollutant.
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
    """Luật 6: không phát lại cùng mức trong COOLDOWN_SEC."""
    last_at = to_datetime(state.get("last_alert_at"))
    if last_at is None or state.get("last_alert_level") != level:
        return False
    return (event_time - last_at).total_seconds() < cfg["COOLDOWN_SEC"]


def _emit(state, alert_type, level, prev_level, aqi, event_time, cfg):
    """Tạo alert (nếu không bị cooldown chặn) và ghi nhận vào state."""
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
    """Áp luật 1–6 cho một quan sát mới.

    aqi:   AQI quan sát (cấp trạm: station_aqi).
    level: mức quan sát nếu đã biết trước (cấp vùng: max alerted_level các trạm).
           Khi truyền level, mức đó đã qua hysteresis ở cấp trạm nên luật 5 chỉ còn
           yêu cầu xác nhận CONFIRM_N lần.
    """
    cfg = cfg or CONFIG
    if aqi is None and level is None:
        return state, None
    ev = to_datetime(event_time)

    # Luật 1: chỉ xét dữ liệu mới hơn bản đo cuối
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

    # Có dữ liệu trở lại sau UNKNOWN mà mức dưới ngưỡng: cập nhật lặng lẽ (luật 7: không RECOVERED)
    if was_unknown and obs < alert_min:
        s["alerted_level"] = obs
        _reset_candidate(s)
        return s, None

    if obs > base or (was_unknown and obs >= alert_min):
        if obs < alert_min:
            # Luật 2: Good -> Moderate chỉ cập nhật state
            s["alerted_level"] = obs
            _reset_candidate(s)
            return s, None
        if obs >= urgent_min:
            # Luật 4: >= Unhealthy báo ngay, nhảy nhiều mức báo mức cao nhất
            s["alerted_level"] = obs
            _reset_candidate(s)
            return s, _emit(s, "ESCALATE", obs, cur, aqi, ev, cfg)
        # Luật 3: lên USG cần CONFIRM_N lần đo liên tiếp
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
            # Moderate -> Good: chỉ cập nhật state
            s["alerted_level"] = obs
            _reset_candidate(s)
            return s, None
        # Luật 5: hysteresis — AQI phải <= ngưỡng dưới của mức hiện tại − HYST
        passed = True if level is not None else aqi <= LEVEL_LOWER_AQI[cur] - cfg["HYST"]
        if not passed:
            _reset_candidate(s)
            return s, None
        cand = s["candidate_level"]
        if cand is not None and cand < cur:
            # Chuỗi giảm liên tiếp: hạ về mức cao nhất đã thấy trong chuỗi (thận trọng)
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
            # Giảm giữa các mức >= USG: chỉ cập nhật state
        return s, None

    # Cùng mức với mức đã báo: chuỗi candidate bị ngắt
    _reset_candidate(s)
    return s, None


def check_timeout(state: dict, now, cfg: Optional[dict] = None) -> Tuple[dict, Optional[dict]]:
    """Luật 7: đang >= USG mà quá STALE_TIMEOUT_SEC không có dữ liệu -> UNKNOWN, không RECOVERED.

    event_time của alert = last_event_time để alert_id tất định khi replay.
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
