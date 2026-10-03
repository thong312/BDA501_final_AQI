"""Unit test bat buoc cua ARCHITECTURE muc 8."""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.alert_rules import check_timeout, decide

T0 = datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc)


def run(aqis, state=None, step_min=15):
    """Chay mot chuoi AQI, tra ve state cuoi va danh sach (aqi, alert)."""
    state = state or {}
    out = []
    for i, aqi in enumerate(aqis):
        state, alert = decide(state, aqi, T0 + timedelta(minutes=step_min * i))
        out.append(alert)
    return state, out


def types(alerts):
    return [a["type"] if a else None for a in alerts]


def test_rising_through_thresholds():
    state, alerts = run([40, 80, 120, 130, 160])
    # 120 lan 1 chua bao, 130 xac nhan USG, 160 bao ngay UNHEALTHY
    assert types(alerts) == [None, None, None, "ESCALATE", "ESCALATE"]
    assert alerts[3]["level"] == "USG" and alerts[3]["prev_level"] == "MODERATE"
    assert alerts[4]["level"] == "UNHEALTHY"
    assert state["alerted_level"] == 3


def test_oscillating_around_100_does_not_spam():
    _, alerts = run([99, 101, 99, 101, 99, 101, 99])
    assert all(a is None for a in alerts)


def test_jump_straight_to_180():
    state, alerts = run([40, 180])
    assert types(alerts) == [None, "ESCALATE"]
    assert alerts[1]["level"] == "UNHEALTHY" and alerts[1]["prev_level"] == "GOOD"


def test_jump_multiple_levels_reports_highest():
    _, alerts = run([40, 350])
    assert alerts[1]["level"] == "HAZARDOUS"


def test_drop_to_95_is_not_recovered():
    # USG, nguong duoi 101 - HYST 10 = 91 -> 95 nam trong vung hysteresis
    state, alerts = run([120, 120, 95, 95, 95])
    assert types(alerts) == [None, "ESCALATE", None, None, None]
    assert state["alerted_level"] == 2


def test_drop_to_85_twice_is_recovered():
    state, alerts = run([120, 120, 85, 85])
    assert types(alerts) == [None, "ESCALATE", None, "RECOVERED"]
    assert alerts[3]["level"] == "MODERATE" and alerts[3]["prev_level"] == "USG"
    assert state["alerted_level"] == 1


def test_recovery_needs_consecutive_readings():
    _, alerts = run([120, 120, 85, 95, 85])
    assert types(alerts)[2:] == [None, None, None]


def test_late_reading_is_ignored():
    state, _ = run([160])
    before = dict(state)
    state, alert = decide(state, 20, T0 - timedelta(hours=1))
    assert alert is None and state == before
    state, alert = decide(state, 20, T0)  # trung event_time cung bi bo
    assert alert is None and state == before


def test_replay_same_reading_no_duplicate_alert():
    state, alerts = run([180])
    assert alerts[0] is not None
    _, alert = decide(state, 180, T0)
    assert alert is None


def test_missing_data_3h_goes_unknown():
    state, _ = run([180])
    s2, alert = check_timeout(state, T0 + timedelta(hours=2))
    assert alert is None
    s3, alert = check_timeout(state, T0 + timedelta(hours=3, minutes=1))
    assert alert["type"] == "UNKNOWN" and alert["prev_level"] == "UNHEALTHY"
    assert s3["alerted_level"] == -1
    # Du lieu quay lai o muc thap: khong gui RECOVERED
    s4, alert = decide(s3, 30, T0 + timedelta(hours=4))
    assert alert is None and s4["alerted_level"] == 0


def test_timeout_ignored_below_usg():
    state, _ = run([80])
    _, alert = check_timeout(state, T0 + timedelta(hours=5))
    assert alert is None


def test_cooldown_blocks_same_level_but_not_escalation():
    # UNHEALTHY -> ha ve USG (lang le) -> lai UNHEALTHY trong 1h: khong bao lai;
    # len VERY_UNHEALTHY la tang muc moi nen van bao ngay
    state, alerts = run([160, 120, 120, 160, 210], step_min=5)
    assert types(alerts) == ["ESCALATE", None, None, None, "ESCALATE"]
    assert alerts[4]["level"] == "VERY_UNHEALTHY"
    # Sau cooldown (alert UNHEALTHY cuoi luc T0) thi bao lai duoc
    state = {"alerted_level": 2, "last_event_time": T0 + timedelta(minutes=90),
             "last_alert_at": T0, "last_alert_level": 3}
    _, alert = decide(state, 160, T0 + timedelta(minutes=95))
    assert alert["level"] == "UNHEALTHY"


def test_region_level_input_skips_aqi_hysteresis():
    state = {}
    state, a1 = decide(state, 120, T0, level=2)
    state, a2 = decide(state, 120, T0 + timedelta(minutes=5), level=2)
    assert a1 is None and a2["level"] == "USG"
    state, _ = decide(state, 99, T0 + timedelta(minutes=10), level=1)
    state, a4 = decide(state, 99, T0 + timedelta(minutes=15), level=1)
    assert a4["type"] == "RECOVERED"
