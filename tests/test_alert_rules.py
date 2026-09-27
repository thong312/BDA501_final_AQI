import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from common.alert_rules import decide, get_level, get_level_name

def test_alert_rules_escalate():
    state = {}
    
    # Lên USG (2) nhưng cần 2 lần xác nhận (CONFIRM_N = 2)
    state, alert = decide(state, 120, "2026-09-26T10:00:00Z")
    assert state["alerted_level"] == 0 # Vẫn chưa báo
    assert alert is None
    
    # Lần 2 xác nhận
    state, alert = decide(state, 130, "2026-09-26T10:15:00Z")
    assert state["alerted_level"] == 2
    assert alert is not None
    assert alert["type"] == "ESCALATE"
    assert alert["level"] == "USG"

def test_alert_rules_urgent():
    state = {}
    # Lên thẳng Unhealthy (3) -> báo ngay lập tức
    state, alert = decide(state, 160, "2026-09-26T10:00:00Z")
    assert state["alerted_level"] == 3
    assert alert is not None
    assert alert["type"] == "ESCALATE"

def test_alert_rules_ignore_old_data():
    state = {"last_event_time": "2026-09-26T10:00:00Z", "alerted_level": 3}
    # Bản đo trễ
    state, alert = decide(state, 50, "2026-09-26T09:00:00Z")
    assert alert is None
    assert state["alerted_level"] == 3

def test_alert_rules_recover():
    state = {"last_event_time": "2026-09-26T10:00:00Z", "alerted_level": 2, "candidate_level": 2, "candidate_count": 2}
    
    # Giảm về Moderate (1)
    state, alert = decide(state, 90, "2026-09-26T11:00:00Z")
    assert state["alerted_level"] == 2 # Chưa qua CONFIRM_N
    assert alert is None
    
    # Lần 2
    state, alert = decide(state, 85, "2026-09-26T11:15:00Z")
    assert state["alerted_level"] == 1
    assert alert is not None
    assert alert["type"] == "RECOVERED"
