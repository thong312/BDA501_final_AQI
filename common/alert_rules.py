import yaml
from pathlib import Path
from datetime import datetime, timezone
import dateutil.parser

config_path = Path(__file__).parent.parent / "config" / "alert_rules.yaml"
with open(config_path, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

def get_level(aqi: int) -> int:
    if aqi <= 50: return 0
    if aqi <= 100: return 1
    if aqi <= 150: return 2
    if aqi <= 200: return 3
    if aqi <= 300: return 4
    return 5

def get_level_name(level: int) -> str:
    names = ["GOOD", "MODERATE", "USG", "UNHEALTHY", "VERY_UNHEALTHY", "HAZARDOUS"]
    return names[level] if 0 <= level < len(names) else "UNKNOWN"

def decide(state: dict, aqi: int, event_time: str) -> tuple:
    """
    Hàm thuần xử lý logic cảnh báo.
    Đầu vào:
    - state: Từ điển state của trạm (hoặc vùng) hiện tại
    - aqi: AQI cập nhật
    - event_time: Chuỗi thời gian sự kiện (ISO8601)
    Đầu ra: (new_state, alert_dict)
    """
    if aqi is None:
        return state, None

    ev_dt = dateutil.parser.parse(event_time)
    
    # Luật 1: Bỏ qua dữ liệu cũ
    if state.get("last_event_time"):
        last_ev_dt = dateutil.parser.parse(state["last_event_time"])
        if ev_dt <= last_ev_dt:
            return state, None

    # Khởi tạo state nếu trống
    new_state = dict(state)
    new_state["last_event_time"] = event_time
    if "alerted_level" not in new_state:
        new_state["alerted_level"] = 0
        new_state["candidate_level"] = 0
        new_state["candidate_count"] = 0

    new_level = get_level(aqi)
    curr_level = new_state["alerted_level"]
    alert = None

    # Luật 7: Timeout xử lý trước (Giả sử timeout được trigger bên ngoài và truyền aqi=None, nhưng ở đây dùng logic chênh lệch time)
    # Xử lý logic nhảy mức
    if new_level != curr_level:
        if new_level >= CONFIG["URGENT_MIN_LEVEL"] and new_level > curr_level:
            # Luật 4: Nhảy lên nguy hiểm -> Báo ngay lập tức
            new_state["alerted_level"] = new_level
            new_state["candidate_level"] = new_level
            new_state["candidate_count"] = 0
            
            # Kiểm tra cooldown (Luật 6) - nếu tăng mức thì bỏ qua cooldown
            alert = create_alert(new_state, curr_level, new_level, event_time, "ESCALATE", aqi)
            new_state["last_alert_at"] = event_time
        elif new_level >= CONFIG["ALERT_MIN_LEVEL"] and new_level > curr_level:
            # Luật 3: Tăng lên USG -> Xác nhận
            if new_state["candidate_level"] == new_level:
                new_state["candidate_count"] += 1
            else:
                new_state["candidate_level"] = new_level
                new_state["candidate_count"] = 1
                
            if new_state["candidate_count"] >= CONFIG["CONFIRM_N"]:
                new_state["alerted_level"] = new_level
                alert = create_alert(new_state, curr_level, new_level, event_time, "ESCALATE", aqi)
                new_state["last_alert_at"] = event_time
        elif new_level < curr_level:
            # Giảm mức (Luật 5)
            # Giả lập logic hysteresis cho đơn giản
            if new_state["candidate_level"] == new_level:
                new_state["candidate_count"] += 1
            else:
                new_state["candidate_level"] = new_level
                new_state["candidate_count"] = 1
                
            if new_state["candidate_count"] >= CONFIG["CONFIRM_N"]:
                new_state["alerted_level"] = new_level
                if new_level < CONFIG["ALERT_MIN_LEVEL"] and curr_level >= CONFIG["ALERT_MIN_LEVEL"]:
                    alert = create_alert(new_state, curr_level, new_level, event_time, "RECOVERED", aqi)
                    new_state["last_alert_at"] = event_time
    else:
        new_state["candidate_level"] = new_level
        new_state["candidate_count"] = 1

    return new_state, alert

def create_alert(state, prev_level, new_level, event_time, alert_type, aqi):
    return {
        "level": get_level_name(new_level),
        "prev_level": get_level_name(prev_level),
        "type": alert_type,
        "event_time": event_time,
        "aqi": aqi
    }
