import yaml
from pathlib import Path
import math

config_path = Path(__file__).parent.parent / "config" / "aqi_breakpoints.yaml"
with open(config_path, "r", encoding="utf-8") as f:
    BREAKPOINTS = yaml.safe_load(f)

def get_level(aqi: int) -> int:
    if aqi <= 50: return 0 # Good
    if aqi <= 100: return 1 # Moderate
    if aqi <= 150: return 2 # USG
    if aqi <= 200: return 3 # Unhealthy
    if aqi <= 300: return 4 # Very Unhealthy
    return 5 # Hazardous

def get_level_name(level: int) -> str:
    return ["GOOD", "MODERATE", "USG", "UNHEALTHY", "VERY_UNHEALTHY", "HAZARDOUS"][level]

def compute_aqi(parameter: str, value: float, units: str) -> int:
    """
    Tính AQI (cấp 0-500) dựa trên công thức nội suy tuyến tính của EPA.
    """
    if value is None or value < 0:
        return None
    
    if parameter not in BREAKPOINTS:
        return None
        
    val = round(value, 1)
    bps = BREAKPOINTS[parameter]
    
    for bp in bps:
        if bp["c_low"] <= val <= bp["c_high"]:
            aqi = (bp["i_high"] - bp["i_low"]) / (bp["c_high"] - bp["c_low"]) * (val - bp["c_low"]) + bp["i_low"]
            return int(round(aqi))
            
    # Xử lý giá trị vượt bảng (Beyond Index)
    highest = bps[-1]
    if val > highest["c_high"]:
        aqi = (highest["i_high"] - highest["i_low"]) / (highest["c_high"] - highest["c_low"]) * (val - highest["c_low"]) + highest["i_low"]
        return int(round(aqi))

    return None
