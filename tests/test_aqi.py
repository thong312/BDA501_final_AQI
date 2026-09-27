import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from common.aqi import compute_aqi

def test_compute_aqi_pm25():
    # PM2.5 = 20.0 -> 71 (Moderate)
    # val = 20.0 -> 20.0, bp: 9.1-35.4 (51-100)
    # (100 - 51) / (35.4 - 9.1) * (20.0 - 9.1) + 51
    # = 49 / 26.3 * 10.9 + 51 = 20.3 + 51 = 71
    assert compute_aqi("pm25", 20.0, "µg/m³") == 71

def test_compute_aqi_exact_boundaries():
    # 9.0 -> 50
    assert compute_aqi("pm25", 9.0, "µg/m³") == 50
    # 9.1 -> 51
    assert compute_aqi("pm25", 9.1, "µg/m³") == 51

def test_compute_aqi_negative_none():
    assert compute_aqi("pm25", -5.0, "µg/m³") is None
    assert compute_aqi("pm25", None, "µg/m³") is None
    assert compute_aqi("unknown_param", 10.0, "") is None

def test_compute_aqi_beyond_index():
    # >= 225.5 -> 301-500
    # For val = 600.0, which is > 500.4
    # The code extends the highest formula
    aqi = compute_aqi("pm25", 600.0, "µg/m³")
    assert aqi > 500
