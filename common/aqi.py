"""Tính AQI theo EPA, dùng chung cho streaming (aqi_instant) và batch (aqi_daily).

Hai loại AQI chỉ khác đầu vào:
- aqi_instant: một bản đo đơn lẻ (averaging="instant")
- aqi_daily:   trung bình theo cửa sổ chuẩn EPA (PM 24h, O3/CO 8h) — batch tự tính rồi truyền vào
"""
import math
from typing import Optional

from common.config import load_yaml

BREAKPOINTS = load_yaml("aqi_breakpoints.yaml")

LEVEL_NAMES = ["GOOD", "MODERATE", "USG", "UNHEALTHY", "VERY_UNHEALTHY", "HAZARDOUS"]
LEVEL_LOWER_AQI = [0, 51, 101, 151, 201, 301]
UNKNOWN_LEVEL = -1

# Khối lượng mol (g/mol) để đổi µg/m³ <-> ppb ở 25°C, 1 atm: ppb = µg/m³ × 24.45 / MW
_MOLAR_MASS = {"o3": 48.00, "no2": 46.01, "so2": 64.07, "co": 28.01}
_MOLAR_VOLUME = 24.45


def get_level(aqi) -> int:
    """Mức EPA 0..5 của một giá trị AQI."""
    if aqi is None:
        return UNKNOWN_LEVEL
    for level in range(len(LEVEL_LOWER_AQI) - 1, -1, -1):
        if aqi >= LEVEL_LOWER_AQI[level]:
            return level
    return 0


def get_level_name(level: int) -> str:
    return LEVEL_NAMES[level] if 0 <= level < len(LEVEL_NAMES) else "UNKNOWN"


def normalize_units(units) -> str:
    u = (units or "").strip().lower().replace("µ", "u").replace("μ", "u").replace("³", "3")
    return u.replace(" ", "")


def convert_units(parameter: str, value: float, units, target: str) -> Optional[float]:
    """Đổi nồng độ về đơn vị của bảng breakpoint. Trả None nếu không đổi được."""
    src = normalize_units(units)
    if src == target or not src:
        return value
    mw = _MOLAR_MASS.get(parameter)
    if src in ("ug/m3", "mg/m3") and target in ("ppb", "ppm") and mw:
        ugm3 = value * 1000.0 if src == "mg/m3" else value
        ppb = ugm3 * _MOLAR_VOLUME / mw
        return ppb if target == "ppb" else ppb / 1000.0
    if src == "ppm" and target == "ppb":
        return value * 1000.0
    if src == "ppb" and target == "ppm":
        return value / 1000.0
    if src == "mg/m3" and target == "ug/m3":
        return value * 1000.0
    return None


def truncate(value: float, decimals: int) -> float:
    """Cắt (không làm tròn) theo quy định EPA; epsilon chống lỗi dấu phẩy động (0.29*100)."""
    factor = 10 ** decimals
    return math.floor(value * factor + 1e-9) / factor


def _round_half_up(x: float) -> int:
    return int(math.floor(x + 0.5))


def aqi_from_table(table_key: str, concentration: float) -> Optional[int]:
    """Nội suy AQI từ một bảng; nồng độ đã ở đúng đơn vị của bảng."""
    table = BREAKPOINTS.get(table_key)
    if table is None or concentration is None or concentration < 0:
        return None
    c = truncate(concentration, table["decimals"])
    bps = table["breakpoints"]
    for c_lo, c_hi, i_lo, i_hi in bps:
        if c_lo <= c <= c_hi:
            return _round_half_up((i_hi - i_lo) / (c_hi - c_lo) * (c - c_lo) + i_lo)
    c_lo, c_hi, i_lo, i_hi = bps[-1]
    if c > c_hi:
        # Vượt bảng: ngoại suy tuyến tính đoạn cuối (AQI > 500)
        return _round_half_up((i_hi - i_lo) / (c_hi - c_lo) * (c - c_lo) + i_lo)
    return None  # dưới đoạn đầu của bảng chỉ định nghĩa từ USG (o3_1h)


def compute_aqi(parameter, value, units, averaging: str = "instant") -> Optional[int]:
    """AQI của một chất.

    averaging:
      "instant" — bản đo đơn lẻ (streaming). O3 lấy max của bảng 8h và bảng 1h.
      "8h"      — giá trị đã là trung bình 8h (O3, CO trong batch).
      "24h"     — giá trị đã là trung bình 24h (PM2.5, PM10 trong batch).
    Chất không có bảng breakpoint -> None.
    """
    if value is None or parameter is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or value < 0:
        return None

    parameter = parameter.lower()
    if parameter == "o3":
        conc = convert_units("o3", value, units, BREAKPOINTS["o3_8h"]["units"])
        if conc is None:
            return None
        aqi_8h = aqi_from_table("o3_8h", conc) if truncate(conc, 3) <= 0.200 else None
        aqi_1h = aqi_from_table("o3_1h", conc)
        if averaging == "8h":
            # 8h không định nghĩa trên 0.200 ppm -> dùng bảng 1h theo hướng dẫn EPA
            return aqi_8h if aqi_8h is not None else aqi_1h
        candidates = [a for a in (aqi_8h, aqi_1h) if a is not None]
        return max(candidates) if candidates else None

    table = BREAKPOINTS.get(parameter)
    if table is None:
        return None
    conc = convert_units(parameter, value, units, table["units"])
    if conc is None:
        return None
    return aqi_from_table(parameter, conc)
