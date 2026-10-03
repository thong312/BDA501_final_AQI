"""Tinh AQI theo EPA, dung chung cho streaming (aqi_instant) va batch (aqi_daily).

Hai loai AQI chi khac dau vao:
- aqi_instant: mot ban do don le (averaging="instant")
- aqi_daily:   trung binh theo cua so chuan EPA (PM 24h, O3/CO 8h) — batch tu tinh roi truyen vao
"""
import math
from typing import Optional

from common.config import load_yaml

BREAKPOINTS = load_yaml("aqi_breakpoints.yaml")

LEVEL_NAMES = ["GOOD", "MODERATE", "USG", "UNHEALTHY", "VERY_UNHEALTHY", "HAZARDOUS"]
LEVEL_LOWER_AQI = [0, 51, 101, 151, 201, 301]
UNKNOWN_LEVEL = -1

# Khoi luong mol (g/mol) de doi µg/m³ <-> ppb o 25°C, 1 atm: ppb = µg/m³ × 24.45 / MW
_MOLAR_MASS = {"o3": 48.00, "no2": 46.01, "so2": 64.07, "co": 28.01}
_MOLAR_VOLUME = 24.45


def get_level(aqi) -> int:
    """Muc EPA 0..5 cua mot gia tri AQI."""
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
    """Doi nong do ve don vi cua bang breakpoint. Tra None neu khong doi duoc."""
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
    """Cat (khong lam tron) theo quy dinh EPA; epsilon chong loi dau phay dong (0.29*100)."""
    factor = 10 ** decimals
    return math.floor(value * factor + 1e-9) / factor


def _round_half_up(x: float) -> int:
    return int(math.floor(x + 0.5))


def aqi_from_table(table_key: str, concentration: float) -> Optional[int]:
    """Noi suy AQI tu mot bang; nong do da o dung don vi cua bang."""
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
        # Vuot bang: ngoai suy tuyen tinh doan cuoi (AQI > 500)
        return _round_half_up((i_hi - i_lo) / (c_hi - c_lo) * (c - c_lo) + i_lo)
    return None  # duoi doan dau cua bang chi dinh nghia tu USG (o3_1h)


def compute_aqi(parameter, value, units, averaging: str = "instant") -> Optional[int]:
    """AQI cua mot chat.

    averaging:
      "instant" — ban do don le (streaming). O3 lay max cua bang 8h va bang 1h.
      "8h"      — gia tri da la trung binh 8h (O3, CO trong batch).
      "1h"      — O3 trung binh 1h, chi dung bang o3_1h (dinh nghia tu USG tro len).
      "24h"     — gia tri da la trung binh 24h (PM2.5, PM10 trong batch).
    Chat khong co bang breakpoint -> None.
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
            # 8h khong dinh nghia tren 0.200 ppm -> dung bang 1h theo huong dan EPA
            return aqi_8h if aqi_8h is not None else aqi_1h
        if averaging == "1h":
            return aqi_1h
        candidates = [a for a in (aqi_8h, aqi_1h) if a is not None]
        return max(candidates) if candidates else None

    table = BREAKPOINTS.get(parameter)
    if table is None:
        return None
    conc = convert_units(parameter, value, units, table["units"])
    if conc is None:
        return None
    return aqi_from_table(parameter, conc)
