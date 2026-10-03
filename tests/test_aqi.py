import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.aqi import compute_aqi, convert_units, get_level, truncate


def test_pm25_20_is_71():
    assert compute_aqi("pm25", 20.0, "µg/m³") == 71


def test_pm25_exact_boundaries():
    assert compute_aqi("pm25", 9.0, "µg/m³") == 50
    assert compute_aqi("pm25", 9.1, "µg/m³") == 51
    assert compute_aqi("pm25", 35.4, "µg/m³") == 100
    assert compute_aqi("pm25", 35.5, "µg/m³") == 101


def test_pm25_truncates_not_rounds():
    # 9.09 cat thanh 9.0 -> 50 (lam tron se ra 9.1 -> 51)
    assert compute_aqi("pm25", 9.09, "µg/m³") == 50


def test_truncate_float_safety():
    assert truncate(0.29, 2) == 0.29
    assert truncate(0.0559, 3) == 0.055


def test_invalid_inputs():
    assert compute_aqi("pm25", -5.0, "µg/m³") is None
    assert compute_aqi("pm25", None, "µg/m³") is None
    assert compute_aqi("unknown_param", 10.0, "") is None
    assert compute_aqi("pm25", "abc", "µg/m³") is None


def test_beyond_index_extrapolates():
    assert compute_aqi("pm25", 400.0, "µg/m³") > 500


def test_units_alias():
    assert compute_aqi("pm25", 20.0, "ug/m3") == 71
    assert compute_aqi("pm25", 20.0, "μg/m³") == 71  # chu mu Hy Lap


def test_o3_ppm_8h_and_instant():
    assert compute_aqi("o3", 0.060, "ppm", averaging="8h") == 67
    # 0.150 ppm vuot bang 8h theo muc USG; instant lay max(8h, 1h)
    assert compute_aqi("o3", 0.150, "ppm") == max(
        compute_aqi("o3", 0.150, "ppm", averaging="8h"), 132)


def test_o3_ugm3_converted_to_ppm():
    # 117.8 µg/m³ ≈ 0.060 ppm
    assert compute_aqi("o3", 117.8, "µg/m³", averaging="8h") == compute_aqi("o3", 0.060, "ppm", averaging="8h")


def test_no2_ppb_and_ppm():
    assert compute_aqi("no2", 53, "ppb") == 50
    assert compute_aqi("no2", 0.054, "ppm") == 51


def test_convert_units():
    assert abs(convert_units("no2", 18.82, "µg/m³", "ppb") - 10.0) < 0.01
    assert convert_units("pm25", 1.0, "ppm", "ug/m3") is None


def test_get_level():
    assert [get_level(a) for a in (0, 50, 51, 100, 101, 150, 151, 200, 201, 300, 301)] == \
           [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5]
