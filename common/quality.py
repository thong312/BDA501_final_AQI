"""Quy tac chat luong du lieu dung chung (ARCHITECTURE 6.2 buoc 3, 6.4, 6.5).

Thu tu uu tien flag: NO_METADATA > NEGATIVE > OUT_OF_RANGE > STALE > OK.
- Streaming query B: chi giu OK.
- Batch (lam sach, MapReduce): gan flag, loc NO_METADATA/NEGATIVE/OUT_OF_RANGE;
  STALE van giu lai vi trong batch ban do tre van la du lieu lich su hop le.
"""
from datetime import datetime
from typing import Optional

from common.aqi import normalize_units
from common.config import load_yaml

_CFG = load_yaml("quality_rules.yaml")
RULES = _CFG.get("rules", {})
STALE_SEC = _CFG.get("STALE_SEC", 10800)

FLAGS = ("OK", "NEGATIVE", "OUT_OF_RANGE", "STALE", "NO_METADATA")
BATCH_DROP_FLAGS = ("NO_METADATA", "NEGATIVE", "OUT_OF_RANGE")


def get_rule(parameter, units) -> Optional[dict]:
    if parameter is None:
        return None
    return RULES.get(parameter.lower(), {}).get(normalize_units(units))


def quality_flag(parameter, units, value, event_time: Optional[datetime] = None,
                 reference_time: Optional[datetime] = None) -> str:
    """Flag cua mot ban do. reference_time: 'now' (streaming) hoac ingested_at (batch)."""
    if parameter is None:
        return "NO_METADATA"
    if value is None or value < 0:
        return "NEGATIVE"
    rule = get_rule(parameter, units)
    if rule and not (rule["min"] <= value <= rule["max"]):
        return "OUT_OF_RANGE"
    if event_time is not None and reference_time is not None \
            and (reference_time - event_time).total_seconds() > STALE_SEC:
        return "STALE"
    return "OK"


def threshold(parameter, units) -> Optional[float]:
    rule = get_rule(parameter, units)
    return rule.get("threshold") if rule else None


# ---------- Phien ban cot Spark (cung logic, sinh tu cung file YAML) ----------

def spark_normalized_units(col):
    from pyspark.sql import functions as F
    u = F.lower(F.trim(col))
    for src, dst in (("µ", "u"), ("μ", "u"), ("³", "3"), (" ", "")):
        u = F.regexp_replace(u, src, dst)
    return u


def spark_quality_flag(parameter_col, units_col, value_col, event_ts_col=None, reference_ts_col=None):
    from pyspark.sql import functions as F
    units_n = spark_normalized_units(units_col)
    param_l = F.lower(parameter_col)
    out_of_range = F.lit(False)
    for param, by_units in RULES.items():
        for unit, rule in by_units.items():
            out_of_range = out_of_range | (
                (param_l == param) & (units_n == unit)
                & ((value_col < rule["min"]) | (value_col > rule["max"])))
    flag = (F.when(parameter_col.isNull(), "NO_METADATA")
            .when(value_col.isNull() | (value_col < 0), "NEGATIVE")
            .when(out_of_range, "OUT_OF_RANGE"))
    if event_ts_col is not None and reference_ts_col is not None:
        flag = flag.when(
            reference_ts_col.cast("long") - event_ts_col.cast("long") > STALE_SEC, "STALE")
    return flag.otherwise("OK")


def spark_threshold(parameter_col, units_col):
    from pyspark.sql import functions as F
    units_n = spark_normalized_units(units_col)
    param_l = F.lower(parameter_col)
    expr = None
    for param, by_units in RULES.items():
        for unit, rule in by_units.items():
            cond = (param_l == param) & (units_n == unit)
            expr = F.when(cond, F.lit(float(rule["threshold"]))) if expr is None \
                else expr.when(cond, F.lit(float(rule["threshold"])))
    return expr.otherwise(F.lit(None).cast("double"))
