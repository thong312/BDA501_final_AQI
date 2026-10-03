"""MapReduce thong ke ngay tu Bronze de doi chieu doc lap voi Spark (ARCHITECTURE 6.4).

Ap CUNG quy tac voi Spark lam sach (common/geo.py, common/quality.py):
  - loc bbox NYC
  - bo NO_METADATA / NEGATIVE / OUT_OF_RANGE (STALE duoc giu nhu Spark)
  - ngay theo gio New York, tinh tu datetime_utc
  - dedupe theo (sensor_id, datetime_utc), giu ban ingested_at moi nhat

Key    = [location_id, parameter, date_local]
Value  = [sensor_id, datetime_utc, ingested_at, offset, value, units]
Output = {count, sum, max, avg, hours_over_threshold}

KHONG dung combiner: combiner cong don cuc bo truoc khi dedupe, nen hai ban trung
(sensor_id, datetime_utc) nam o hai mapper khac nhau se deu duoc cong vao sum/count -> sai.
Reducer can thay toan bo ban ghi goc cua mot key de dedupe truoc khi tong hop.
"""
import json
import os
import sys
from datetime import timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dateutil import parser as dtparser
from dateutil import tz
from mrjob.job import MRJob

from common.config import DEFAULT_BBOX, TIMEZONE
from common.geo import in_bbox
from common.quality import BATCH_DROP_FLAGS, quality_flag, threshold

NY = tz.gettz(TIMEZONE)


def date_local_of(datetime_utc: str):
    dt = dtparser.isoparse(datetime_utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(NY).date().isoformat(), dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def map_record(line: str, lookup: dict, bbox: str, target_date: str = None):
    """Ham thuan cua mapper: mot dong Bronze -> (key, value) hoac None."""
    try:
        row = json.loads(line)
        data = json.loads(row["value"])
    except (ValueError, KeyError, TypeError):
        return None
    if not isinstance(data, dict) or data.get("sensor_id") is None or data.get("datetime_utc") is None:
        return None
    sensor_id = data["sensor_id"]
    meta = lookup.get(sensor_id, {})
    parameter = data.get("parameter") or meta.get("parameter")
    units = data.get("units") or meta.get("units")
    lat, lon = data.get("lat"), data.get("lon")
    if lat is None or lon is None:
        lat, lon = meta.get("lat"), meta.get("lon")
    if not in_bbox(lat, lon, bbox):
        return None
    # Giong from_json cua Spark: chi nhan so JSON, con lai -> null -> NEGATIVE
    raw_value = data.get("value")
    value = float(raw_value) if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool) else None
    if quality_flag(parameter, units, value) in BATCH_DROP_FLAGS:
        return None
    try:
        date_local, dt_utc = date_local_of(data["datetime_utc"])
    except (ValueError, OverflowError):
        return None
    if target_date and date_local != target_date:
        return None
    if data.get("location_id") is None:
        return None
    key = [data["location_id"], parameter, date_local]
    return key, [sensor_id, dt_utc, data.get("ingested_at") or "", row.get("offset") or 0, float(value), units]


def reduce_values(key, values):
    """Ham thuan cua reducer: dedupe roi tinh thong ke."""
    latest = {}
    for sensor_id, dt_utc, ingested_at, offset, value, units in values:
        rank = (ingested_at, offset, value)
        k = (sensor_id, dt_utc)
        if k not in latest or rank > latest[k][0]:
            latest[k] = (rank, value, units)
    if not latest:
        return None
    vals = [v for _, v, _ in latest.values()]
    units = next(iter(latest.values()))[2]
    thr = threshold(key[1], units)
    return {
        "count": len(vals),
        "sum": sum(vals),
        "max": max(vals),
        "avg": sum(vals) / len(vals),
        "hours_over_threshold": sum(1 for v in vals if thr is not None and v > thr),
    }


class DailyStatsMR(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_file_arg("--lookup", help="validation/sensor_lookup.json (JSON Lines)")
        self.add_passthru_arg("--date", default=None, help="Chi giu date_local nay (YYYY-MM-DD)")

    def mapper_init(self):
        # bbox lay tu env nhu cac thanh phan khac (gia tri bat dau bang '-' khong truyen duoc qua argv)
        self.bbox = os.getenv("NYC_BBOX", DEFAULT_BBOX)
        self.lookup = {}
        if self.options.lookup:
            with open(self.options.lookup, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        d = json.loads(line)
                        self.lookup[d["sensor_id"]] = d

    def mapper(self, _, line):
        out = map_record(line, self.lookup, self.bbox, self.options.date)
        if out is not None:
            yield out

    def reducer(self, key, values):
        stats = reduce_values(key, values)
        if stats is not None:
            yield key, stats


if __name__ == "__main__":
    DailyStatsMR.run()
