"""MapReduce thống kê ngày từ Bronze để đối chiếu độc lập với Spark (ARCHITECTURE 6.4).

Áp CÙNG quy tắc với Spark làm sạch (common/geo.py, common/quality.py):
  - lọc bbox NYC
  - bỏ NO_METADATA / NEGATIVE / OUT_OF_RANGE (STALE được giữ như Spark)
  - ngày theo giờ New York, tính từ datetime_utc
  - dedupe theo (sensor_id, datetime_utc), giữ bản ingested_at mới nhất

Key    = [location_id, parameter, date_local]
Value  = [sensor_id, datetime_utc, ingested_at, offset, value, units]
Output = {count, sum, max, avg, hours_over_threshold}

KHÔNG dùng combiner: combiner cộng dồn cục bộ trước khi dedupe, nên hai bản trùng
(sensor_id, datetime_utc) nằm ở hai mapper khác nhau sẽ đều được cộng vào sum/count -> sai.
Reducer cần thấy toàn bộ bản ghi gốc của một key để dedupe trước khi tổng hợp.
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
    """Hàm thuần của mapper: một dòng Bronze -> (key, value) hoặc None."""
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
    # Giống from_json của Spark: chỉ nhận số JSON, còn lại -> null -> NEGATIVE
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
    """Hàm thuần của reducer: dedupe rồi tính thống kê."""
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
        self.add_passthru_arg("--date", default=None, help="Chỉ giữ date_local này (YYYY-MM-DD)")

    def mapper_init(self):
        # bbox lấy từ env như các thành phần khác (giá trị bắt đầu bằng '-' không truyền được qua argv)
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
