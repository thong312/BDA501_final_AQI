"""Giả lập luồng streaming cho buổi demo: OpenAQ API chỉ có giá trị theo giờ, công bố trễ,
nên trên sân khấu gần như không thấy dữ liệu mới. Script bơm bản đo PM2.5 của các sensor thật
(scripts/fake_data.csv) vào aq.openaq.measurements.v1, đi qua đúng đường poller -> Kafka -> Query A/B.

    docker exec -it -w /opt/aq spark-master python3 scripts/inject_from_csv.py --minutes 10

- event_time = thời điểm hiện tại: không đẩy watermark của Query B vào tương lai (dữ liệu thật vẫn
  được nhận) và state trạm/vùng trong PostgreSQL không bị nhảy giờ.
- Mỗi trạm đi theo một sóng low -> high -> low (lệch pha), cộng nhiễu: AQI vượt ngưỡng rồi về lại,
  nên thấy đủ ESCALATE, xác nhận 2 lần (USG), hysteresis và RECOVERED; alert_id theo giờ + cooldown
  1 giờ chặn spam khi sóng lặp lại.
- Hết thời gian: bơm giá trị low thêm SETTLE_SEC để các trạm/vùng về Good (tập dượt lại được).
- source = "manual-demo" để phân biệt với dữ liệu OpenAQ.
"""
import argparse
import csv
import math
import random
import time
from datetime import datetime, timezone

from common.aqi import compute_aqi, get_level, get_level_name
from scripts.inject_test_measurements import fmt, load_sensors, send

SOURCE = "manual-demo"
SETTLE_SEC = 150  # vùng cần 2 micro-batch (trigger 1 phút) để xác nhận hạ mức


def load_rows(path, sensors):
    meta = {m["sensor_id"]: m for m in sensors}
    rows = []
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            m = meta.get(int(r["sensor_id"]))
            if m is None or m.get("parameter") != "pm25":
                print(f"Bỏ qua sensor {r['sensor_id']}: không có trong metadata hoặc không phải pm25")
                continue
            rows.append({"sensor_id": m["sensor_id"], "location_id": m["location_id"], "name": m["location_name"],
                         "units": m["units"], "low": float(r["low"]), "high": float(r["high"]),
                         "phase": float(r["phase"])})
    if not rows:
        raise SystemExit("Không có sensor hợp lệ trong " + path)
    return rows


def wave(r, elapsed, period):
    """low -> high -> low trong một chu kỳ, lệch pha theo từng trạm, nhiễu ±8%."""
    x = 0.5 - 0.5 * math.cos(2 * math.pi * (elapsed / period + r["phase"]))
    return round(max(0.0, (r["low"] + (r["high"] - r["low"]) * x) * random.uniform(0.92, 1.08)), 1)


def tick(rows, values):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    send([(r["sensor_id"], r["location_id"], v, now) for r, v in zip(rows, values)], now, source=SOURCE, quiet=True)
    parts = []
    for r, v in zip(rows, values):
        aqi = compute_aqi("pm25", v, r["units"])
        parts.append(f"{r['name'][:18]:<18} pm25={v:6.1f} AQI={aqi:>3} {get_level_name(get_level(aqi))}")
    print(fmt(now), " | ".join(parts), flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="scripts/fake_data.csv")
    ap.add_argument("--minutes", type=float, default=10, help="thời gian giả lập (phút)")
    ap.add_argument("--interval", type=int, default=20, help="giây giữa 2 lần bơm")
    ap.add_argument("--period", type=float, default=4, help="chu kỳ sóng (phút)")
    ap.add_argument("--no-settle", action="store_true", help="không đưa các trạm về Good khi kết thúc")
    args = ap.parse_args()

    rows = load_rows(args.csv, load_sensors())
    print(f"Giả lập {len(rows)} trạm trong {args.minutes:g} phút, mỗi {args.interval}s (Ctrl+C để dừng)")
    start = time.monotonic()
    try:
        while (elapsed := time.monotonic() - start) < args.minutes * 60:
            tick(rows, [wave(r, elapsed, args.period * 60) for r in rows])
            time.sleep(args.interval)
        if not args.no_settle:
            print(f"Kết thúc: bơm giá trị low thêm {SETTLE_SEC}s để các trạm/vùng về Good...")
            end = time.monotonic() + SETTLE_SEC
            while time.monotonic() < end:
                tick(rows, [r["low"] for r in rows])
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nĐã dừng. Đưa về Good: python3 scripts/inject_test_measurements.py --recover")


if __name__ == "__main__":
    main()
