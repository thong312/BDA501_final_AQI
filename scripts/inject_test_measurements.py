"""Bơm bản đo thử vào aq.openaq.measurements.v1 (demo cảnh báo / replay).

Chạy trong container (cần job streaming đang chạy):
    docker exec -w /opt/aq spark-master python3 scripts/inject_test_measurements.py            # đẩy AQI vọt lên -> ESCALATE
    docker exec -w /opt/aq spark-master python3 scripts/inject_test_measurements.py --recover  # 2 lần đo sạch -> RECOVERED
    docker exec -w /opt/aq spark-master python3 scripts/inject_test_measurements.py --at 2026-10-03T17:43:44Z --pm25 200
        # replay đúng lần bơm trước (cùng sensor, cùng event_time) -> không sinh cảnh báo trùng

Sensor được chọn từ metadata thật (topic compacted aq.openaq.sensors.v1): mỗi borough một sensor PM2.5,
ưu tiên borough đang ở mức Good/Moderate trong realtime.region_status (borough đã báo thì không báo lại).
Kèm 3 bản lỗi để minh hoạ quality flag: NEGATIVE, NO_METADATA, STALE.
Mọi bản ghi có source = "manual-alert-test" để phân biệt với dữ liệu OpenAQ.
"""
import argparse
import json
import time
from datetime import datetime, timedelta, timezone

import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition

from common.config import TOPIC_MEASUREMENTS, TOPIC_SENSORS
from common.alerting import region_alert_id
from common.geo import get_borough

BOOTSTRAP = "kafka:9092"
PG_DSN = "postgresql://aq_user:aq_password@postgres:5432/aq"
SOURCE = "manual-alert-test"
# alert_id tất định theo (borough, giờ UTC, level) nên cùng level trong cùng giờ bị ON CONFLICT bỏ qua.
# Mỗi lần chạy chọn level chưa dùng trong giờ hiện tại -> tập dượt được nhiều lần trong 1 giờ.
SPIKES = [(200.0, "VERY_UNHEALTHY"), (100.0, "UNHEALTHY"), (300.0, "HAZARDOUS")]  # >= Unhealthy: báo ngay
CLEANS = [(5.0, "GOOD"), (20.0, "MODERATE")]  # qua hysteresis, 2 lần liên tiếp -> RECOVERED
NO_METADATA_SENSOR = 999999999


def load_sensors():
    """Đọc toàn bộ topic compacted, giữ bản mới nhất mỗi key."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "aq-inject-probe",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    parts = c.list_topics(TOPIC_SENSORS, timeout=10).topics[TOPIC_SENSORS].partitions
    tps, ends = [], {}
    for p in parts:
        _lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC_SENSORS, p), timeout=10)
        if hi > 0:
            tps.append(TopicPartition(TOPIC_SENSORS, p, 0))
            ends[p] = hi
    c.assign(tps)
    latest = {}
    while ends:
        msg = c.poll(5)
        if msg is None:
            break
        if msg.error():
            continue
        latest[msg.key()] = json.loads(msg.value()) if msg.value() else None
        if msg.offset() + 1 >= ends.get(msg.partition(), 0):
            ends.pop(msg.partition(), None)
    c.close()
    return [m for m in latest.values() if m]


def region_levels():
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT borough, region_level FROM realtime.region_status")
        return dict(cur.fetchall())


def used_levels(borough, now):
    """Các level đã có alert cấp vùng của borough trong giờ UTC của now."""
    prefix = region_alert_id(borough, now, "")
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT alert_id FROM realtime.alerts WHERE alert_id LIKE %s", (prefix + "%",))
        return {r[0][len(prefix):] for r in cur.fetchall()}


def choose(options, borough, now, strict=True):
    used = used_levels(borough, now)
    for value, level in options:
        if level not in used:
            return value, level
    names = [lv for _v, lv in options]
    if strict:
        raise SystemExit(f"{borough}: đã dùng hết level {names} trong giờ này, đợi sang giờ mới.")
    # Recover: vẫn đưa state về Good, chỉ là alert trùng id nên không ghi/gửi lại
    print(f"{borough}: đã dùng hết level {names} trong giờ này -> state vẫn về Good nhưng không có alert mới.")
    return options[0]


def pick_sensors(sensors, levels, n_boroughs, replay=False):
    by_borough = {}
    for m in sorted(sensors, key=lambda m: m["sensor_id"]):
        if m.get("parameter") != "pm25":
            continue
        b = get_borough(m.get("lat"), m.get("lon"))
        if b and b not in by_borough:
            by_borough[b] = m
    ready = [b for b in sorted(by_borough) if replay or (levels.get(b) or 0) < 2]
    if len(ready) < len(by_borough):
        print("Bỏ qua borough đang >= USG (chạy --recover trước):",
              sorted(set(by_borough) - set(ready)))
    return [dict(by_borough[b], borough=b) for b in ready[:n_boroughs]]  # Python 3.8 trong container


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--recover", action="store_true", help="đưa các borough đang báo về Good")
    ap.add_argument("--boroughs", type=int, default=2, help="số borough cho kịch bản ESCALATE")
    ap.add_argument("--at", help="event_time ISO của lần bơm cần replay (cùng sensor, cùng thời điểm)")
    ap.add_argument("--pm25", type=float, help="giá trị PM2.5 của lần bơm cần replay (đi cùng --at)")
    args = ap.parse_args()

    now = datetime.now(timezone.utc).replace(microsecond=0)
    if args.at:
        now = datetime.fromisoformat(args.at.replace("Z", "+00:00"))
    sensors = load_sensors()
    levels = region_levels()
    print(f"metadata: {len(sensors)} sensors; region_level hiện tại: {levels}")

    if args.recover:
        # Trạm về Good ngay trong 1 trigger (2 bản đo), nhưng vùng chỉ được đánh giá 1 lần mỗi
        # micro-batch nên cần thêm 1 trigger nữa mới đủ CONFIRM_N = 2 -> gửi lại tới khi vùng hạ mức.
        for attempt in range(1, 4):
            by_borough = {}
            for m in sorted(sensors, key=lambda m: m["sensor_id"]):
                b = get_borough(m.get("lat"), m.get("lon"))
                if m.get("parameter") == "pm25" and b and (levels.get(b) or 0) >= 2:
                    by_borough.setdefault(b, m)
            if not by_borough:
                print("Không còn borough nào >= USG." if attempt > 1 else
                      "Không có borough nào đang >= USG, không cần recover.")
                return
            rows = []
            for b, m in sorted(by_borough.items()):
                value, level = choose(CLEANS, b, now, strict=False)
                print(f"RECOVER {b} (lần {attempt}): sensor {m['sensor_id']} @ location {m['location_id']} "
                      f"({m['location_name']}) pm25={value} -> {level}")
                rows += [(m["sensor_id"], m["location_id"], value, now + timedelta(seconds=k))
                         for k in (0, 1)]
            send(rows, now)
            print("Đợi 75 giây cho 1 trigger của Query B...")
            time.sleep(75)
            now = datetime.now(timezone.utc).replace(microsecond=0)
            levels = region_levels()
        print("Vẫn còn borough >= USG sau 3 lần, kiểm tra log streaming:", levels)
        return

    picked = pick_sensors(sensors, levels, args.boroughs, replay=bool(args.at))
    if not picked:
        raise SystemExit("Không có borough nào sẵn sàng; chạy lại với --recover rồi thử lại.")
    rows = []  # (sensor_id, location_id, value, event_time)
    if args.at and args.pm25 is None:
        raise SystemExit("--at cần kèm --pm25 (in ra ở lần bơm gốc).")
    for m in picked:
        value, level = (args.pm25, "replay") if args.at else choose(SPIKES, m["borough"], now)
        print(f"SPIKE {m['borough']}: sensor {m['sensor_id']} @ location {m['location_id']} "
              f"({m['location_name']}) pm25={value} -> {level}")
        rows.append((m["sensor_id"], m["location_id"], value, now))
    first = picked[0]
    rows += [
        (first["sensor_id"], first["location_id"], -5.0, now - timedelta(minutes=1)),   # NEGATIVE
        (NO_METADATA_SENSOR, first["location_id"], 10.0, now),                           # NO_METADATA
        (first["sensor_id"], first["location_id"], 30.0, now - timedelta(hours=5)),      # STALE
    ]
    send(rows, now)
    print("Cảnh báo xuất hiện sau 1 trigger (~1 phút): realtime.alerts, log notifier, dashboard :5050")
    if not args.at:
        print(f"Replay không sinh cảnh báo trùng: ... inject_test_measurements.py --at {fmt(now)} "
              f"--pm25 {rows[0][2]}")


def fmt(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def send(rows, now, source=SOURCE, quiet=False):
    """rows: (sensor_id, location_id, value, event_time); key Kafka = location_id như poller."""
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for sid, loc, v, t in rows:
        m = {"sensor_id": sid, "location_id": loc, "value": v, "datetime_utc": fmt(t),
             "ingested_at": fmt(now), "source": source}
        p.produce(TOPIC_MEASUREMENTS, key=str(loc).encode(), value=json.dumps(m).encode())
    undelivered = p.flush(10)
    if undelivered or not quiet:
        print(f"sent {len(rows)} messages, event_time {fmt(now)}, undelivered {undelivered}")

if __name__ == "__main__":
    main()
