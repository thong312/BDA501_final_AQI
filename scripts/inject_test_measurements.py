"""Bơm bản đo thử vào aq.openaq.measurements.v1 (demo cảnh báo / replay).

Chạy trong container: docker exec -w /opt/aq spark-master python3 scripts/inject_test_measurements.py [ISO_BASE_TIME]
Cần metadata của các sensor 3916, 3917, 5001, 5002, 6001 trong aq.openaq.sensors.v1.
"""
import json, sys
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
p = Producer({"bootstrap.servers": "kafka:9092"})
base = datetime.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=10)
fmt = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")
now = fmt(datetime.now(timezone.utc))
rows = [
    (3916, 2178, 60.0, base), (3917, 2178, 0.040, base),
    (5001, 1001, 20.0, base), (5002, 1002, 120.0, base), (6001, 1003, 200.0, base),
    (3916, 2178, -5.0, base + timedelta(minutes=1)),      # NEGATIVE
    (9999, 2178, 10.0, base),                             # NO_METADATA
    (5001, 1001, 30.0, base - timedelta(hours=5)),        # STALE
]
for sid, loc, v, t in rows:
    m = {"sensor_id": sid, "location_id": loc, "value": v, "datetime_utc": fmt(t), "ingested_at": now, "source": "openaq-api"}
    p.produce("aq.openaq.measurements.v1", key=str(loc).encode(), value=json.dumps(m).encode())
print("base", fmt(base), "undelivered", p.flush(10))
