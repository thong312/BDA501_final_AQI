"""Poller OpenAQ v3 -> Kafka (ARCHITECTURE 6.1).

Hai vong lap:
1. Metadata: khi khoi dong + moi 24h, publish moi sensor vao aq.openaq.sensors.v1 (key sensor_id).
2. Ban do: moi POLL_INTERVAL_SEC, /locations/{id}/latest -> measurements hoac DLQ.
"""
import logging
import os
import random
import threading
import time

import requests
from confluent_kafka import Producer

from services.poller.parser import parse_measurements, parse_sensor_metadata

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("poller")

API_BASE = "https://api.openaq.org"
API_KEY = os.getenv("OPENAQ_API_KEY", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SEC", "600"))
BBOX = os.getenv("NYC_BBOX", "-74.26,40.49,-73.70,40.92")
METADATA_INTERVAL = 24 * 3600
METADATA_RETRY_SEC = 300
MAX_RETRIES = 5
# OpenAQ gioi han 60 request/phut -> gian cach toi thieu giua 2 request
MIN_REQUEST_GAP_SEC = 1.1

TOPIC_SENSORS = "aq.openaq.sensors.v1"
TOPIC_MEASUREMENTS = "aq.openaq.measurements.v1"
TOPIC_DLQ = "aq.openaq.measurements.v1.dlq"

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 50,
})

session = requests.Session()
if API_KEY:
    session.headers["X-API-Key"] = API_KEY

# location_id -> (lat, lon); ghi boi vong metadata, doc boi vong ban do
locations = {}
locations_lock = threading.Lock()
metadata_ready = threading.Event()
_request_lock = threading.Lock()
_last_request_at = 0.0


class ApiError(Exception):
    pass


def _delivery_report(err, msg):
    if err is not None:
        logger.error("Kafka delivery failed topic=%s key=%s: %s", msg.topic(), msg.key(), err)


def _throttle():
    global _last_request_at
    with _request_lock:
        wait = _last_request_at + MIN_REQUEST_GAP_SEC - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request_at = time.monotonic()


def api_get(path, params=None):
    """GET co retry + exponential backoff cho 429/5xx/loi mang; ton trong header rate limit."""
    url = f"{API_BASE}{path}"
    for attempt in range(MAX_RETRIES):
        _throttle()
        try:
            resp = session.get(url, params=params, timeout=15)
        except requests.RequestException as e:
            delay = min(60, 2 ** attempt) + random.random()
            logger.warning("Request error %s (attempt %d): %s; retry in %.1fs", path, attempt + 1, e, delay)
            time.sleep(delay)
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            retry_after = resp.headers.get("Retry-After") or resp.headers.get("x-ratelimit-reset")
            try:
                delay = float(retry_after)
            except (TypeError, ValueError):
                delay = min(60, 2 ** attempt) + random.random()
            logger.warning("HTTP %d %s (attempt %d); retry in %.1fs", resp.status_code, path, attempt + 1, delay)
            time.sleep(delay)
            continue
        if resp.status_code >= 400:
            raise ApiError(f"HTTP {resp.status_code} {path}: {resp.text[:200]}")
        if resp.headers.get("x-ratelimit-remaining") == "0":
            reset = float(resp.headers.get("x-ratelimit-reset", "60"))
            logger.info("Rate limit exhausted, sleeping %.0fs", reset)
            time.sleep(reset)
        return resp.json()
    raise ApiError(f"Giving up after {MAX_RETRIES} attempts: {path}")


def fetch_metadata() -> bool:
    try:
        data = api_get("/v3/locations", params={"bbox": BBOX, "limit": 1000})
    except ApiError as e:
        logger.error("Metadata fetch failed: %s", e)
        return False
    results = data.get("results", [])
    new_locations = {}
    n_sensors = 0
    for loc in results:
        # Publish lai ke ca khi khong doi (topic compacted, Spark doc ban moi nhat)
        for s in parse_sensor_metadata(loc):
            producer.produce(TOPIC_SENSORS, key=str(s.sensor_id).encode(),
                             value=s.model_dump_json().encode(), on_delivery=_delivery_report)
            n_sensors += 1
        coords = loc.get("coordinates") or {}
        if loc.get("id") is not None:
            new_locations[loc["id"]] = (coords.get("latitude"), coords.get("longitude"))
    producer.flush()
    with locations_lock:
        locations.clear()
        locations.update(new_locations)
    logger.info("Published metadata: %d locations, %d sensors", len(new_locations), n_sensors)
    metadata_ready.set()
    return True


def fetch_measurements():
    with locations_lock:
        snapshot = dict(locations)
    ok = failed = http_failed = 0
    for loc_id, (lat, lon) in snapshot.items():
        endpoint = f"/v3/locations/{loc_id}/latest"
        try:
            data = api_get(endpoint)
        except ApiError as e:
            http_failed += 1
            logger.error("Measurement fetch failed %s: %s", endpoint, e)
            continue
        ok_list, dlq_list = parse_measurements(data.get("results", []), loc_id, lat, lon, endpoint)
        for m in ok_list:
            producer.produce(TOPIC_MEASUREMENTS, key=str(m.location_id).encode(),
                             value=m.model_dump_json().encode(), on_delivery=_delivery_report)
        for d in dlq_list:
            producer.produce(TOPIC_DLQ, value=d.model_dump_json().encode(), on_delivery=_delivery_report)
        ok += len(ok_list)
        failed += len(dlq_list)
        producer.poll(0)
    producer.flush()

    total = ok + failed
    ratio = failed / total if total else 0.0
    logger.info("Measurement poll complete: locations=%d ok=%d failed=%d http_failed=%d ratio=%.3f",
                len(snapshot), ok, failed, http_failed, ratio)
    if snapshot and ok == 0:
        logger.warning("100%% failure in this poll (ok=0, failed=%d, http_failed=%d)", failed, http_failed)
    elif ratio > 0.05:
        logger.warning("High DLQ ratio: %.3f", ratio)


def metadata_loop():
    while True:
        success = fetch_metadata()
        time.sleep(METADATA_INTERVAL if success else METADATA_RETRY_SEC)


def measurement_loop():
    metadata_ready.wait()
    while True:
        started = time.monotonic()
        try:
            fetch_measurements()
        except Exception:
            logger.exception("Unexpected error in measurement poll")
        time.sleep(max(0.0, POLL_INTERVAL - (time.monotonic() - started)))


def main():
    logger.info("Starting poller: kafka=%s bbox=%s interval=%ss", KAFKA_BOOTSTRAP, BBOX, POLL_INTERVAL)
    threads = [threading.Thread(target=metadata_loop, daemon=True, name="metadata"),
               threading.Thread(target=measurement_loop, daemon=True, name="measurements")]
    for t in threads:
        t.start()
    try:
        while all(t.is_alive() for t in threads):
            time.sleep(5)
        logger.error("A poller thread died, exiting so the container restarts")
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
