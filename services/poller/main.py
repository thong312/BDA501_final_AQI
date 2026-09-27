import os
import time
import logging
import requests
import threading
from confluent_kafka import Producer

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from services.poller.parser import parse_sensor_metadata, parse_measurements

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_KEY = os.getenv("OPENAQ_API_KEY", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SEC", "600"))
BBOX = os.getenv("NYC_BBOX", "-74.26,40.49,-73.70,40.92")

HEADERS = {"X-API-Key": API_KEY} if API_KEY else {}
TOPIC_SENSORS = "aq.openaq.sensors.v1"
TOPIC_MEASUREMENTS = "aq.openaq.measurements.v1"
TOPIC_DLQ = "aq.openaq.measurements.v1.dlq"

producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'acks': 'all',
    'enable.idempotence': True,
    'linger.ms': 50
})

location_cache = {}

def fetch_metadata():
    url = f"https://api.openaq.org/v3/locations?bbox={BBOX}&limit=1000"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        
        for loc in results:
            sensors = parse_sensor_metadata(loc)
            for s in sensors:
                producer.produce(TOPIC_SENSORS, key=str(s.sensor_id).encode('utf-8'), value=s.model_dump_json().encode('utf-8'))
            if "id" in loc and "coordinates" in loc:
                location_cache[loc["id"]] = (
                    loc["coordinates"].get("latitude", 0),
                    loc["coordinates"].get("longitude", 0)
                )
        producer.flush()
        logger.info(f"Published metadata for {len(results)} locations.")
    except Exception as e:
        logger.error(f"Error fetching metadata: {e}")

def fetch_measurements():
    if not location_cache:
        logger.warning("No locations in cache. Skipping measurement poll.")
        return

    total_ok = 0
    total_dlq = 0
    
    for loc_id, (lat, lon) in location_cache.items():
        endpoint = f"/v3/locations/{loc_id}/latest"
        url = f"https://api.openaq.org{endpoint}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 429:
                time.sleep(2)
                resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            
            results = resp.json().get("results", [])
            ok_list, dlq_list = parse_measurements(results, loc_id, lat, lon, endpoint)
            
            for m in ok_list:
                producer.produce(TOPIC_MEASUREMENTS, key=str(m.location_id).encode('utf-8'), value=m.model_dump_json().encode('utf-8'))
                total_ok += 1
                
            for d in dlq_list:
                producer.produce(TOPIC_DLQ, key=str(loc_id).encode('utf-8'), value=d.model_dump_json().encode('utf-8'))
                total_dlq += 1
                
        except Exception as e:
            pass
            
    producer.flush()
    total = total_ok + total_dlq
    ratio = total_dlq / total if total > 0 else 0
    
    logger.info(f"Measurement poll complete: ok={total_ok}, failed={total_dlq}, ratio={ratio:.2f}")
    if ratio > 0.05 or (total > 0 and total_ok == 0):
        logger.warning(f"High failure rate detected: ratio={ratio:.2f}")

def metadata_loop():
    while True:
        fetch_metadata()
        time.sleep(86400)

def measurement_loop():
    time.sleep(10)
    while True:
        fetch_measurements()
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    logger.info("Starting Poller Service...")
    t1 = threading.Thread(target=metadata_loop, daemon=True)
    t2 = threading.Thread(target=measurement_loop, daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
