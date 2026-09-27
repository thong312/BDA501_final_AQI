"""Spark Structured Streaming: một application, hai query (ARCHITECTURE 6.2).

Query A: ghi raw vào Bronze (JSON Lines gzip), không lọc, không sửa.
Query B: AQI tức thời + bộ luật cảnh báo, state lưu trong PostgreSQL schema realtime,
         alert cấp vùng publish vào aq.alerts.level-changed.v1 (qua outbox realtime.alerts.published_at).
"""
import json
import logging
import os
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition
from psycopg.rows import dict_row
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from common.alert_rules import CONFIG as ALERT_CFG
from common.alert_rules import check_timeout, decide, to_datetime
from common.alerting import alert_to_message, build_alert_record, region_observation
from common.aqi import compute_aqi
from common.config import BRONZE_PATH, CHECKPOINTS, TOPIC_ALERTS, TOPIC_MEASUREMENTS, TOPIC_SENSORS
from common.geo import get_borough
from common.quality import spark_quality_flag
from common.schemas import MEASUREMENT_STRUCT, SENSOR_STRUCT
from common.spark_utils import create_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger("aq_streaming")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PG_DSN = os.getenv("PG_DSN", "postgresql://aq_user:aq_password@postgres:5432/aq")

METADATA_MAX_AGE = timedelta(hours=24)
METADATA_UNKNOWN_REFRESH_GAP = timedelta(minutes=5)
TIMEOUT_CHECK_SEC = 300
TS_FMT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

# Serialize foreachBatch và luồng kiểm tra timeout trên driver (cùng ghi station/region_status)
DRIVER_LOCK = threading.Lock()


# ------------------------------------------------------------------ Query A

def build_query_a_df(raw_stream: DataFrame) -> DataFrame:
    return (raw_stream.select(
        F.col("value").cast("string").alias("value"),
        "topic", "partition", "offset", "timestamp")
        .withColumn("ingest_date", F.to_date("timestamp")))


# ------------------------------------------------------------------ Query B: parse + dedupe

def parse_measurements(raw_stream: DataFrame) -> DataFrame:
    return (raw_stream
            .select(F.from_json(F.col("value").cast("string"), MEASUREMENT_STRUCT).alias("m"))
            .select("m.sensor_id", "m.location_id", "m.value", "m.datetime_utc", "m.ingested_at")
            .withColumn("event_time", F.to_timestamp("datetime_utc"))
            .filter(F.col("sensor_id").isNotNull() & F.col("event_time").isNotNull()))


def build_query_b_df(raw_stream: DataFrame) -> DataFrame:
    return (parse_measurements(raw_stream)
            .withWatermark("event_time", "2 hours")
            .dropDuplicatesWithinWatermark(["sensor_id", "event_time"]))


@pandas_udf(DoubleType())
def aqi_instant_udf(parameter: pd.Series, value: pd.Series, units: pd.Series) -> pd.Series:
    out = [compute_aqi(p, v, u, averaging="instant") for p, v, u in zip(parameter, value, units)]
    return pd.Series([float("nan") if a is None else float(a) for a in out])


# ------------------------------------------------------------------ Metadata

META_SCHEMA = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("meta_location_id", IntegerType()),
    StructField("location_name", StringType()),
    StructField("parameter", StringType()),
    StructField("units", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("borough", StringType()),
])


class MetadataCache:
    """Metadata sensor từ topic compacted, cache trên driver, làm mới theo điều kiện 6.2 bước 1."""

    def __init__(self):
        self.df = None
        self.loaded_at = None
        self.end_offsets = None
        self.last_unknown_refresh = None
        self._consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP,
                                   "group.id": "aq-streaming-metadata-probe",
                                   "enable.auto.commit": False})

    def _topic_end_offsets(self):
        md = self._consumer.list_topics(TOPIC_SENSORS, timeout=10)
        parts = sorted(md.topics[TOPIC_SENSORS].partitions)
        return tuple(self._consumer.get_watermark_offsets(TopicPartition(TOPIC_SENSORS, p), timeout=10)[1]
                     for p in parts)

    def get(self, spark: SparkSession) -> DataFrame:
        now = datetime.now(timezone.utc)
        offsets = self._topic_end_offsets()
        if self.df is None or offsets != self.end_offsets or now - self.loaded_at > METADATA_MAX_AGE:
            self._load(spark, offsets)
        return self.df

    def refresh_for_unknown(self, spark: SparkSession) -> bool:
        """Gặp sensor_id không khớp: đọc lại, tối đa 1 lần / 5 phút."""
        now = datetime.now(timezone.utc)
        if self.last_unknown_refresh and now - self.last_unknown_refresh < METADATA_UNKNOWN_REFRESH_GAP:
            return False
        self.last_unknown_refresh = now
        self._load(spark, self._topic_end_offsets())
        return True

    def _load(self, spark: SparkSession, offsets):
        raw = (spark.read.format("kafka")
               .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
               .option("subscribe", TOPIC_SENSORS)
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
               .load())
        # Bản mới nhất mỗi key theo offset; bỏ key mà bản mới nhất là tombstone
        latest = (raw.withColumn("rn", F.row_number().over(
                      Window.partitionBy("key").orderBy(F.col("partition").desc(), F.col("offset").desc())))
                  .filter("rn = 1")
                  .filter(F.col("value").isNotNull())
                  .select(F.from_json(F.col("value").cast("string"), SENSOR_STRUCT).alias("m"))
                  .select("m.*")
                  .filter(F.col("sensor_id").isNotNull()))
        rows = latest.collect()
        # Metadata nhỏ -> gán borough trên driver bằng shapely
        records = [(r.sensor_id, r.location_id, r.location_name, r.parameter, r.units, r.lat, r.lon,
                    get_borough(r.lat, r.lon)) for r in rows]
        if self.df is not None:
            self.df.unpersist()
        self.df = spark.createDataFrame(records, META_SCHEMA).cache()
        self.df.count()
        self.loaded_at = datetime.now(timezone.utc)
        self.end_offsets = offsets
        upsert_stations(records)
        logger.info("Metadata loaded: %d sensors, end offsets %s", len(records), offsets)


def upsert_stations(records):
    stations = {}
    for sensor_id, loc_id, name, _p, _u, lat, lon, borough in records:
        if loc_id is not None:
            stations[loc_id] = (loc_id, name, lat, lon, lon, lat, borough)
    if not stations:
        return
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO realtime.stations (location_id, name, lat, lon, geom, borough, updated_at)
            VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s, now())
            ON CONFLICT (location_id) DO UPDATE SET
                name = EXCLUDED.name, lat = EXCLUDED.lat, lon = EXCLUDED.lon, geom = EXCLUDED.geom,
                borough = EXCLUDED.borough, updated_at = EXCLUDED.updated_at
        """, list(stations.values()))


METADATA = None  # khởi tạo trong main để import module (unit test) không cần Kafka


# ------------------------------------------------------------------ Postgres helpers

STATION_COLS = ("location_id", "station_aqi", "dominant_pollutant", "alerted_level", "candidate_level",
                "candidate_count", "last_event_time", "last_alert_at", "last_alert_level")


def _upsert_station_status(cur, states):
    cur.executemany(f"""
        INSERT INTO realtime.station_status ({", ".join(STATION_COLS)}, updated_at)
        VALUES ({", ".join(["%s"] * len(STATION_COLS))}, now())
        ON CONFLICT (location_id) DO UPDATE SET
            station_aqi = EXCLUDED.station_aqi, dominant_pollutant = EXCLUDED.dominant_pollutant,
            alerted_level = EXCLUDED.alerted_level, candidate_level = EXCLUDED.candidate_level,
            candidate_count = EXCLUDED.candidate_count, last_event_time = EXCLUDED.last_event_time,
            last_alert_at = EXCLUDED.last_alert_at, last_alert_level = EXCLUDED.last_alert_level,
            updated_at = EXCLUDED.updated_at
        WHERE realtime.station_status.last_event_time IS NULL
           OR EXCLUDED.last_event_time > realtime.station_status.last_event_time
    """, [tuple(s.get(c) for c in STATION_COLS) for s in states])


def _upsert_region_status(cur, borough, state, trigger_location_id):
    cur.execute("""
        INSERT INTO realtime.region_status (borough, region_level, trigger_location_id, candidate_level,
            candidate_count, last_event_time, last_alert_at, last_alert_level, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (borough) DO UPDATE SET
            region_level = EXCLUDED.region_level, trigger_location_id = EXCLUDED.trigger_location_id,
            candidate_level = EXCLUDED.candidate_level, candidate_count = EXCLUDED.candidate_count,
            last_event_time = EXCLUDED.last_event_time, last_alert_at = EXCLUDED.last_alert_at,
            last_alert_level = EXCLUDED.last_alert_level, updated_at = EXCLUDED.updated_at
        WHERE realtime.region_status.last_event_time IS NULL
           OR EXCLUDED.last_event_time > realtime.region_status.last_event_time
    """, (borough, state["alerted_level"], trigger_location_id, state.get("candidate_level"),
          state.get("candidate_count", 0), state["last_event_time"], state.get("last_alert_at"),
          state.get("last_alert_level")))


def _region_state(row):
    """Dòng region_status -> state cho decide() (region_level đóng vai alerted_level)."""
    return {"alerted_level": row["region_level"], "candidate_level": row["candidate_level"],
            "candidate_count": row["candidate_count"], "last_event_time": row["last_event_time"],
            "last_alert_at": row["last_alert_at"], "last_alert_level": row["last_alert_level"]}


def _insert_alerts(cur, records):
    cols = ("alert_id", "scope", "borough", "trigger_location_id", "aqi", "level", "prev_level", "type",
            "dominant_pollutant", "event_time", "created_at")
    cur.executemany(f"""
        INSERT INTO realtime.alerts ({", ".join(cols)}) VALUES ({", ".join(["%s"] * len(cols))})
        ON CONFLICT (alert_id) DO NOTHING
    """, [tuple(r[c] for c in cols) for r in records])


def evaluate_regions(cur, boroughs, now):
    """Bước 8: mức vùng = max alerted_level các trạm cùng borough, qua bộ luật như cấp trạm."""
    if not boroughs:
        return []
    cur.execute("SELECT * FROM realtime.region_status WHERE borough = ANY(%s) FOR UPDATE", (list(boroughs),))
    region_rows = {r["borough"]: r for r in cur.fetchall()}
    cur.execute("""
        SELECT st.location_id, s.borough, st.alerted_level, st.station_aqi, st.dominant_pollutant,
               st.last_event_time
        FROM realtime.station_status st JOIN realtime.stations s USING (location_id)
        WHERE s.borough = ANY(%s)
    """, (list(boroughs),))
    by_borough = {}
    for r in cur.fetchall():
        by_borough.setdefault(r["borough"], []).append(r)

    alerts = []
    for borough in sorted(boroughs):
        row = region_rows.get(borough) or {"region_level": 0, "candidate_level": None, "candidate_count": 0,
                                           "last_event_time": None, "last_alert_at": None,
                                           "last_alert_level": None}
        state = _region_state(row)
        obs = region_observation(by_borough.get(borough, []), state["alerted_level"], now)
        if obs is None or obs["blocked_down"]:
            continue
        new_state, alert = decide(state, obs["aqi"], obs["event_time"], level=obs["level"])
        if new_state is state:
            continue
        _upsert_region_status(cur, borough, new_state, obs["trigger_location_id"])
        if alert:
            alerts.append(build_alert_record(alert, "region", borough, obs["trigger_location_id"],
                                             obs["dominant_pollutant"]))
    return alerts


# ------------------------------------------------------------------ Kafka outbox

_producer = None


def publish_pending_alerts():
    """Publish alert cấp vùng chưa gửi (outbox). Chỉ đánh dấu published khi Kafka xác nhận."""
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all",
                              "enable.idempotence": True, "linger.ms": 50})
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT alert_id, scope, borough, trigger_location_id, aqi, level, prev_level, type,
                   dominant_pollutant, event_time, created_at
            FROM realtime.alerts WHERE scope = 'region' AND published_at IS NULL
            ORDER BY created_at FOR UPDATE SKIP LOCKED
        """)
        pending = cur.fetchall()
        if not pending:
            return
        delivered = []

        def on_delivery(err, msg):
            if err is None:
                delivered.append(msg.headers()[0][1].decode())
            else:
                logger.error("Alert publish failed: %s", err)

        for rec in pending:
            _producer.produce(TOPIC_ALERTS, key=(rec["borough"] or "").encode(),
                              value=json.dumps(alert_to_message(rec)).encode(),
                              headers=[("alert_id", rec["alert_id"].encode())], on_delivery=on_delivery)
        _producer.flush(15)
        if delivered:
            cur.execute("UPDATE realtime.alerts SET published_at = now() WHERE alert_id = ANY(%s)", (delivered,))
        logger.info("Published %d/%d region alerts", len(delivered), len(pending))


# ------------------------------------------------------------------ Timeout (luật 7)

def run_timeouts(now=None):
    """Trạm/vùng đang >= USG mà quá STALE_TIMEOUT_SEC không có dữ liệu -> UNKNOWN."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=ALERT_CFG["STALE_TIMEOUT_SEC"])
    records = []
    with DRIVER_LOCK:
        with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT st.*, s.borough FROM realtime.station_status st
                LEFT JOIN realtime.stations s USING (location_id)
                WHERE st.alerted_level >= %s AND st.last_event_time < %s FOR UPDATE OF st
            """, (ALERT_CFG["ALERT_MIN_LEVEL"], cutoff))
            for row in cur.fetchall():
                new_state, alert = check_timeout(dict(row), now)
                if alert is None:
                    continue
                cur.execute("""
                    UPDATE realtime.station_status SET alerted_level = %s, candidate_level = NULL,
                        candidate_count = 0, last_alert_at = %s, last_alert_level = %s, updated_at = now()
                    WHERE location_id = %s
                """, (new_state["alerted_level"], new_state["last_alert_at"], new_state["last_alert_level"],
                      row["location_id"]))
                records.append(build_alert_record(alert, "station", row["borough"], row["location_id"],
                                                  row["dominant_pollutant"]))

            cur.execute("""
                SELECT * FROM realtime.region_status
                WHERE region_level >= %s AND last_event_time < %s FOR UPDATE
            """, (ALERT_CFG["ALERT_MIN_LEVEL"], cutoff))
            for row in cur.fetchall():
                new_state, alert = check_timeout(_region_state(row), now)
                if alert is None:
                    continue
                cur.execute("""
                    UPDATE realtime.region_status SET region_level = %s, candidate_level = NULL,
                        candidate_count = 0, last_alert_at = %s, last_alert_level = %s, updated_at = now()
                    WHERE borough = %s
                """, (new_state["alerted_level"], new_state["last_alert_at"], new_state["last_alert_level"],
                      row["borough"]))
                records.append(build_alert_record(alert, "region", row["borough"], row["trigger_location_id"],
                                                  None))
            if records:
                _insert_alerts(cur, records)
    if records:
        logger.warning("Timeout: %d station/region went UNKNOWN", len(records))
    publish_pending_alerts()


def timeout_loop(stop_event: threading.Event):
    while not stop_event.wait(TIMEOUT_CHECK_SEC):
        try:
            run_timeouts()
        except Exception:
            logger.exception("Timeout check failed")


# ------------------------------------------------------------------ foreachBatch

def enrich_and_flag(batch_df: DataFrame, meta_df: DataFrame) -> DataFrame:
    """Bước 2–3: broadcast join metadata + quality_flag."""
    joined = batch_df.join(F.broadcast(meta_df), "sensor_id", "left")
    return joined.withColumn("quality_flag", spark_quality_flag(
        F.col("parameter"), F.col("units"), F.col("value"), F.col("event_time"), F.current_timestamp()))


def station_aqi(ok_df: DataFrame) -> DataFrame:
    """Bước 5: AQI trạm = max aqi_instant theo (location_id, event_time), kèm chất trội."""
    return (ok_df.filter(F.col("aqi_instant").isNotNull())
            .groupBy("location_id", "event_time")
            .agg(F.max("aqi_instant").alias("station_aqi"),
                 F.max_by("parameter", "aqi_instant").alias("dominant_pollutant")))


def process_batch(batch_df: DataFrame, batch_id: int):
    with DRIVER_LOCK:
        _process_batch(batch_df, batch_id)
    publish_pending_alerts()


def _process_batch(batch_df: DataFrame, batch_id: int):
    spark = batch_df.sparkSession
    batch_df = batch_df.persist()
    try:
        if batch_df.isEmpty():
            return
        now = datetime.now(timezone.utc)

        # 1–2. Metadata + join
        flagged = enrich_and_flag(batch_df, METADATA.get(spark))
        n_unmatched = flagged.filter(F.col("parameter").isNull()).count()
        if n_unmatched and METADATA.refresh_for_unknown(spark):
            flagged = enrich_and_flag(batch_df, METADATA.df)
            n_unmatched = flagged.filter(F.col("parameter").isNull()).count()
        if n_unmatched:
            logger.warning("batch %d: %d readings without metadata (NO_METADATA)", batch_id, n_unmatched)

        # 3. Data quality
        flagged = flagged.persist()
        flag_counts = {r["quality_flag"]: r["count"] for r in flagged.groupBy("quality_flag").count().collect()}
        logger.info("batch %d quality flags: %s", batch_id, flag_counts)

        # 4. AQI từng chất (pandas UDF trên executors)
        ok = (flagged.filter(F.col("quality_flag") == "OK")
              .withColumn("aqi_instant", aqi_instant_udf("parameter", "value", "units").cast("int"))
              .persist())
        readings = ok.select("location_id", "sensor_id", "parameter", "value", "units", "aqi_instant",
                             F.date_format("event_time", TS_FMT).alias("event_time"), "ingested_at").collect()
        # 5. AQI trạm, sắp theo thời gian để áp luật đúng thứ tự
        stations = (station_aqi(ok)
                    .withColumn("event_time", F.date_format("event_time", TS_FMT))
                    .orderBy("event_time", "location_id").collect())
        ok.unpersist()
        flagged.unpersist()

        # 6–9. Một transaction
        with psycopg.connect(PG_DSN, row_factory=dict_row) as conn, conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO realtime.readings (location_id, sensor_id, parameter, value, units, aqi_instant,
                                               event_time, ingested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
            """, [tuple(r) for r in readings])

            loc_ids = sorted({r["location_id"] for r in stations if r["location_id"] is not None})
            cur.execute("SELECT * FROM realtime.station_status WHERE location_id = ANY(%s) FOR UPDATE",
                        (loc_ids,))
            states = {r["location_id"]: dict(r) for r in cur.fetchall()}
            cur.execute("SELECT location_id, borough FROM realtime.stations WHERE location_id = ANY(%s)",
                        (loc_ids,))
            borough_of = {r["location_id"]: r["borough"] for r in cur.fetchall()}

            changed, alerts = {}, []
            for r in stations:
                loc = r["location_id"]
                state = states.get(loc, {"location_id": loc})
                new_state, alert = decide(state, r["station_aqi"], r["event_time"])
                if new_state is state:
                    continue  # luật 1: bản đo cũ hơn state
                new_state["station_aqi"] = r["station_aqi"]
                new_state["dominant_pollutant"] = r["dominant_pollutant"]
                states[loc] = changed[loc] = new_state
                if alert:
                    alerts.append(build_alert_record(alert, "station", borough_of.get(loc), loc,
                                                     r["dominant_pollutant"]))
            if changed:
                _upsert_station_status(cur, changed.values())

            boroughs = {borough_of.get(loc) for loc in changed} - {None}
            alerts += evaluate_regions(cur, boroughs, now)
            if alerts:
                _insert_alerts(cur, alerts)
        logger.info("batch %d: readings=%d stations=%d changed=%d alerts=%d",
                    batch_id, len(readings), len(stations), len(changed), len(alerts))
    finally:
        batch_df.unpersist()


# ------------------------------------------------------------------ main

def main():
    global METADATA
    spark = create_spark_session("AQ_Streaming_Job")
    METADATA = MetadataCache()

    raw_stream = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                  .option("subscribe", TOPIC_MEASUREMENTS)
                  .option("startingOffsets", "latest")
                  .option("maxOffsetsPerTrigger", 10000)
                  .load())

    (build_query_a_df(raw_stream).writeStream.queryName("query_a_bronze")
     .format("json").option("compression", "gzip").partitionBy("ingest_date")
     .option("path", BRONZE_PATH)
     .option("checkpointLocation", f"{CHECKPOINTS}/streaming/bronze/")
     .trigger(processingTime="5 minutes")
     .start())

    (build_query_b_df(raw_stream).writeStream.queryName("query_b_alerts")
     .foreachBatch(process_batch)
     .option("checkpointLocation", f"{CHECKPOINTS}/streaming/alerts/")
     .trigger(processingTime="1 minute")
     .start())

    stop = threading.Event()
    threading.Thread(target=timeout_loop, args=(stop,), daemon=True, name="alert-timeouts").start()
    try:
        spark.streams.awaitAnyTermination()
    finally:
        stop.set()


if __name__ == "__main__":
    main()
