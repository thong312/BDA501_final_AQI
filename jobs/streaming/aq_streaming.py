import os
import json
import logging
from datetime import datetime, timezone
import yaml
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

import sys
sys.path.append(str(Path(__file__).parent.parent.parent))

from common.aqi import compute_aqi
from common.alert_rules import decide
from common.geo import get_borough
import psycopg2

logger = logging.getLogger(__name__)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PG_DSN = os.getenv("PG_DSN", "postgresql://aq_user:aq_password@postgres:5432/aq")

# Load rules
config_path = Path(__file__).parent.parent.parent / "config" / "quality_rules.yaml"
try:
    with open(config_path, "r") as f:
        QUALITY_RULES = yaml.safe_load(f)
except:
    QUALITY_RULES = {}

def build_query_a_df(raw_stream):
    return raw_stream.select(
        col("value").cast("string").alias("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
    ).withColumn("ingest_date", to_date(col("timestamp")))

measurements_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("value", DoubleType(), True),
    StructField("datetime_utc", StringType(), True),
    StructField("datetime_local", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("source", StringType(), True)
])

def build_query_b_df(raw_stream):
    """ Hàm thuần chuẩn bị DF cho query B """
    df = raw_stream.select(from_json(col("value").cast("string"), measurements_schema).alias("data")).select("data.*")
    df = df.withColumn("datetime_utc_ts", to_timestamp(col("datetime_utc")))
    df = df.withWatermark("datetime_utc_ts", "2 hours").dropDuplicates(["sensor_id", "datetime_utc_ts"])
    return df

metadata_cache = {}
last_metadata_update = None

def get_metadata(spark):
    global metadata_cache, last_metadata_update
    df = (spark.read.format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
          .option("subscribe", "aq.openaq.sensors.v1")
          .option("startingOffsets", "earliest")
          .load())
    records = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").collect()
    new_cache = {}
    for r in records:
        if r.value:
            data = json.loads(r.value)
            sensor_id = data.get("sensor_id")
            data["borough"] = get_borough(data.get("lat"), data.get("lon"))
            new_cache[sensor_id] = data
    metadata_cache = new_cache
    last_metadata_update = datetime.now()
    return new_cache

def process_batch(batch_df, batch_id):
    spark = batch_df.sparkSession
    global metadata_cache, last_metadata_update
    if not metadata_cache or (datetime.now() - last_metadata_update).total_seconds() > 86400:
        get_metadata(spark)
        
    records = batch_df.collect()
    if not records:
        return
        
    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()
        for row in records:
            sensor_id = row.sensor_id
            val = row.value
            dt_utc = row.datetime_utc
            
            meta = metadata_cache.get(sensor_id)
            if not meta:
                continue
                
            parameter = meta.get("parameter")
            units = meta.get("units")
            loc_id = meta.get("location_id")
            
            rules = QUALITY_RULES.get(parameter, {})
            q_flag = "OK"
            if val is None or val < 0:
                q_flag = "NEGATIVE"
            elif rules and (val < rules.get("min", -999) or val > rules.get("max", 99999)):
                q_flag = "OUT_OF_RANGE"
                
            if q_flag != "OK":
                continue
                
            aqi_inst = compute_aqi(parameter, val, units)
            
            try:
                cur.execute("""
                    INSERT INTO realtime.readings 
                    (location_id, sensor_id, parameter, value, units, aqi_instant, event_time, ingested_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (loc_id, sensor_id, parameter, val, units, aqi_inst, dt_utc, row.ingested_at))
            except Exception as e:
                pass
                
            if aqi_inst is not None:
                cur.execute("SELECT station_aqi, dominant_pollutant, alerted_level, candidate_level, candidate_count, last_event_time, last_alert_at FROM realtime.station_status WHERE location_id = %s", (loc_id,))
                status = cur.fetchone()
                state = {}
                if status:
                    state = {
                        "station_aqi": status[0],
                        "dominant_pollutant": status[1],
                        "alerted_level": status[2],
                        "candidate_level": status[3],
                        "candidate_count": status[4],
                        "last_event_time": status[5].isoformat() if status[5] else None,
                        "last_alert_at": status[6].isoformat() if status[6] else None
                    }
                new_state, alert = decide(state, aqi_inst, dt_utc)
                if new_state.get("last_event_time") != state.get("last_event_time"):
                    cur.execute("""
                        INSERT INTO realtime.station_status 
                        (location_id, station_aqi, dominant_pollutant, alerted_level, candidate_level, candidate_count, last_event_time, last_alert_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (location_id) DO UPDATE SET
                            station_aqi = EXCLUDED.station_aqi, dominant_pollutant = EXCLUDED.dominant_pollutant,
                            alerted_level = EXCLUDED.alerted_level, candidate_level = EXCLUDED.candidate_level,
                            candidate_count = EXCLUDED.candidate_count, last_event_time = EXCLUDED.last_event_time,
                            last_alert_at = EXCLUDED.last_alert_at, updated_at = EXCLUDED.updated_at
                        WHERE EXCLUDED.last_event_time > realtime.station_status.last_event_time
                    """, (loc_id, new_state.get("station_aqi", aqi_inst), parameter, new_state.get("alerted_level", 0), new_state.get("candidate_level", 0), new_state.get("candidate_count", 0), new_state.get("last_event_time"), new_state.get("last_alert_at")))
                
                if alert:
                    alert_id = f"{meta['borough']}_{alert['event_time'][:13]}_{alert['level']}"
                    cur.execute("""
                        INSERT INTO realtime.alerts 
                        (alert_id, scope, borough, trigger_location_id, aqi, level, prev_level, type, dominant_pollutant, event_time, created_at)
                        VALUES (%s, 'region', %s, %s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (alert_id) DO NOTHING
                    """, (alert_id, meta["borough"], loc_id, alert["aqi"], alert["level"], alert["prev_level"], alert["type"], parameter, alert["event_time"]))
                    
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Batch Error: {e}")

def create_spark_session():
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:4566")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "test")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "test")
    return (SparkSession.builder
            .appName("AQ_Streaming_Job")
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.streaming.checkpointLocation", "s3a://aq-checkpoints/streaming/")
            .getOrCreate())

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    raw_stream = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", "aq.openaq.measurements.v1")
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", 10000)
            .load())
            
    df_a = build_query_a_df(raw_stream)
    query_a = (df_a.writeStream.queryName("query_a_bronze")
            .format("json").option("compression", "gzip").partitionBy("ingest_date")
            .option("path", "s3a://aq-lake/bronze/openaq/measurements/")
            .option("checkpointLocation", "s3a://aq-checkpoints/streaming/bronze/")
            .trigger(processingTime="5 minutes")
            .start())
            
    df_b = build_query_b_df(raw_stream)
    query_b = (df_b.writeStream.queryName("query_b_alerts")
            .foreachBatch(process_batch)
            .option("checkpointLocation", "s3a://aq-checkpoints/streaming/alerts/")
            .trigger(processingTime="1 minute")
            .start())
            
    spark.streams.awaitAnyTermination()
