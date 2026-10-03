"""AQ Dashboard - Flask API + Static HTML server."""

import os
import json
from datetime import datetime, timezone

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import psycopg
from psycopg.rows import dict_row

app = Flask(__name__, static_folder="static")
CORS(app)

PG_DSN = os.getenv("PG_DSN", "postgresql://aq_user:aq_password@postgres:5432/aq")

def get_conn():
    return psycopg.connect(PG_DSN, row_factory=dict_row)

@app.route(/)
def index():
    return send_from_directory("static", "index.html")

@app.route(/<path:path>)
def static_files(path):
    return send_from_directory("static", path)

@app.route(/api/overview)
def overview():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT count(*) AS cnt FROM realtime.station_status WHERE last_event_time IS NOT NULL")
        n_stations = cur.fetchone()["cnt"]
        cur.execute("SELECT count(*) AS cnt FROM realtime.readings")
        n_readings = cur.fetchone()["cnt"]
        cur.execute("SELECT count(*) AS cnt FROM realtime.alerts WHERE scope = 'region'")
        n_alerts = cur.fetchone()["cnt"]
        cur.execute("SELECT COALESCE(round(avg(station_aqi)), 0) AS avg_aqi, COALESCE(max(station_aqi), 0) AS max_aqi FROM realtime.station_status WHERE station_aqi IS NOT NULL")
        aqi_row = cur.fetchone()
        cur.execute("SELECT count(*) AS cnt FROM analytics.daily_summary")
        n_daily = cur.fetchone()["cnt"]
        cur.execute("SELECT min(event_time) AS first_ts, max(event_time) AS last_ts FROM realtime.readings")
        ts_row = cur.fetchone()
        return jsonify({
            "stations": n_stations, "readings": n_readings, "alerts": n_alerts,
            "avg_aqi": int(aqi_row["avg_aqi"]), "max_aqi": int(aqi_row["max_aqi"]),
            "daily_records": n_daily,
            "first_reading": ts_row["first_ts"].isoformat() if ts_row["first_ts"] else None,
            "last_reading": ts_row["last_ts"].isoformat() if ts_row["last_ts"] else None,
        })

@app.route(/api/stations)
def stations():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT s.location_id, s.name, s.lat, s.lon, s.borough, ss.station_aqi, ss.dominant_pollutant, ss.alerted_level, ss.last_event_time FROM realtime.stations s JOIN realtime.station_status ss ON s.location_id = ss.location_id WHERE ss.last_event_time IS NOT NULL ORDER BY s.borough, s.name")
        rows = cur.fetchall()
        for r in rows:
            if r.get("last_event_time"):
                r["last_event_time"] = r["last_event_time"].isoformat()
        return jsonify(rows)

@app.route(/api/readings)
def readings():
    try:
        hours = int(request.args.get("hours", 24))
        if hours <= 0: hours = 24
    except ValueError:
        hours = 24
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT r.location_id, s.name, s.borough, r.parameter, r.value, r.units, r.aqi_instant, r.event_time FROM realtime.readings r JOIN realtime.stations s ON r.location_id = s.location_id WHERE r.event_time >= now() - make_interval(hours => %(h)s) ORDER BY r.event_time DESC LIMIT 500", {"h": hours})
        rows = cur.fetchall()
        for r in rows:
            if r.get("event_time"):
                r["event_time"] = r["event_time"].isoformat()
        return jsonify(rows)

@app.route(/api/aqi_hourly)
def aqi_hourly():
    try:
        hours = int(request.args.get("hours", 72))
        if hours <= 0: hours = 72
    except ValueError:
        hours = 72
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT borough, bucket, aqi_max, aqi_avg::int AS aqi_avg, n_readings FROM realtime.aqi_hourly_by_borough WHERE bucket >= now() - make_interval(hours => %(h)s) ORDER BY bucket", {"h": hours})
        rows = cur.fetchall()
        for r in rows:
            if r.get("bucket"):
                r["bucket"] = r["bucket"].isoformat()
        return jsonify(rows)

@app.route(/api/alerts)
def alerts():
    try:
        limit = int(request.args.get("limit", 50))
        if limit <= 0: limit = 50
    except ValueError:
        limit = 50
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT alert_id, scope, borough, trigger_location_id, aqi, level, prev_level, type, dominant_pollutant, event_time, created_at FROM realtime.alerts WHERE scope = 'region' ORDER BY created_at DESC LIMIT %(lim)s", {"lim": limit})
        rows = cur.fetchall()
        for r in rows:
            for k in ("event_time", "created_at"):
                if r.get(k):
                    r[k] = r[k].isoformat()
        return jsonify(rows)

@app.route(/api/daily_summary)
def daily_summary():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT location_id, date_local, borough, aqi_daily, dominant_pollutant, level, cluster, cluster_name FROM analytics.daily_summary WHERE borough IS NOT NULL ORDER BY date_local DESC, borough LIMIT 1000")
        rows = cur.fetchall()
        for r in rows:
            if r.get("date_local"):
                r["date_local"] = r["date_local"].isoformat()
        return jsonify(rows)

@app.route(/api/cluster_profiles)
def cluster_profiles():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT cluster, cluster_name, description, share_of_days, n_days, round(aqi_mean::numeric, 1) AS aqi_mean, round(aqi_max::numeric, 1) AS aqi_max, round(hours_over_100::numeric, 1) AS hours_over_100, round(peak_hour::numeric, 1) AS peak_hour, round(pm25_o3_ratio::numeric, 2) AS pm25_o3_ratio FROM analytics.cluster_profiles ORDER BY cluster")
        rows = cur.fetchall()
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "as_integer_ratio"):
                    r[k] = float(v)
        return jsonify(rows)

@app.route(/api/region_stats)
def region_stats():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT borough, period_type, period, n_station_days, round(aqi_avg::numeric, 1) AS aqi_avg, aqi_max, days_over_100, cluster_distribution FROM analytics.region_stats ORDER BY borough, period_type, period")
        rows = cur.fetchall()
        for r in rows:
            if r.get("aqi_avg"):
                r["aqi_avg"] = float(r["aqi_avg"])
        return jsonify(rows)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=False)