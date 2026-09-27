-- Kích hoạt PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- Tạo các schema
CREATE SCHEMA IF NOT EXISTS realtime;
CREATE SCHEMA IF NOT EXISTS analytics;

-- 1. Schema realtime
CREATE TABLE realtime.stations (
    location_id INT PRIMARY KEY,
    name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    geom GEOGRAPHY(Point),
    borough TEXT,
    updated_at TIMESTAMPTZ
);

CREATE TABLE realtime.readings (
    location_id INT,
    sensor_id INT,
    parameter TEXT,
    value DOUBLE PRECISION,
    units TEXT,
    aqi_instant INT,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ,
    PRIMARY KEY (sensor_id, event_time)
);
-- Chuyển thành hypertable của TimescaleDB
SELECT create_hypertable('realtime.readings', 'event_time');

CREATE TABLE realtime.station_status (
    location_id INT PRIMARY KEY,
    station_aqi INT,
    dominant_pollutant TEXT,
    alerted_level INT,
    candidate_level INT,
    candidate_count INT,
    last_event_time TIMESTAMPTZ,
    last_alert_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE realtime.region_status (
    borough TEXT PRIMARY KEY,
    region_level INT,
    trigger_location_id INT,
    candidate_level INT,
    candidate_count INT,
    last_event_time TIMESTAMPTZ,
    last_alert_at TIMESTAMPTZ
);

CREATE TABLE realtime.alerts (
    alert_id TEXT PRIMARY KEY,
    scope TEXT,
    borough TEXT,
    trigger_location_id INT,
    aqi INT,
    level TEXT,
    prev_level TEXT,
    type TEXT,
    dominant_pollutant TEXT,
    event_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ
);
CREATE INDEX idx_alerts_borough_created_at ON realtime.alerts(borough, created_at DESC);

-- Continuous aggregate cho AQI trung bình theo giờ
CREATE MATERIALIZED VIEW realtime.aqi_hourly_by_borough
WITH (timescaledb.continuous) AS
SELECT 
    s.borough,
    time_bucket('1 hour', r.event_time) AS bucket,
    MAX(r.aqi_instant) AS aqi_max,
    AVG(r.aqi_instant) AS aqi_avg,
    COUNT(*) AS n_readings
FROM realtime.readings r
JOIN realtime.stations s ON r.location_id = s.location_id
GROUP BY s.borough, time_bucket('1 hour', r.event_time)
WITH NO DATA;

-- 2. Schema analytics
CREATE TABLE analytics.daily_summary (
    location_id INT,
    date_local DATE,
    aqi_daily INT,
    dominant_pollutant TEXT,
    level TEXT,
    cluster INT,
    cluster_name TEXT,
    PRIMARY KEY (location_id, date_local)
);

CREATE TABLE analytics.region_stats (
    borough TEXT,
    period_type TEXT,
    period TEXT,
    aqi_avg DOUBLE PRECISION,
    days_over_100 INT,
    PRIMARY KEY (borough, period_type, period)
);

CREATE TABLE analytics.cluster_profiles (
    cluster INT PRIMARY KEY,
    cluster_name TEXT,
    description TEXT,
    share_of_days DOUBLE PRECISION,
    pm25_mean DOUBLE PRECISION,
    o3_mean DOUBLE PRECISION
);
