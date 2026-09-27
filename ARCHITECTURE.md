# Kiến trúc hệ thống giám sát và cảnh báo chất lượng không khí NYC

> Tài liệu này mô tả kiến trúc **đã chốt** và đủ chi tiết để giao cho công cụ AI code (vibecode) từng phần.
> Khi vibecode: đưa **toàn bộ file này** làm ngữ cảnh, rồi yêu cầu làm **một thành phần mỗi lần** theo thứ tự ở mục 12.

---

## 1. Tóm tắt một đoạn

Hệ thống lấy nồng độ các chất ô nhiễm (PM2.5, O₃, NO₂…) của các trạm quan trắc tại New York City từ **OpenAQ**. Mọi dữ liệu mới đi qua **một đường ingest duy nhất**: Poller gọi API → **Kafka** → **Spark Structured Streaming**. Từ Spark Streaming dữ liệu tách hai nhánh chạy song song:

- **Query A** ghi dữ liệu thô vào data lake (**Bronze** trên S3/MinIO), để phần batch xử lý tiếp **Bronze → Silver → Gold** và phân tích.
- **Query B** tính **AQI tức thời**, áp bộ luật cảnh báo, ghi trạng thái và cảnh báo vào **PostgreSQL**, đồng thời đẩy cảnh báo vào một topic Kafka cho Notifier.

Dữ liệu lịch sử được nạp **một lần** từ **OpenAQ S3 archive** vào Bronze (backfill). Tầng serving duy nhất là **PostgreSQL** (TimescaleDB + PostGIS), một database `aq` gồm hai schema `realtime` và `analytics`.

---

## 2. Sơ đồ luồng (dạng chữ)

```
                    OpenAQ API (REST v3)
                           │ ① poll: metadata mỗi ngày, bản đo mỗi 5–15 phút
                           ▼
                        Poller (Python)
                           │ ② chuẩn hoá + kiểm schema → publish
                           ▼
   ┌──────────────────────── Kafka (KRaft) ────────────────────────┐
   │ aq.openaq.measurements.v1     aq.openaq.sensors.v1            │
   │ aq.openaq.measurements.v1.dlq aq.alerts.level-changed.v1      │
   └───────────────────────────────────────────────────────────────┘
                           │ ③ readStream (measurements)
                           ▼
                 Spark Structured Streaming  (1 job, 2 query)
          ┌────────────────┴─────────────────┐
   ④ Query A: ghi raw                 ④ Query B: AQI tức thời + cảnh báo
          ▼                                   ├──► PostgreSQL schema realtime
   S3/MinIO  bronze/  ◄── backfill 1 lần       └──► Kafka aq.alerts.level-changed.v1 ──► Notifier (proposed)
          │                 từ OpenAQ S3 archive
          │   ── Batch hằng ngày (Airflow) ─────────────────────────────
          ├──► ⑤ MapReduce: thống kê ngày (để đối chiếu)
          ▼
   ⑤ Spark làm sạch  → silver/ (Parquet theo giờ)
          ▼
   ⑥ Spark tổng hợp  → gold/ (dim, fact, mart, cluster_features)  ◄─ đối chiếu với MapReduce
          ▼
   ⑦ Spark SQL + ML (KMeans) → PostgreSQL schema analytics
```

Ảnh kiến trúc: `single_flow_architecture_pg.png`.

---

## 3. Các bước thực hiện (đọc theo số)

| Bước | Ai làm | Đầu vào | Đầu ra | Tần suất |
|---|---|---|---|---|
| ① | Poller | OpenAQ API | Bản ghi JSON đã chuẩn hoá | Metadata: 1 lần/ngày + khi khởi động. Bản đo: 5–15 phút |
| ② | Poller | Bản ghi chuẩn hoá | Message vào 3 topic Kafka | Theo ① |
| ③ | Spark Streaming | `aq.openaq.measurements.v1` | DataFrame micro-batch | Liên tục |
| ④A | Spark Streaming – query A | micro-batch | `bronze/openaq/measurements/` (JSON Lines gzip) | Trigger 5 phút |
| ④B | Spark Streaming – query B | micro-batch + metadata | `realtime.*` trong Postgres + topic cảnh báo | Trigger 1 phút |
| ⑤ | Spark làm sạch | Bronze | Silver (Parquet) | Hằng ngày (Airflow) |
| ⑤ | MapReduce | Bronze | `validation/mr_daily_stats/` | Hằng ngày (Airflow) |
| ⑥ | Spark tổng hợp | Silver | Gold (Parquet) | Hằng ngày |
| ⑥' | Job đối chiếu | MR output + Gold | Báo cáo đối chiếu JSON | Hằng ngày |
| ⑦ | Spark SQL + ML | Gold | `analytics.*` trong Postgres | Hằng ngày |
| (0) | Backfill | OpenAQ S3 archive | Bronze (cùng schema) | **Chạy tay một lần** |

---

## 4. Thành phần: là gì, làm gì, vai trò

| Thành phần | Là gì | Làm gì | Vai trò trong kiến trúc |
|---|---|---|---|
| **OpenAQ API v3** | REST API dữ liệu không khí mở | Trả danh sách trạm/sensor và bản đo mới nhất | Nguồn dữ liệu realtime |
| **OpenAQ S3 archive** | Kho file csv.gz công khai, mỗi file = 1 trạm × 1 ngày | Cung cấp dữ liệu nhiều năm | Nguồn lịch sử cho backfill |
| **Poller** | Service Python chạy liên tục | Gọi API, chuẩn hoá, kiểm schema bằng pydantic, publish Kafka | Tầng ingestion. **Không** chứa logic nghiệp vụ |
| **Kafka (KRaft)** | Message broker | Lưu bền message theo partition, cho Spark đọc lại theo offset | Vùng đệm giữa nguồn và xử lý, chống mất dữ liệu |
| **Schema Registry** | Dịch vụ quản lý schema | Lưu JSON Schema của từng topic | Data contract (tuỳ chọn) |
| **Kafka UI** | Giao diện web | Xem topic, message, DLQ | Giám sát vận hành |
| **Spark Standalone** | 1 master + 2 worker (container) | Chạy job streaming và batch | Engine xử lý phân tán |
| **Spark Streaming job** | 1 application, 2 query | A: ghi raw; B: AQI + cảnh báo | Tầng xử lý gần thời gian thực |
| **MinIO** | Object storage tương thích S3 | Chứa data lake (bronze/silver/gold) và checkpoint | Lưu trữ; code dùng `s3a://` nên chuyển sang AWS S3 chỉ đổi endpoint |
| **MapReduce job** | Python (mrjob / Hadoop Streaming) | Thống kê ngày từ Bronze | Kiểm chứng độc lập kết quả Spark |
| **Spark làm sạch** | Batch job | Bronze → Silver | Chuẩn hoá, dedupe, compact file nhỏ |
| **Spark tổng hợp** | Batch job | Silver → Gold | Tính AQI ngày chuẩn EPA, dựng dim/fact/mart |
| **Spark SQL + ML** | Batch job | Truy vấn thống kê + KMeans | Phân tích; ghi kết quả vào Postgres |
| **PostgreSQL (TimescaleDB + PostGIS)** | CSDL quan hệ | Lưu trạng thái realtime, cảnh báo, kết quả phân tích | Tầng serving duy nhất |
| **Airflow** | Bộ lập lịch | Chạy các job batch hằng ngày theo thứ tự | Điều phối (có thể thay bằng Makefile/cron) |
| **Notifier** | Consumer Python | Đọc topic cảnh báo, gửi email/Telegram | Proposed, làm nếu còn thời gian |

---

## 5. Kafka topics

Quy ước tên: `<domain>.<nguồn|nhóm>.<đối tượng>.v<phiên bản>[.dlq]`, chữ thường, phân cách bằng dấu chấm.

| Topic | Producer | Consumer | Key | Partition | Cleanup | Retention |
|---|---|---|---|---|---|---|
| `aq.openaq.measurements.v1` | Poller | Spark Streaming (query A, B) | `location_id` | 6 | delete | 7 ngày |
| `aq.openaq.sensors.v1` | Poller | Spark (đọc batch, cache) | `sensor_id` | 1 | **compact** | vô hạn (compaction) |
| `aq.openaq.measurements.v1.dlq` | Poller | Con người (Kafka UI) | không | 1 | delete | 30 ngày |
| `aq.alerts.level-changed.v1` | Spark query B | Notifier | `borough` | 3 | delete | 7 ngày |

**Lý do key:** cùng key → cùng partition → giữ thứ tự theo trạm. Topic metadata bắt buộc có key vì dùng compaction.

### 5.1 Schema message

`aq.openaq.measurements.v1`
```json
{
  "sensor_id": 3916,
  "location_id": 2178,
  "value": 20.0,
  "datetime_utc": "2026-09-26T14:00:00Z",
  "datetime_local": "2026-09-26T10:00:00-04:00",
  "lat": 40.73,
  "lon": -73.82,
  "ingested_at": "2026-09-26T14:07:12Z",
  "source": "openaq-api"
}
```
Bắt buộc: `sensor_id`, `location_id`, `value` (số), `datetime_utc`. Thiếu hoặc sai kiểu → DLQ.

`aq.openaq.sensors.v1` (một message cho mỗi sensor)
```json
{
  "sensor_id": 3916,
  "location_id": 2178,
  "location_name": "Queens College",
  "parameter": "pm25",
  "units": "µg/m³",
  "lat": 40.73,
  "lon": -73.82,
  "updated_at": "2026-09-26T00:05:00Z"
}
```

`aq.openaq.measurements.v1.dlq`
```json
{
  "raw_payload": "<chuỗi JSON gốc>",
  "error": "value_not_numeric",
  "endpoint": "/v3/locations/2178/latest",
  "failed_at": "2026-09-26T14:05:12Z"
}
```

`aq.alerts.level-changed.v1`
```json
{
  "alert_id": "Queens_2026092614_UNHEALTHY",
  "scope": "region",
  "borough": "Queens",
  "trigger_location_id": 2178,
  "aqi": 156,
  "level": "UNHEALTHY",
  "prev_level": "USG",
  "type": "ESCALATE",
  "dominant_pollutant": "pm25",
  "event_time": "2026-09-26T14:00:00Z",
  "created_at": "2026-09-26T14:08:30Z"
}
```
`type ∈ {ESCALATE, RECOVERED, UNKNOWN}`. `alert_id` tạo **tất định** từ `borough + giờ event + level` để chống trùng khi replay.

---

## 6. Chi tiết từng thành phần (để vibecode)

### 6.1 Poller — `services/poller/`

**Hai vòng lặp:**

1. **Metadata** (khi khởi động + mỗi 24h):
   - `GET /v3/locations?bbox=-74.26,40.49,-73.70,40.92&limit=1000` (header `X-API-Key` từ biến môi trường).
   - Mỗi location có mảng `sensors`; làm phẳng thành **mỗi sensor một message** theo schema 5.1.
   - Publish vào `aq.openaq.sensors.v1`, key = `sensor_id`. Publish lại **kể cả khi không đổi**.
   - Lưu danh sách `location_id` NYC trong bộ nhớ để dùng cho vòng 2.
2. **Bản đo** (mỗi `POLL_INTERVAL_SEC`, mặc định 600):
   - Với mỗi `location_id`: `GET /v3/locations/{id}/latest`.
   - Mỗi phần tử kết quả → validate bằng pydantic model `Measurement`.
     - Hợp lệ → publish `aq.openaq.measurements.v1`, key = `str(location_id)`.
     - Không hợp lệ → publish `aq.openaq.measurements.v1.dlq`.
   - Sau mỗi vòng: log `ok`, `failed`, `ratio`. Cảnh báo vận hành (log WARNING) nếu `ratio > 0.05` hoặc 100% lỗi.

**Yêu cầu kỹ thuật:**
- Producer: `acks=all`, `enable.idempotence=true`, `linger.ms=50`.
- Retry có backoff khi API trả 429/5xx; tôn trọng rate limit.
- Poller **không** tính AQI, **không** lọc giá trị âm (đó là việc của Spark).
- Cấu hình qua env: `OPENAQ_API_KEY`, `KAFKA_BOOTSTRAP`, `POLL_INTERVAL_SEC`, `NYC_BBOX`.

**Chấp nhận khi:** Kafka UI thấy message ở 2 topic chính; tắt/bật poller không làm crash; gửi payload sai tay → xuất hiện trong DLQ.

### 6.2 Spark Streaming job — `jobs/streaming/aq_streaming.py`

Một `spark-submit`, hai query đọc cùng topic `aq.openaq.measurements.v1`.

**Đọc Kafka:**
```python
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribe", "aq.openaq.measurements.v1")
       .option("startingOffsets", "latest")
       .option("maxOffsetsPerTrigger", 10000)
       .load())
```

**Query A – ghi raw vào Bronze**
- Chọn `value` (chuỗi JSON), `topic`, `partition`, `offset`, `timestamp` (thời điểm Kafka nhận); thêm `ingest_date`.
- Ghi `format("json")`, `compression=gzip`, `partitionBy("ingest_date")`, path `s3a://aq-lake/bronze/openaq/measurements/`.
- Trigger `processingTime="5 minutes"`, checkpoint `s3a://aq-checkpoints/streaming/bronze/`.
- **Không** lọc, **không** sửa dữ liệu.

**Query B – AQI tức thời + cảnh báo**
- Parse JSON theo schema cố định (`from_json`).
- `withWatermark("datetime_utc", "2 hours")` + `dropDuplicatesWithinWatermark(["sensor_id", "datetime_utc"])`.
- Trigger `processingTime="1 minute"`, checkpoint `s3a://aq-checkpoints/streaming/alerts/`.
- `foreachBatch(process_batch)` làm các bước sau **trên driver + executors**:
  1. **Metadata:** `get_metadata()` đọc topic `aq.openaq.sensors.v1` bằng `spark.read` (từ `earliest`), giữ bản mới nhất mỗi `sensor_id` (theo `offset`), bỏ tombstone, **gán `borough`** bằng point-in-polygon với `config/nyc_boroughs.geojson` (shapely, chạy trên driver vì metadata nhỏ), rồi `cache()`.
     - Đọc lại khi: end offset của topic đổi, **hoặc** gặp `sensor_id` không khớp (tối đa 1 lần / 5 phút), **hoặc** quá 24h.
     - Mỗi lần đọc lại: upsert vào `realtime.stations`.
  2. **Join** `broadcast(metadata)` theo `sensor_id` (left join). Không khớp → ghi log + đếm.
  3. **Data quality:** thêm `quality_flag` ∈ `{OK, NEGATIVE, OUT_OF_RANGE, STALE, NO_METADATA}`. Ngưỡng theo `parameter` trong `config/quality_rules.yaml`. `STALE` = `datetime_utc` cũ hơn 3h so với hiện tại. Chỉ bản ghi `OK` đi tiếp.
  4. **AQI từng chất:** gọi `common.aqi.compute_aqi(parameter, value, units)` → `aqi_instant` (pandas UDF). Chất không có bảng breakpoint → `null`.
  5. **AQI trạm:** theo `(location_id, datetime_utc)`: `station_aqi = max(aqi_instant)`, `dominant_pollutant` = chất có AQI lớn nhất.
  6. **Ghi `realtime.readings`** (insert, `ON CONFLICT DO NOTHING`).
  7. **Bộ luật cảnh báo cấp trạm** (mục 8) — **state lưu trong bảng `realtime.station_status`**:
     - Đọc state của các trạm có trong batch.
     - Áp hàm thuần `common.alert_rules.decide(state, aqi, event_time)` theo thứ tự thời gian.
     - Ghi state mới bằng upsert có điều kiện `WHERE excluded.last_event_time > station_status.last_event_time`.
  8. **Cảnh báo cấp vùng:** `region_level = max(alerted_level)` của các trạm cùng `borough`. So với `realtime.region_status`; nếu đổi và qua được luật (mục 8) → tạo alert.
  9. **Ghi alert:** `realtime.alerts` (`ON CONFLICT (alert_id) DO NOTHING`) và publish `aq.alerts.level-changed.v1`.
  10. **Timeout:** trạm/vùng đang ≥ USG mà `last_event_time` cũ hơn 3h → chuyển `UNKNOWN`, tạo alert `type=UNKNOWN`, **không** gửi RECOVERED.
  - Bước 6–9 dùng `psycopg` trong **một transaction**.

> **Quyết định thiết kế:** state cảnh báo lưu trong Postgres thay vì Spark state store. Lý do: metadata cần làm mới giữa chừng (chỉ làm được trong `foreachBatch`), state xem/sửa được bằng SQL, không mất khi đổi checkpoint. Tính bất biến khi replay được đảm bảo bởi luật 1 (bỏ bản đo cũ hơn `last_event_time`) và `alert_id` tất định.

**Chấp nhận khi:** Bronze có file mới mỗi 5 phút; `realtime.station_status` cập nhật; bơm thử bản đo PM2.5 = 60 → có alert ESCALATE; bơm lại cùng bản đo → **không** có alert trùng; kill job rồi chạy lại → tiếp tục từ checkpoint.

### 6.3 Backfill — `jobs/batch/backfill_archive.py` (chạy tay 1 lần)

- Đọc csv.gz từ OpenAQ S3 archive cho khoảng thời gian `BACKFILL_FROM`–`BACKFILL_TO` (ví dụ 2–3 năm).
- Cột nguồn: `location_id, sensors_id, location, datetime, lat, lon, parameter, units, value`.
- Lọc NYC theo bbox (mục 11).
- Ghi vào **cùng thư mục Bronze**, cùng dạng JSON Lines, map sang schema measurement 5.1 và **thêm** `parameter`, `units`, `location_name` (archive có sẵn), `source="openaq-archive"`.
- Ghi log: số dòng đọc, số dòng sau lọc, dung lượng.

### 6.4 MapReduce — `jobs/mapreduce/daily_stats.py` (mrjob)

- **Input:** Bronze JSON Lines. **Chạy sau bước làm sạch** (cần `sensor_lookup.json`). Thứ tự DAG: làm sạch → (MapReduce ∥ tổng hợp) → đối chiếu → SQL + ML.
- **Mapper:** parse; áp **cùng 3 quy tắc với Spark**: lọc bbox, bỏ `value < 0` hoặc vượt ngưỡng, lấy **ngày theo giờ New York** từ `datetime_local`. Emit `key = (location_id, parameter, date_local)`, `value = (datetime_utc, value)`.
- **Reducer:** dedupe theo `datetime_utc`, tính `count`, `sum`, `max`, `hours_over_threshold`.
- **Combiner:** **không dùng** (vì cần dedupe; combiner cộng dồn sẽ đếm trùng). Ghi rõ lý do trong báo cáo.
- **Output:** `s3a://aq-lake/validation/mr_daily_stats/date=YYYY-MM-DD/`.
- Chỉ bản đo có `parameter` (từ backfill) hoặc join được metadata mới tính; nếu dữ liệu streaming chưa có `parameter` trong Bronze, mapper đọc bảng tra `sensor_id → parameter` từ file `validation/sensor_lookup.json` do job làm sạch xuất ra.

### 6.5 Spark làm sạch — `jobs/batch/clean_bronze_to_silver.py`

- Đọc Bronze của ngày cần xử lý (tham số `--date`).
- Parse JSON, ép schema; với bản ghi streaming: join metadata để có `parameter`, `units`, `location_name`.
- Lọc bbox NYC; gắn `quality_flag` như 6.2 bước 3.
- Dedupe theo `(sensor_id, datetime_utc)`, giữ bản `ingested_at` mới nhất.
- Thêm `date_local`, `hour_local`, `borough`.
- Ghi Parquet `s3a://aq-lake/silver/measurements/` `partitionBy("year","month")`, `coalesce` để mỗi partition vài file (**compact**).
- Xuất `validation/sensor_lookup.json`.
- Log số dòng trước/sau từng bước (dùng cho mục data quality của báo cáo).

### 6.6 Spark tổng hợp — `jobs/batch/aggregate_silver_to_gold.py`

Tạo các bảng Parquet trong `s3a://aq-lake/gold/`:

| Bảng | Khoá | Nội dung |
|---|---|---|
| `dim_station` | `location_id` | tên, lat, lon, borough, danh sách chất đo |
| `fact_hourly` | `location_id, parameter, hour_utc` | nồng độ theo giờ, `aqi_hourly` |
| `fact_daily_aqi` | `location_id, date_local` | AQI ngày **chuẩn EPA**: PM2.5 trung bình 24h; O₃ max của trung bình trượt 8h; `aqi_daily` = max các chất; `dominant_pollutant`; `level` |
| `mart_region_daily` | `borough, date_local` | AQI max/trung bình, số trạm, số trạm ≥ USG |
| `mart_temporal` | `borough, hour_local / dow / month` | AQI trung bình, số giờ vượt 100 |
| `cluster_features` | `location_id, date_local` | `aqi_mean, aqi_max, hours_over_100, peak_hour, pm25_o3_ratio`; chỉ ngày có ≥ 18 giờ dữ liệu |

- Join `fact_*` với `dim_station` bằng **broadcast join**; in `explain("formatted")` và lưu vào `reports/plans/`.

### 6.7 Đối chiếu — `jobs/batch/validate_mr_vs_spark.py`

- Tính lại từ Silver đúng các chỉ số của MapReduce theo cùng khoá.
- Full outer join với output MR; báo cáo: tổng số khoá, số khớp, số chỉ có một bên, sai số lớn nhất của `avg`/`max`.
- Ghi `reports/validation/YYYY-MM-DD.json`. Thất bại nếu có lệch không giải thích được.

### 6.8 Spark SQL + ML — `jobs/batch/analytics.py`

**Spark SQL (tối thiểu 2 truy vấn, lưu kết quả + `EXPLAIN FORMATTED`):**
1. Xếp hạng borough theo số ngày `aqi_daily > 100` theo năm.
2. AQI trung bình theo `borough × hour_local` (xu hướng trong ngày) và theo `month` (mùa).
3. (Thêm) Những ngày có ≥ 50% số trạm cùng ≥ USG (sự kiện diện rộng).

**KMeans:**
```
VectorAssembler → StandardScaler → KMeans(k, seed=42)
```
- Thử `k = 2..7`, ghi silhouette (`ClusteringEvaluator`) từng k vào `reports/ml/k_selection.csv`; chọn k theo silhouette + khả năng diễn giải.
- Lưu model `s3a://aq-lake/models/kmeans/vN/`.
- Xuất tâm cụm (đổi về thang gốc) để nhóm đặt tên cụm trong `config/cluster_names.yaml`.

**Ghi Postgres (JDBC):** `analytics.daily_summary`, `analytics.region_stats`, `analytics.cluster_profiles`.

---

## 7. PostgreSQL — database `aq`

Image: `timescale/timescaledb-ha` (có TimescaleDB + PostGIS). DDL trong `sql/`.

### Schema `realtime` (ghi bởi Streaming query B)

| Bảng | Loại | Cột chính | Ghi chú |
|---|---|---|---|
| `stations` | bảng | `location_id PK, name, lat, lon, geom geography(Point), borough, updated_at` | upsert khi làm mới metadata |
| `readings` | **hypertable** theo `event_time` | `location_id, sensor_id, parameter, value, units, aqi_instant, event_time, ingested_at`; PK `(sensor_id, event_time)` | retention 30 ngày |
| `station_status` | bảng | `location_id PK, station_aqi, dominant_pollutant, alerted_level, candidate_level, candidate_count, last_event_time, last_alert_at, updated_at` | **state của luật cảnh báo** |
| `region_status` | bảng | `borough PK, region_level, trigger_location_id, candidate_level, candidate_count, last_event_time, last_alert_at` | 5 dòng |
| `alerts` | bảng | `alert_id PK, scope, borough, trigger_location_id, aqi, level, prev_level, type, dominant_pollutant, event_time, created_at` | index `(borough, created_at DESC)` |
| `aqi_hourly_by_borough` | **continuous aggregate** | `borough, bucket (1h), aqi_max, aqi_avg, n_readings` | từ `readings` join `stations` |

### Schema `analytics` (ghi bởi batch)

| Bảng | Khoá | Nội dung |
|---|---|---|
| `daily_summary` | `(location_id, date_local)` | `aqi_daily, dominant_pollutant, level, cluster, cluster_name` |
| `region_stats` | `(borough, period_type, period)` | thống kê theo tháng/mùa/năm, phân bố cụm |
| `cluster_profiles` | `cluster` | tâm cụm (thang gốc), `cluster_name`, `description`, `share_of_days` |

### Truy vấn đọc lại để demo
```sql
-- Trạm nào đang ô nhiễm nhất
SELECT s.name, s.borough, st.station_aqi, st.alerted_level
FROM realtime.station_status st JOIN realtime.stations s USING (location_id)
ORDER BY st.station_aqi DESC LIMIT 10;

-- Cảnh báo 24h qua theo borough
SELECT * FROM realtime.alerts
WHERE borough = 'Bronx' AND created_at > now() - interval '24 hours'
ORDER BY created_at DESC;

-- So sánh AQI tức thời và AQI ngày chuẩn EPA
SELECT d.location_id, d.date_local, d.aqi_daily, max(r.aqi_instant) AS max_instant
FROM analytics.daily_summary d
JOIN realtime.readings r ON r.location_id = d.location_id
 AND (r.event_time AT TIME ZONE 'America/New_York')::date = d.date_local
GROUP BY 1,2,3;
```

---

## 8. Bộ luật cảnh báo — `common/alert_rules.py`

Viết thành **hàm thuần Python** (không phụ thuộc Spark) để unit test: `decide(state, aqi, event_time) -> (new_state, alert | None)`.

**Mức AQI (EPA):** 0 Good (0–50), 1 Moderate (51–100), 2 USG (101–150), 3 Unhealthy (151–200), 4 Very Unhealthy (201–300), 5 Hazardous (301+).

| # | Luật | Chi tiết |
|---|---|---|
| 1 | Chỉ xét dữ liệu mới | `event_time <= last_event_time` → bỏ qua |
| 2 | Ngưỡng cảnh báo | Chỉ phát alert khi mức mới ≥ USG (2). Good ↔ Moderate chỉ cập nhật state |
| 3 | Tăng lên USG cần xác nhận | Cần `CONFIRM_N` lần đo liên tiếp ở mức mới |
| 4 | Tăng lên ≥ Unhealthy báo ngay | Bỏ qua xác nhận; nhảy nhiều mức thì báo mức cao nhất |
| 5 | Giảm mức có hysteresis | AQI ≤ `ngưỡng dưới của mức hiện tại − HYST`, liên tiếp `CONFIRM_N` lần. Chỉ gửi `RECOVERED` khi về dưới USG |
| 6 | Cooldown | Không phát lại cùng mức trong `COOLDOWN_SEC`; tăng mức thì vẫn báo ngay |
| 7 | Mất dữ liệu | Quá `STALE_TIMEOUT_SEC` không có dữ liệu khi đang ≥ USG → `UNKNOWN`, không gửi `RECOVERED` |

Áp dụng **cho cả cấp trạm và cấp vùng**. Mức vùng = max `alerted_level` các trạm trong vùng.

Tham số trong `config/alert_rules.yaml`:
```yaml
CONFIRM_N: 2
HYST: 10
COOLDOWN_SEC: 3600
STALE_TIMEOUT_SEC: 10800
ALERT_MIN_LEVEL: 2
URGENT_MIN_LEVEL: 3
```

**Unit test bắt buộc:** tăng dần qua ngưỡng; dao động quanh 100 không spam; nhảy thẳng lên 180; giảm về 95 không RECOVERED (chưa qua hysteresis); giảm về 85 hai lần → RECOVERED; bản đo trễ bị bỏ qua; mất dữ liệu 3h → UNKNOWN.

---

## 9. Tính AQI — `common/aqi.py`

- **Một module dùng chung** cho streaming (AQI tức thời) và batch (AQI ngày). Chỉ khác **đầu vào** (bản đo đơn lẻ vs trung bình theo cửa sổ).
- Công thức nội suy:
  ```
  AQI = (I_hi − I_lo) / (BP_hi − BP_lo) × (C − BP_lo) + I_lo
  ```
- Bảng breakpoint để trong `config/aqi_breakpoints.yaml`, **lấy từ tài liệu kỹ thuật chính thức của EPA** (AQI Technical Assistance Document) cho: PM2.5 (µg/m³, bản cập nhật 2024), PM10, O₃ (ppm, 8h và 1h), NO₂ (ppb), SO₂ (ppb), CO (ppm). Ví dụ PM2.5:

  | Nồng độ | AQI |
  |---|---|
  | 0.0–9.0 | 0–50 |
  | 9.1–35.4 | 51–100 |
  | 35.5–55.4 | 101–150 |
  | 55.5–125.4 | 151–200 |
  | 125.5–225.4 | 201–300 |
  | ≥ 225.5 | 301+ |

- Làm tròn/cắt nồng độ đúng số chữ số theo EPA trước khi tra bảng; quy đổi đơn vị nếu OpenAQ trả khác đơn vị trong bảng.
- Tên cột **bắt buộc phân biệt**: `aqi_instant` (streaming, từ một bản đo) và `aqi_daily` (batch, chuẩn EPA).
- **Test:** PM2.5 = 20.0 → 71; giá trị đúng ngưỡng 9.0 → 50, 9.1 → 51.

---

## 10. Bố cục data lake (MinIO / S3)

```
s3a://aq-lake/
  bronze/openaq/measurements/ingest_date=YYYY-MM-DD/*.json.gz   (streaming + backfill, bất biến)
  silver/measurements/year=YYYY/month=MM/*.parquet
  gold/dim_station/  gold/fact_hourly/  gold/fact_daily_aqi/
  gold/mart_region_daily/  gold/mart_temporal/  gold/cluster_features/
  validation/mr_daily_stats/date=YYYY-MM-DD/
  validation/sensor_lookup.json
  models/kmeans/vN/
s3a://aq-checkpoints/
  streaming/bronze/     streaming/alerts/
```
Nguyên tắc: Bronze **không bao giờ sửa**; mọi lỗi sửa bằng cách chạy lại batch từ Bronze.

---

## 11. Hằng số và cấu hình

| Tên | Giá trị | Dùng ở |
|---|---|---|
| `NYC_BBOX` | lat 40.49 → 40.92, lon −74.26 → −73.70 | Poller, backfill, MapReduce, Spark làm sạch |
| `TIMEZONE` | `America/New_York` | Mọi phép tính theo ngày/giờ địa phương |
| `config/nyc_boroughs.geojson` | ranh giới 5 borough (NYC Open Data), file tham chiếu tĩnh | gán `borough` |
| `config/quality_rules.yaml` | ngưỡng hợp lệ theo `parameter` | Streaming, làm sạch, MapReduce |
| Biến môi trường | `OPENAQ_API_KEY, KAFKA_BOOTSTRAP, S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, PG_DSN` | Mọi service. **Không commit** giá trị thật; có `.env.example` |

---

## 12. Thứ tự xây dựng (dùng khi vibecode)

Mỗi mốc là **một phiên vibecode**. Chỉ sang mốc tiếp theo khi mốc trước đạt "chấp nhận khi".

| Mốc | Làm gì | Chấp nhận khi |
|---|---|---|
| M1 | `docker-compose.yml` + `.env.example` + script tạo bucket/topic/DDL | `make up` chạy đủ service; Kafka UI, Spark UI, MinIO console, Postgres truy cập được |
| M2 | `common/aqi.py`, `common/alert_rules.py` + unit test | `pytest` xanh với các test ở mục 8, 9 |
| M3 | Poller | message xuất hiện đúng 3 topic |
| M4 | Streaming query A | file JSON Lines xuất hiện trong Bronze |
| M5 | Streaming query B | `station_status`, `alerts` cập nhật; test replay không tạo alert trùng |
| M6 | Backfill | Bronze có dữ liệu lịch sử, log số dòng |
| M7 | Spark làm sạch | Silver có Parquet, log số dòng trước/sau |
| M8 | MapReduce + đối chiếu | báo cáo đối chiếu 0 lệch không giải thích được |
| M9 | Spark tổng hợp | các bảng Gold; lưu query plan |
| M10 | Spark SQL + KMeans + ghi `analytics.*` | bảng silhouette, `cluster_profiles`, kết quả truy vấn |
| M11 | Airflow DAG (hoặc Makefile) | chạy M7→M10 bằng một lệnh |
| M12 | Thí nghiệm hiệu năng | bảng thời gian chạy theo số partition hoặc broadcast bật/tắt |
| M13 | Notifier (tuỳ chọn) | cảnh báo in ra console / gửi Telegram |

---

## 13. Docker Compose services

| Service | Image gợi ý | Cổng |
|---|---|---|
| `kafka` | `apache/kafka` (KRaft, không ZooKeeper) | 9092 |
| `schema-registry` (tuỳ chọn) | `confluentinc/cp-schema-registry` | 8081 |
| `kafka-ui` | `provectuslabs/kafka-ui` | 8080 |
| `spark-master` | `bitnami/spark:3.5` | 7077, 8082 (UI) |
| `spark-worker-1`, `spark-worker-2` | `bitnami/spark:3.5` (2 core, 2 GB mỗi worker) | – |
| `minio` + `minio-init` | `minio/minio`, `minio/mc` | 9000, 9001 |
| `postgres` | `timescale/timescaledb-ha` | 5432 |
| `poller` | build từ `services/poller/` | – |
| `airflow` (tuỳ chọn) | `apache/airflow` | 8083 |

Spark cần package: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.x`, `org.apache.hadoop:hadoop-aws`, `org.postgresql:postgresql`. **Ghim phiên bản** trong `requirements.txt` và lệnh `spark-submit`.

---

## 14. Cấu trúc repo

```
.
├── ARCHITECTURE.md            (file này)
├── README.md                  (lệnh chạy từ đầu đến kết quả)
├── docker-compose.yml
├── .env.example
├── Makefile
├── config/
│   ├── aqi_breakpoints.yaml
│   ├── alert_rules.yaml
│   ├── quality_rules.yaml
│   ├── cluster_names.yaml
│   └── nyc_boroughs.geojson
├── common/                    (dùng chung poller + Spark)
│   ├── aqi.py
│   ├── alert_rules.py
│   ├── schemas.py             (pydantic + Spark StructType)
│   └── geo.py                 (bbox, gán borough)
├── services/
│   ├── poller/
│   └── notifier/
├── jobs/
│   ├── streaming/aq_streaming.py
│   ├── batch/backfill_archive.py
│   ├── batch/clean_bronze_to_silver.py
│   ├── batch/aggregate_silver_to_gold.py
│   ├── batch/validate_mr_vs_spark.py
│   ├── batch/analytics.py
│   └── mapreduce/daily_stats.py
├── sql/                       (DDL schema realtime, analytics)
├── dags/                      (Airflow)
├── tests/
└── reports/                   (query plan, validation, ml, perf)
```

---

## 15. Quyết định thiết kế và rủi ro đã biết

| Chủ đề | Quyết định | Lý do / rủi ro |
|---|---|---|
| Kiến trúc | Một đường ingest qua Kafka vào data lake (kiểu Kappa) | Đơn giản; batch đọc lại từ Bronze |
| Cảnh báo | Query B trong cùng job streaming | Không phải chờ batch |
| Lịch sử | Backfill một lần từ OpenAQ S3 | API chỉ có dữ liệu từ lúc chạy |
| Hai loại AQI | `aqi_instant` (streaming) và `aqi_daily` (batch, chuẩn EPA) | Streaming không có đủ dữ liệu cả ngày; dữ liệu trễ; backfill không đi qua streaming |
| MapReduce | Kiểm chứng độc lập, không dùng combiner | Yêu cầu của đề; dedupe làm combiner cộng dồn không an toàn |
| ML | KMeans phân cụm "kiểu ngày ô nhiễm" | Không cần nhãn; dự báo để future work |
| Serving | Chỉ PostgreSQL (TimescaleDB + PostGIS) | **Rủi ro:** đề yêu cầu NoSQL serving → cần hỏi giảng viên và giải thích trong báo cáo |
| State cảnh báo | Lưu trong Postgres | Làm mới metadata trong `foreachBatch`; state truy vấn được |
| Cluster | Spark Standalone trên một máy | Không phải cluster thật; ghi rõ trong báo cáo |
| Phân vùng | Borough từ file geojson tĩnh | Mỗi borough chỉ vài trạm, AQI vùng mang tính đại diện hạn chế |

---

## 16. Mẫu prompt vibecode

```
Ngữ cảnh: [dán toàn bộ ARCHITECTURE.md]

Nhiệm vụ: thực hiện mốc [Mx – tên] trong mục 12.
- Chỉ tạo/sửa các file thuộc thành phần này theo cấu trúc repo ở mục 14.
- Tuân thủ đúng tên topic, schema message, tên bảng, tên cột trong tài liệu.
- Cấu hình đọc từ biến môi trường / config/*.yaml, không hardcode secret.
- Logic nghiệp vụ tách thành hàm thuần để viết unit test.
- Kèm: lệnh chạy, cách kiểm tra "chấp nhận khi", và cập nhật README.
- Nếu tài liệu mơ hồ hoặc mâu thuẫn, hỏi lại trước khi code.
```
