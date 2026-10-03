# Giám sát và cảnh báo chất lượng không khí NYC

Hệ thống lấy dữ liệu các trạm quan trắc ở New York City từ OpenAQ, đẩy qua Kafka vào Spark Structured Streaming. Streaming vừa ghi data lake (Bronze), vừa tính AQI tức thời và phát cảnh báo. Batch hằng ngày xử lý Bronze → Silver → Gold, đối chiếu với MapReduce, rồi chạy Spark SQL + KMeans. Tầng serving là PostgreSQL (TimescaleDB + PostGIS).

Kiến trúc chi tiết: [ARCHITECTURE.md](ARCHITECTURE.md).

## Yêu cầu

- Docker Desktop (khoảng 8 GB RAM cho container)
- API key OpenAQ v3: <https://explore.openaq.org/register>
- `make` (Linux/macOS/WSL) hoặc các script PowerShell `start_streaming.ps1`, `run_batch.ps1` trên Windows

## 1. Khởi động hạ tầng (M1)

```bash
make env            # tạo .env từ .env.example, rồi điền OPENAQ_API_KEY
make up             # build image Spark/poller/notifier và chạy toàn bộ service
```

| Dịch vụ | Địa chỉ |
|---|---|
| Kafka UI | http://localhost:8080 (đổi bằng `KAFKA_UI_PORT` nếu cổng bận) |
| Spark master UI | http://localhost:8082 (job đang chạy: http://localhost:4040) |
| MinIO console | http://localhost:9001 (`minioadmin` / `minioadmin123`) |
| PostgreSQL | `localhost:5432`, db `aq`, user `aq_user` / `aq_password` |
| Kafka từ máy host | `localhost:29092` |

Container `kafka-init`, biến `MINIO_DEFAULT_BUCKETS` của MinIO và script `sql/init.sql` lần lượt tạo 4 topic, 2 bucket và schema `realtime`/`analytics`. DDL chỉ chạy khi volume Postgres còn trống; sửa DDL xong thì chạy `make reset && make up`.

## 2. Unit test (M2)

```bash
pip install pytest pyyaml python-dateutil pydantic shapely mrjob setuptools confluent-kafka requests
make test           # test thuần Python: AQI, bộ luật cảnh báo, poller, MapReduce, notifier
make test-spark     # toàn bộ test, gồm cả Spark, chạy trong container spark-master
```

Test bắt buộc theo ARCHITECTURE mục 8 và 9 nằm trong [tests/test_alert_rules.py](tests/test_alert_rules.py) và [tests/test_aqi.py](tests/test_aqi.py).

## 3. Ingestion: Poller (M3)

Service `poller` chạy cùng `make up`. Theo dõi bằng `docker logs -f poller`. Mỗi vòng đo, log in dòng `Measurement poll complete: ... ok=.. failed=.. ratio=..`.

**Chấp nhận khi:** Kafka UI hiển thị message trong `aq.openaq.sensors.v1` và `aq.openaq.measurements.v1`. Bản ghi thiếu trường hoặc sai kiểu nằm trong `aq.openaq.measurements.v1.dlq`, kèm mã lỗi như `value_not_numeric` hay `missing_sensor_id`.

## 4. Streaming: Query A + B (M4, M5)

```bash
make streaming      # hoặc .\start_streaming.ps1
```

- **Query A** ghi `s3a://aq-lake/bronze/openaq/measurements/ingest_date=.../*.json.gz` mỗi 5 phút.
- **Query B** chạy mỗi phút: join metadata, gắn quality flag, tính AQI tức thời, áp bộ luật cảnh báo cấp trạm rồi cấp vùng. State lưu trong `realtime.station_status` / `region_status`. Alert cấp vùng được publish vào `aq.alerts.level-changed.v1`.
- Một luồng nền kiểm tra mỗi 5 phút: trạm/vùng đang ≥ USG mà quá 3h không có dữ liệu sẽ chuyển `UNKNOWN`.

Kiểm tra trong Postgres:

```sql
SELECT s.name, s.borough, st.station_aqi, st.alerted_level
FROM realtime.station_status st JOIN realtime.stations s USING (location_id)
ORDER BY st.station_aqi DESC LIMIT 10;

SELECT * FROM realtime.alerts ORDER BY created_at DESC LIMIT 20;
```

**Thử cảnh báo và replay:** bơm hai bản đo PM2.5 = 60 µg/m³ cho một sensor có metadata, `datetime_utc` cách nhau vài phút và nằm trong 3h gần nhất. AQI 154 thuộc mức UNHEALTHY nên bản đầu tiên đã tạo alert ESCALATE. Bơm lại đúng các message đó thì không có alert mới: luật 1 bỏ bản đo cũ, `alert_id` là tất định.

```bash
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
  --topic aq.openaq.measurements.v1 --property parse.key=true --property key.separator='|'
2178|{"sensor_id":3916,"location_id":2178,"value":60.0,"datetime_utc":"<UTC gần đây>Z","ingested_at":"<now>Z"}
```

Kill job rồi `make streaming` lại: job tiếp tục từ checkpoint `s3a://aq-checkpoints/streaming/`.

## 5. Backfill lịch sử (M6, chạy một lần)

Đặt `BACKFILL_FROM` / `BACKFILL_TO` trong `.env`, sau đó:

```bash
make backfill
# hoặc chỉ định location: ... backfill_archive.py --from 2024-01-01 --to 2024-12-31 --locations 2178,1234
```

Job đọc ẩn danh `s3://openaq-data-archive/records/csv.gz/locationid=*/year=*/month=*/`. Danh sách location lấy từ topic metadata, nên cần poller chạy trước. Log in số dòng đọc, số dòng sau lọc bbox và dung lượng ghi thêm vào Bronze.

Sau backfill, xử lý lại lịch sử theo khoảng ngày:

```bash
docker exec -i spark-master /opt/spark/bin/spark-submit ... /opt/aq/jobs/batch/clean_bronze_to_silver.py --from 2024-01-01 --to 2024-12-31
docker exec -i spark-master /opt/spark/bin/spark-submit ... /opt/aq/jobs/batch/aggregate_silver_to_gold.py --from 2024-01-01 --to 2024-12-31
```

(Tham số `...` giống biến `SUBMIT` trong [Makefile](Makefile).)

## 6. Batch hằng ngày (M7–M11)

```bash
make batch DATE=2026-09-26        # hoặc .\run_batch.ps1 -Date 2026-09-26
```

Thứ tự chạy giống DAG Airflow [dags/aq_batch_pipeline.py](dags/aq_batch_pipeline.py): `clean → (mapreduce ∥ gold) → validate → analytics`.

| Bước | Lệnh riêng | Kết quả |
|---|---|---|
| M7 làm sạch | `make clean-silver DATE=..` | `silver/measurements/year=/month=/date_local=/`, `validation/sensor_lookup.json`, [reports/data_quality/](reports/) (số dòng trước/sau từng bước) |
| M8 MapReduce | `make mapreduce DATE=..` | `validation/mr_daily_stats/date=../part-00000` (mrjob, không dùng combiner, lý do ghi trong [daily_stats.py](jobs/mapreduce/daily_stats.py)) |
| M9 Gold | `make gold DATE=..` | `gold/{dim_station,fact_hourly,fact_daily_aqi,mart_region_daily,mart_temporal,cluster_features}`, plan trong `reports/plans/` |
| M8 đối chiếu | `make validate DATE=..` | `reports/validation/DATE.json`, exit code 1 nếu có lệch |
| M10 SQL + ML | `make analytics` | `reports/sql/*.csv`, `reports/plans/q*.txt`, `reports/ml/k_selection.csv`, `reports/ml/cluster_centers.csv`, model `models/kmeans/vN/`, bảng `analytics.*` |

**Đặt tên cụm:** xem `reports/ml/cluster_centers.csv`, sửa tên và mô tả trong [config/cluster_names.yaml](config/cluster_names.yaml) (gán theo thứ hạng `aqi_mean`), rồi chạy lại `make analytics`. Muốn chọn k khác k có silhouette cao nhất thì thêm `--k N`.

## 7. Thí nghiệm hiệu năng (M12)

```bash
make perf          # reports/perf/perf_results.csv: shuffle join / broadcast / repartition
```

## 8. Notifier (M13)

Service `notifier` đọc `aq.alerts.level-changed.v1`, in cảnh báo ra log (`docker logs -f notifier`) và gửi Telegram nếu `.env` có `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`.

## Quyết định so với tài liệu gốc

- Batch **giữ** bản ghi `STALE` (chỉ gắn flag), còn Streaming loại chúng. MapReduce dùng cùng quy tắc với Spark làm sạch.
- Kafka chỉ nhận alert **cấp vùng**; alert cấp trạm chỉ ghi vào `realtime.alerts` (`scope='station'`).
- Silver thêm cấp partition `date_local` để chạy lại một ngày là idempotent.
- MapReduce chạy bằng runner `local` của mrjob (không có cụm Hadoop); đổi sang `--runner hadoop` khi có cụm.

## Cấu trúc

```
common/      aqi.py, alert_rules.py, alerting.py, quality.py, geo.py, schemas.py, spark_utils.py, config.py
config/      aqi_breakpoints.yaml, alert_rules.yaml, quality_rules.yaml, cluster_names.yaml, nyc_boroughs.geojson
services/    poller/, notifier/
jobs/        streaming/aq_streaming.py, batch/*.py, mapreduce/daily_stats.py + run_daily_stats.py
sql/         init.sql (schema realtime + analytics)
dags/        aq_batch_pipeline.py
docker/      spark/Dockerfile (image Spark kèm thư viện Python)
tests/       unit test (+ fixtures/)
reports/     plans, validation, data_quality, sql, ml, perf (sinh khi chạy)
```
