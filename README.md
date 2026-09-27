# Hệ thống giám sát và cảnh báo chất lượng không khí NYC

Dự án này thu thập, xử lý và phân tích dữ liệu chất lượng không khí tại New York City từ OpenAQ.

## M1: Khởi tạo cơ sở hạ tầng

Bao gồm Kafka, Spark, MinIO, và PostgreSQL (TimescaleDB + PostGIS).

### 1. Chuẩn bị biến môi trường
Tạo file `.env` từ `.env.example`:
```bash
make env
# Hoặc cp .env.example .env (trên Windows)
```

### 2. Khởi động môi trường
Sử dụng lệnh sau để khởi chạy tất cả các dịch vụ qua Docker Compose:
```bash
docker-compose up -d
# Hoặc make up
```

### 3. Kiểm tra (Chấp nhận khi)
Đảm bảo bạn có thể truy cập các giao diện sau:
- **Kafka UI**: [http://localhost:8080](http://localhost:8080) 
- **Spark UI**: [http://localhost:8082](http://localhost:8082)
- **MinIO**: `http://localhost:9000`
- **Postgres**: Kết nối qua cổng `localhost:5432` với user `aq_user` và password `aq_password`.

## M2: Logic tính AQI và Cảnh báo
Cài đặt 2 module lõi: `common/aqi.py` và `common/alert_rules.py`. Các luật cảnh báo được xây dựng dưới dạng hàm thuần, bao gồm hystersis, cooldown, nhảy mức.
**Cách kiểm tra:** Chạy `pytest tests/test_aqi.py tests/test_alert_rules.py` để xác nhận mọi logic đúng chuẩn EPA.

## M3: Ingest Data - Poller Service
Dịch vụ Python (`services/poller/main.py`) chạy 2 vòng lặp ngầm:
1. Giao tiếp với OpenAQ API để lấy cấu hình các trạm mỗi 24h và ghi vào Kafka `aq.openaq.sensors.v1`.
2. Lấy số đo chất lượng không khí mới nhất của từng trạm (mặc định mỗi 10 phút) và đẩy thẳng vào topic `aq.openaq.measurements.v1`.
- Các bản ghi thiếu cấu trúc chuẩn bị đẩy sang `aq.openaq.measurements.v1.dlq`.
- Dữ liệu chuẩn bị qua hàm thuần `parser.py` để validate (Dùng Pydantic Schema).

### Cách kiểm tra (Chấp nhận khi)
- Khi service `poller` trên Docker lên, mở **Kafka UI** ([http://localhost:8080](http://localhost:8080)).
- Vào mục **Topics**, chọn `aq.openaq.measurements.v1` và `aq.openaq.sensors.v1`.
- Chuyển sang tab **Messages**, bạn sẽ thấy các message JSON được đẩy vào liên tục mỗi 10 phút.
- Gửi payload cố tình sai lên topic bằng tay, message lỗi sẽ được xuất hiện trong topic `dlq`.
- Log của poller: `docker logs poller -f` (Không được in ra lỗi crash, phải có "Measurement poll complete").

## M4: Spark Streaming - Query A
Ứng dụng Spark Structured Streaming đọc message liên tục từ topic `aq.openaq.measurements.v1`. Tại mốc này, `Query A` lấy dữ liệu thô (raw) và ghi xuống Data Lake (MinIO) vào thư mục `bronze` dưới định dạng JSON Lines gzip. Cứ mỗi 5 phút sẽ trigger ghi một lần (micro-batch) để tối ưu IO.

### Lệnh chạy Spark Job (Submit vào cluster Docker)
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2 \
  /opt/bitnami/spark/jobs/streaming/aq_streaming.py
```

### Cách kiểm tra (Chấp nhận khi)
1. **Kiểm tra MinIO Bucket**: Đăng nhập `http://localhost:9001`, vào thư mục `aq-lake/bronze/openaq/measurements/`.
2. Chờ 5 phút (chu kỳ trigger), bạn sẽ thấy các file `.json.gz` xuất hiện, phân chia tự động theo partition ngày `ingest_date=YYYY-MM-DD/`.
3. **Chạy Unit Test Logic Spark**:
```bash
# Yêu cầu máy host cài pyspark
pytest tests/test_streaming.py tests/test_query_b.py
```

## M5: Spark Streaming - Query B (AQI & Alerts)
Bên trong cùng một Job Spark của M4, `Query B` phân tích dữ liệu realtime:
- Lọc nhiễu âm, nhiễu tĩnh, parse JSON theo Schema.
- Khớp với bảng metadata `sensor_id` qua phép join (với dictionary broadcast) để lấy thông tin vùng (Shapely Giao cắt đa giác).
- Tính `AQI` tức thời bằng hàm thuần `compute_aqi`.
- Dùng `psycopg2` để Upsert (Ghi đè) trạng thái trạm (`realtime.station_status`) và cảnh báo mức ô nhiễm (`realtime.alerts`) vào **PostgreSQL TimescaleDB** bên trong `foreachBatch`.

### Cách kiểm tra (Chấp nhận khi)
1. Job streaming chạy không bị lỗi crash (đã bao gồm Query A và Query B).
2. Dùng DB Client kết nối vào Postgres `localhost:5432` (`aq_user`/`aq_password`), chạy lệnh query xem kết quả:
```sql
SELECT * FROM realtime.readings ORDER BY event_time DESC LIMIT 10;
SELECT * FROM realtime.station_status;
SELECT * FROM realtime.alerts;
```
3. Test chống trùng lặp (replay message) không tạo ra dòng Alert dư thừa.

## M6: Backfill (Nạp dữ liệu lịch sử)
Quá trình nạp lịch sử cho dữ liệu chất lượng không khí từ OpenAQ AWS S3 Archive vào thư mục Bronze. Module `jobs/batch/backfill_archive.py` sẽ lọc toàn bộ dữ liệu lịch sử bằng toạ độ NYC (`NYC_BBOX`), đóng gói thành schema đồng nhất với Streaming và thêm các metadata quan trọng (`parameter`, `units`, `location_name`).

### Lệnh chạy Spark Batch
Tải một file mẫu (ví dụ CSV) và nhét vào thư mục `scratch` hoặc upload lên MinIO trước khi chạy script.
```bash
# Ví dụ chạy với dữ liệu mồi
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/bitnami/spark/jobs/batch/backfill_archive.py --input /opt/bitnami/spark/tests/mock_archive.csv
```

### Cách kiểm tra (Chấp nhận khi)
1. **Kiểm tra Log Output**: Log sẽ thông báo tổng số dòng đọc được từ CSV, và tổng số dòng sau khi lọc thành công theo phạm vi toạ độ của NYC.
2. **Kiểm tra MinIO Bucket**: Trong `aq-lake/bronze/openaq/measurements/ingest_date=.../`, bạn sẽ thấy các file `json.gz` mới được tạo ra bởi lệnh Batch này.
3. **Chạy Unit Test**:
```bash
pytest tests/test_backfill.py
```

## M7: Spark Làm Sạch (Bronze to Silver)
Tiến trình Batch chay định kỳ hằng ngày (Job `jobs/batch/clean_bronze_to_silver.py`). 
Mục tiêu: Đọc dữ liệu Raw từ Data Lake (Thư mục Bronze của 1 ngày cụ thể), Parse JSON, ánh xạ với danh mục cảm biến, lọc rác (giá trị âm/vô lý theo YAML), loại bỏ các bản ghi trùng lặp sinh ra do lỗi replay của stream, tính toán các cột phụ trợ (như giờ, quận) và nén xuống định dạng cột Parquet (Silver) để tối ưu truy vấn dữ liệu lớn. Đồng thời trích xuất từ điển `sensor_lookup.json` cho MapReduce.

### Lệnh chạy Spark Batch
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/bitnami/spark/jobs/batch/clean_bronze_to_silver.py --date 2026-09-26
```
*(Thay thế `2026-09-26` bằng ngày có thật trong thư mục Bronze mà Streaming/Backfill đã tạo)*

### Cách kiểm tra (Chấp nhận khi)
1. **Kiểm tra Log Output**: Log in ra bảng báo cáo data quality rõ ràng (Số dòng đọc -> Số dòng OK -> Số dòng sau khi Dedupe).
2. **Kiểm tra MinIO Bucket**: 
   - `aq-lake/silver/measurements/year=.../month=.../` sẽ chứa các file `*.parquet` đã được gom nhóm (compacted).
   - `aq-lake/validation/` sẽ chứa file `sensor_lookup.json`.
3. **Chạy Unit Test**:
```bash
pytest tests/test_cleaning.py
```

## M8: MapReduce & Đối chiếu (Validation)
Mốc này chạy một script **Hadoop Streaming (bằng thư viện `mrjob`)** độc lập bằng cách đọc trực tiếp từ thư mục Bronze (Raw). Script này thực hiện chính xác các quy tắc tính tổng hằng ngày (dedupe, filter out-of-range, trung bình cộng, đếm) như Spark, nhưng ở mức Map/Reduce cấp thấp. Lý do **không dùng Combiner** là vì việc dedupe đòi hỏi phải thu thập tất cả timestamp trước mới có thể xoá dòng trùng lặp, Combiner sẽ tự ý cộng dồn cục bộ làm sai dữ liệu.

Sau đó, script **Spark Validation** sẽ chạy và dùng toán tử `FULL OUTER JOIN` ráp kết quả của MapReduce với kết quả tính của bảng Silver (M7) dựa trên cụm khoá `location_id`, `parameter`, `date_local`. Nếu tỷ lệ sai lệch > 1% hoặc lệch số chìa khoá thì sẽ báo `FAIL`.

### Lệnh chạy MapReduce 
```bash
# Cài mrjob trên host nếu muốn chạy thử
pip install mrjob
python jobs/mapreduce/daily_stats.py \
  --lookup validation/sensor_lookup.json \
  --rules config/quality_rules.yaml \
  < sample_bronze_file.json
```

### Lệnh chạy Đối chiếu Spark
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/bitnami/spark/jobs/batch/validate_mr_vs_spark.py --date 2026-09-26
```

### Cách kiểm tra (Chấp nhận khi)
1. Job đối chiếu Spark sinh ra file báo cáo `.json` tại `reports/validation/2026-09-26.json`.
2. Nội dung file báo cáo ghi `status: "PASS"`, tổng số key MapReduce và Spark tương đồng 100%, không bị lệch chìa và dung sai (max_diff_avg) là 0.
3. Chạy Unit test cho MRJob:
```bash
pytest tests/test_mapreduce.py
```

## M9: Spark Tổng Hợp (Silver to Gold Data Lake)
Bảng Silver tuy sạch nhưng chưa phải là các bảng phân tích (Analytics). Tại mốc này, `jobs/batch/aggregate_silver_to_gold.py` sẽ gom nhóm dữ liệu theo chiều (Dimensions) và sự kiện (Facts). 
Quá trình này tuân thủ tuyệt đối **Quy chuẩn tính AQI của EPA**:
- Sử dụng trung bình 24h đối với hạt PM2.5.
- Sử dụng xấp xỉ mức tối đa (Max) trong ngày đối với khí O3.
- Sử dụng Broadcast Hash Join giữa `fact_*` và `dim_station` để tăng tốc độ truy vấn mà không bị xáo trộn (shuffle) dữ liệu mạng. Explain Plan của truy vấn này sẽ được xuất ra file log txt.

### Lệnh chạy Spark Batch
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/bitnami/spark/jobs/batch/aggregate_silver_to_gold.py --date 2026-09-26
```

### Cách kiểm tra (Chấp nhận khi)
1. **Kiểm tra MinIO Bucket**: `aq-lake/gold/` xuất hiện thêm 6 thư mục: `dim_station`, `fact_hourly`, `fact_daily_aqi`, `mart_region_daily`, `mart_temporal`, `cluster_features`. Tất cả đều lưu chuẩn nén `.parquet`.
2. Mở file thư mục `reports/plans/mart_region_plan_2026-09-26.txt` xem qua Explain Plan sẽ thấy rõ toán tử `BroadcastExchange` và `BroadcastHashJoin`.
3. **Chạy Unit Test**:
```bash
pytest tests/test_gold.py
```

## M10: Spark SQL & KMeans Machine Learning
Sử dụng thư viện `pyspark.ml` để tạo Data Pipeline gồm: `VectorAssembler` -> `StandardScaler` (tinh chỉnh Mean và Std) -> `KMeans` để phân cụm "Những ngày ô nhiễm" theo đặc trưng: AQI trung bình, AQI max và số giờ trạm quá ngưỡng ô nhiễm. Hệ thống tự động so sánh Silhouette Score với `K` từ 2 đến 7 để chọn mô hình tốt nhất, sau đó khôi phục toạ độ tâm cụm (Denormalize) về thang đo gốc.
Hệ thống cũng chạy các truy vấn **Spark SQL** chuyên sâu và đẩy (JDBC Write) thẳng kết quả Dashboard vào schema `analytics` trên PostgreSQL.

### Lệnh chạy Machine Learning Batch
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2 \
  /opt/bitnami/spark/jobs/batch/analytics.py
```

### Cách kiểm tra (Chấp nhận khi)
1. **Kiểm tra MinIO Bucket**: Sinh ra thư mục mô hình AI đã được Serialized tại `aq-lake/models/kmeans/v1/`.
2. Truy xuất PostgreSQL `localhost:5432` (`aq` DB), kiểm tra các bảng đã được Fill dữ liệu đầy đủ từ Spark đẩy qua:
```sql
SELECT * FROM analytics.region_stats;
SELECT * FROM analytics.daily_summary LIMIT 10;
```
3. Mở file `reports/ml/k_selection.csv` để đối chiếu điểm số thuật toán phân cụm Silhouette của từng hệ số `K`.
4. **Chạy Unit Test**:
```bash
pytest tests/test_ml.py
```

## M11: Bộ lập lịch tự động (Airflow DAG / PowerShell)
Vì môi trường Windows có thể không cài sẵn Airflow và Makefile, tôi đã cung cấp cả hai phương án:
1. **`run_batch.ps1`**: Một script PowerShell thay thế Makefile, thực thi tuần tự từ M7 -> M9 -> M8 -> M10 chỉ bằng 1 lệnh duy nhất.
2. **`dags/aq_batch_pipeline.py`**: Một DAG Airflow hoàn chỉnh để lên lịch tự động cho pipeline hằng ngày vào 2:00 sáng.

### Lệnh chạy luồng Batch bằng 1 lệnh duy nhất (Dành cho Windows Host)
```powershell
# Chạy script (bạn cần mở cửa sổ PowerShell ở thư mục dự án)
.\run_batch.ps1 -Date "2026-09-26"
```

### Cách kiểm tra (Chấp nhận khi)
1. Cửa sổ terminal in ra màn hình trình tự thực thi màu sắc rõ ràng.
2. Các bước M7, M9, M8, M10 lần lượt được Spark Container chạy thông qua lệnh `docker exec` thành công mà không văng lỗi (ngoại trừ M8 nếu thiếu file mồi mapreduce).
3. **Chạy Unit Test DAG Airflow**:
```bash
# Cần cài airflow cục bộ để test cấu trúc DAG
pip install apache-airflow
pytest tests/test_dag.py
```

## M12: Thí nghiệm hiệu năng hệ thống
Mốc này cung cấp đoạn script test độc lập `jobs/batch/performance_test.py` nhằm đo đạc trực tiếp trên Spark tốc độ xử lý khi bật/tắt tính năng Broadcast Hash Join và Repartition. 

### Lệnh chạy Thí nghiệm
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/bitnami/spark/jobs/batch/performance_test.py
```
- **Chấp nhận khi**: Log Spark báo hoàn tất và file xuất ra bảng kết quả (CSV) nằm tại thư mục host: `reports/perf/perf_results.csv`. Bảng này sẽ cho thấy rõ ưu thế tốc độ vượt trội (thời gian tính bằng giây nhỏ hơn nhiều) khi dùng chế độ Broadcast so với Shuffle Join mặc định.

## M13: Ứng dụng Notifier (Cảnh báo thời gian thực)
Đây là service Consumer cuối cùng của kiến trúc. Nó lắng nghe Topic `aq.alerts.level-changed.v1` (Sinh ra từ Job Spark Streaming M4/M5 lúc ban đầu) và định dạng thông báo màu sắc sống động, sau đó in ra log hoặc gửi qua API Telegram Bot nếu cấu hình môi trường.

### Lệnh chạy Notifier
Service này đã được đưa thẳng vào `docker-compose.yml`. Bạn chỉ cần Build lại và cấp Token qua file `.env`:
```bash
# Đảm bảo bạn đã ghi TELEGRAM_BOT_TOKEN và TELEGRAM_CHAT_ID trong .env
docker-compose up -d --build notifier
```

### Cách kiểm tra (Chấp nhận khi)
1. Theo dõi tiến trình gửi Alert trực tiếp bằng lệnh: `docker logs -f notifier`.
2. Chờ lúc luồng Spark Streaming quết qua dữ liệu chạm ngưỡng, Terminal sẽ nháy Alert JSON và format sang tiếng Việt cực đẹp.
3. **Chạy Unit Test**:
```bash
pytest tests/test_notifier.py
```

