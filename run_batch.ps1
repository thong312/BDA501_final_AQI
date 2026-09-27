param(
    [string]$Date = (Get-Date).ToString("yyyy-MM-dd")
)

Write-Host "===============================================" -ForegroundColor Cyan
Write-Host " BẮT ĐẦU QUY TRÌNH BATCH CHO NGÀY: $Date" -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan

Write-Host "`n[1/4] Chạy M7: Làm sạch dữ liệu (Bronze to Silver)..." -ForegroundColor Yellow
docker exec -i spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark/jobs/batch/clean_bronze_to_silver.py --date $Date
if ($LASTEXITCODE -ne 0) { throw "Lỗi tại bước M7" }

Write-Host "`n[2/4] Chạy M9: Tổng hợp kho dữ liệu (Silver to Gold)..." -ForegroundColor Yellow
docker exec -i spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4 /opt/spark/jobs/batch/aggregate_silver_to_gold.py --date $Date
if ($LASTEXITCODE -ne 0) { throw "Lỗi tại bước M9" }

Write-Host "`n[3/4] Chạy M8: Đối chiếu (Validation)..." -ForegroundColor Yellow
docker exec -i spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4 /opt/spark/jobs/batch/validate_mr_vs_spark.py --date $Date
# (Bỏ qua ném lỗi M8 nếu bạn chưa nạp dữ liệu chạy MapReduce mồi)

Write-Host "`n[4/4] Chạy M10: SQL Analytics & Machine Learning (KMeans)..." -ForegroundColor Yellow
docker exec -i spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2 /opt/spark/jobs/batch/analytics.py
if ($LASTEXITCODE -ne 0) { throw "Lỗi tại bước M10" }

Write-Host "`n===============================================" -ForegroundColor Green
Write-Host " HOÀN THÀNH TOÀN BỘ QUY TRÌNH BATCH!" -ForegroundColor Green
Write-Host "===============================================" -ForegroundColor Green
