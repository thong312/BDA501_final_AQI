# Batch hằng ngày trên Windows (tương đương `make batch DATE=...`):
# clean -> mapreduce -> gold -> validate -> analytics
param(
    [string]$Date = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
)
$ErrorActionPreference = "Stop"
$Packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3"

function Submit([string]$Job, [string[]]$JobArgs) {
    docker exec -i spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 `
        --conf spark.jars.ivy=/opt/aq-ivy --packages $Packages "/opt/aq/jobs/batch/$Job" @JobArgs
    if ($LASTEXITCODE -ne 0) { throw "Lỗi tại $Job" }
}

Write-Host "=== Batch cho ngày $Date ===" -ForegroundColor Cyan

Write-Host "[1/5] M7 Làm sạch Bronze -> Silver" -ForegroundColor Yellow
Submit "clean_bronze_to_silver.py" @("--date", $Date)

Write-Host "[2/5] M8 MapReduce thống kê ngày" -ForegroundColor Yellow
docker exec -i -w /opt/aq spark-master python3 jobs/mapreduce/run_daily_stats.py --date $Date
if ($LASTEXITCODE -ne 0) { throw "Lỗi tại MapReduce" }

Write-Host "[3/5] M9 Tổng hợp Silver -> Gold" -ForegroundColor Yellow
Submit "aggregate_silver_to_gold.py" @("--date", $Date)

Write-Host "[4/5] M8 Đối chiếu MapReduce vs Spark" -ForegroundColor Yellow
Submit "validate_mr_vs_spark.py" @("--date", $Date)

Write-Host "[5/5] M10 Spark SQL + KMeans" -ForegroundColor Yellow
Submit "analytics.py" @()

Write-Host "=== Hoàn thành ===" -ForegroundColor Green
