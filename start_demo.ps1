# Demo canh bao realtime cho buoi trinh bay. KHONG xoa checkpoint, KHONG tat poller:
# state canh bao nam trong PostgreSQL (realtime.station_status / region_status), xoa checkpoint
# khong reset duoc gi ma con lam Query A bo qua cac batch Bronze da commit.
$ErrorActionPreference = "Stop"

Write-Host ">>> BUOC 1: KHOI DONG STACK (Kafka, Spark, MinIO, PostgreSQL, poller, notifier, dashboard)..." -ForegroundColor Yellow
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { throw "docker compose up that bai" }

Write-Host ">>> BUOC 2: KIEM TRA SPARK STREAMING..." -ForegroundColor Yellow
docker exec spark-master pgrep -f aq_streaming.py | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Streaming chua chay -> mo trong cua so moi (Ctrl+C o cua so do de dung)." -ForegroundColor Cyan
    Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit -File .\start_streaming.ps1"
    Write-Host "Doi 90 giay de Spark Streaming khoi dong va nap metadata..." -ForegroundColor Cyan
    Start-Sleep -Seconds 90
} else {
    Write-Host "Streaming dang chay, bo qua." -ForegroundColor Cyan
}

Write-Host ">>> BUOC 3: BOM DU LIEU GIA LAP VAO KAFKA (sensor PM2.5 that, AQI ~275)..." -ForegroundColor Green
docker exec -w /opt/aq spark-master python3 scripts/inject_test_measurements.py
if ($LASTEXITCODE -ne 0) { throw "inject that bai (borough dang canh bao? chay voi --recover truoc)" }

Write-Host "Doi 75 giay cho 1 trigger cua Query B..." -ForegroundColor Cyan
Start-Sleep -Seconds 75
docker exec postgres psql -U aq_user -d aq -c "SELECT scope, borough, level, prev_level, type, event_time FROM realtime.alerts ORDER BY created_at DESC LIMIT 6"

Write-Host ">>> HOAN TAT! Xem Telegram / 'docker logs notifier' va Dashboard http://localhost:5050" -ForegroundColor Red
Write-Host "Replay (khong sinh canh bao trung): docker exec -w /opt/aq spark-master python3 scripts/inject_test_measurements.py --at <event_time> --pm25 <gia tri> (in o buoc 3)"
Write-Host "Dua ve Good de tap lai:           docker exec -w /opt/aq spark-master python3 scripts/inject_test_measurements.py --recover"
