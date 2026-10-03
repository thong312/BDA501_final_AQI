# Demo canh bao realtime cho buoi trinh bay: gia lap luong streaming (OpenAQ API chi co gia tri theo gio). KHONG xoa checkpoint, KHONG tat poller:
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

Write-Host ">>> BUOC 3: GIA LAP LUONG STREAMING 10 PHUT (3 sensor PM2.5 that, event_time = bay gio)..." -ForegroundColor Green
Write-Host "Mo Dashboard http://localhost:5050 va Telegram; Ctrl+C de dung som." -ForegroundColor Cyan
docker exec -it -w /opt/aq spark-master python3 scripts/inject_from_csv.py --minutes 10
docker exec postgres psql -U aq_user -d aq -c "SELECT scope, borough, level, prev_level, type, event_time FROM realtime.alerts ORDER BY created_at DESC LIMIT 6"

Write-Host ">>> HOAN TAT! Xem Telegram / 'docker logs notifier' va Dashboard http://localhost:5050" -ForegroundColor Red
Write-Host "Ban 1 lan (khong lien tuc):       docker exec -w /opt/aq spark-master python3 scripts/inject_test_measurements.py"
Write-Host "Dua ve Good de tap lai:           docker exec -w /opt/aq spark-master python3 scripts/inject_test_measurements.py --recover"
