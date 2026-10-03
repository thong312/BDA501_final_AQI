Write-Host ">>> BUOC 1: TAT POLLER VA DON DEP RAC..." -ForegroundColor Yellow
docker stop poller
docker exec -i minio mc rm -r --force local/aq-checkpoints/
docker exec -i spark-master rm -rf /opt/spark/work/*

Write-Host ">>> BUOC 2: BAT SPARK STREAMING TRONG CUA SO MOI..." -ForegroundColor Yellow
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit -File .\start_streaming.ps1"

Write-Host "Dang doi 25 giay de Spark Streaming san sang hung du lieu..." -ForegroundColor Cyan
Start-Sleep -Seconds 60

Write-Host ">>> BUOC 3: BOM DU LIEU GIA LAP VAO KAFKA..." -ForegroundColor Green
docker exec -i spark-master python3 scripts/inject_from_csv.py

Write-Host ">>> HOAN TAT! Hay mo Telegram va Dashboard len de xem canh bao!" -ForegroundColor Red
Write-Host "(De dung Streaming, hay sang cua so moi kia va an Ctrl+C)"