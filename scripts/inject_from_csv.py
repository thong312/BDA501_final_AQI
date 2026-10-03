import json, sys, random, csv, time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

print('Khoi dong CSV Injector Continuous...')
p = Producer({'bootstrap.servers': 'kafka:9092'})
fmt = lambda t: t.strftime('%Y-%m-%dT%H:%M:%SZ')

# Doc file CSV
rows = []
with open('scripts/fake_data.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append(row)

# Thoi gian ao ban dau (+3 tieng de ep watermark)
offset_minutes = 180

while True:
    try:
        selected_rows = random.sample(rows, min(3, len(rows)))
        base = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=offset_minutes)
        now = fmt(datetime.now(timezone.utc))

        for row in selected_rows:
            # Them xiu nhieu (noise) cho random de bieu do nhay len xuong cho that
            random_value = float(row['value']) + random.uniform(-30.0, 80.0)
            if random_value < 10: random_value = 10.0
            
            m = {
                'sensor_id': int(row['sensor_id']),
                'location_id': int(row['location_id']),
                'value': round(random_value, 2),
                'datetime_utc': fmt(base),
                'ingested_at': now,
                'source': 'openaq-csv-demo'
            }
            p.produce('aq.openaq.measurements.v1', key=str(row['location_id']).encode(), value=json.dumps(m).encode())
            print(f'Da ban data ({fmt(base)}): {m}')

        p.flush(10)
        
        # Tang thoi gian ao len 2 phut cho lan ban tiep theo de watermark lien tuc day toi
        offset_minutes += 2 
        
        print("Doi 15 giay de ban dot tiep theo... (An Ctrl+C de dung)")
        time.sleep(15)
        
    except KeyboardInterrupt:
        print("\nDa dung ban CSV!")
        break
