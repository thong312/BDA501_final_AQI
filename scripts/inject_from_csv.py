import json, sys, random, csv, time
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

print('Khoi dong CSV Injector...')
p = Producer({'bootstrap.servers': 'kafka:9092'})
fmt = lambda t: t.strftime('%Y-%m-%dT%H:%M:%SZ')

# Doc file CSV
rows = []
with open('scripts/fake_data.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append(row)

# Ban random 3 dong tu CSV
selected_rows = random.sample(rows, min(3, len(rows)))
base = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=10)
now = fmt(datetime.now(timezone.utc))

for row in selected_rows:
    # Them xiu nhieu (noise) cho random
    random_value = float(row['value']) + random.uniform(-10.0, 10.0)
    
    m = {
        'sensor_id': int(row['sensor_id']),
        'location_id': int(row['location_id']),
        'value': round(random_value, 2),
        'datetime_utc': fmt(base),
        'ingested_at': now,
        'source': 'openaq-csv-demo'
    }
    p.produce('aq.openaq.measurements.v1', key=str(row['location_id']).encode(), value=json.dumps(m).encode())
    print(f'Da ban data: {m}')

p.flush(10)
print('Hoan tat ban CSV!')