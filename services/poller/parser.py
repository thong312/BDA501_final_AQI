from datetime import datetime, timezone
import json
from common.schemas import Measurement, Sensor, DLQMessage
from pydantic import ValidationError

def parse_sensor_metadata(raw_location: dict) -> list[Sensor]:
    sensors = []
    loc_id = raw_location.get("id")
    loc_name = raw_location.get("name", "Unknown")
    coords = raw_location.get("coordinates", {})
    lat = coords.get("latitude", 0.0) if coords else 0.0
    lon = coords.get("longitude", 0.0) if coords else 0.0
    
    for s in raw_location.get("sensors", []):
        try:
            param = s.get("parameter", {})
            sensor = Sensor(
                sensor_id=s.get("id"),
                location_id=loc_id,
                location_name=loc_name,
                parameter=param.get("name", "unknown"),
                units=param.get("units", "unknown"),
                lat=lat,
                lon=lon,
                updated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            sensors.append(sensor)
        except Exception:
            continue
    return sensors

def parse_measurements(raw_results: list, location_id: int, lat: float, lon: float, endpoint: str) -> tuple[list[Measurement], list[DLQMessage]]:
    ok_list = []
    dlq_list = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    for item in raw_results:
        try:
            sensor_id = item.get("sensorsId") or item.get("sensorId") or item.get("id")
            val = item.get("value")
            
            if isinstance(item.get("latest"), dict):
                val = item["latest"].get("value")
                dt_utc = item["latest"].get("datetime")
                dt_local = dt_utc
            else:
                period = item.get("period", {})
                dt_to = period.get("datetimeTo", {})
                dt_utc = dt_to.get("utc") or item.get("datetime", {}).get("utc") or item.get("datetime")
                dt_local = dt_to.get("local") or item.get("datetime", {}).get("local") or dt_utc

            if isinstance(val, dict):
                val = val.get("value")
                
            m = Measurement(
                sensor_id=int(sensor_id) if sensor_id else -1,
                location_id=location_id,
                value=float(val) if val is not None else -1.0,
                datetime_utc=str(dt_utc),
                datetime_local=str(dt_local),
                lat=lat,
                lon=lon,
                ingested_at=now,
                source="openaq-api"
            )
            ok_list.append(m)
        except ValidationError as e:
            dlq_list.append(DLQMessage(raw_payload=json.dumps(item), error=str(e), endpoint=endpoint, failed_at=now))
        except Exception as e:
            dlq_list.append(DLQMessage(raw_payload=json.dumps(item), error="parse_error_or_missing_fields", endpoint=endpoint, failed_at=now))
            
    return ok_list, dlq_list
