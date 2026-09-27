from pydantic import BaseModel
from typing import Optional

class Measurement(BaseModel):
    sensor_id: int
    location_id: int
    value: float
    datetime_utc: str
    datetime_local: str
    lat: float
    lon: float
    ingested_at: str
    source: str = "openaq-api"

class Sensor(BaseModel):
    sensor_id: int
    location_id: int
    location_name: str
    parameter: str
    units: str
    lat: float
    lon: float
    updated_at: str

class DLQMessage(BaseModel):
    raw_payload: str
    error: str
    endpoint: str
    failed_at: str
