"""Schema message (ARCHITECTURE 5.1): pydantic cho Poller, StructType cho Spark."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, StrictStr, field_validator


def _check_iso_datetime(v: Optional[str], required: bool) -> Optional[str]:
    if v is None:
        if required:
            raise ValueError("missing")
        return v
    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("datetime must include timezone")
    return v


class Measurement(BaseModel):
    # Bat buoc: thieu hoac sai kieu -> DLQ
    sensor_id: int
    location_id: int
    value: float
    datetime_utc: StrictStr
    # Tuy chon
    datetime_local: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    ingested_at: Optional[str] = None
    source: str = "openaq-api"

    @field_validator("datetime_utc")
    @classmethod
    def _utc(cls, v):
        return _check_iso_datetime(v, required=True)

    @field_validator("datetime_local")
    @classmethod
    def _local(cls, v):
        return _check_iso_datetime(v, required=False)


class Sensor(BaseModel):
    sensor_id: int
    location_id: int
    location_name: Optional[str] = None
    parameter: str
    units: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    updated_at: str


class DLQMessage(BaseModel):
    raw_payload: str
    error: str
    endpoint: str
    failed_at: str


class Alert(BaseModel):
    alert_id: str
    scope: str
    borough: Optional[str] = None
    trigger_location_id: Optional[int] = None
    aqi: Optional[int] = None
    level: str
    prev_level: str
    type: str
    dominant_pollutant: Optional[str] = None
    event_time: str
    created_at: str


# ---------- Spark StructType (pyspark chi co trong image Spark) ----------
try:
    from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                                   StructType)

    MEASUREMENT_STRUCT = StructType([
        StructField("sensor_id", IntegerType()),
        StructField("location_id", IntegerType()),
        StructField("value", DoubleType()),
        StructField("datetime_utc", StringType()),
        StructField("datetime_local", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("ingested_at", StringType()),
        StructField("source", StringType()),
        # Chi co o ban ghi backfill (archive)
        StructField("parameter", StringType()),
        StructField("units", StringType()),
        StructField("location_name", StringType()),
    ])

    SENSOR_STRUCT = StructType([
        StructField("sensor_id", IntegerType()),
        StructField("location_id", IntegerType()),
        StructField("location_name", StringType()),
        StructField("parameter", StringType()),
        StructField("units", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("updated_at", StringType()),
    ])
except ImportError:  # Poller / Notifier khong can Spark
    MEASUREMENT_STRUCT = SENSOR_STRUCT = None
