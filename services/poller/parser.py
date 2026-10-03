"""Chuan hoa response OpenAQ v3 sang message Kafka (ham thuan, de unit test).

Poller khong tinh AQI va khong loc gia tri am — do la viec cua Spark.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from pydantic import ValidationError

from common.schemas import DLQMessage, Measurement, Sensor


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_sensor_metadata(raw_location: dict, now: str = None) -> list[Sensor]:
    """Mot location OpenAQ -> moi sensor mot message."""
    now = now or utc_now_str()
    coords = raw_location.get("coordinates") or {}
    sensors = []
    for s in raw_location.get("sensors") or []:
        param = s.get("parameter") or {}
        try:
            sensors.append(Sensor(
                sensor_id=s.get("id"),
                location_id=raw_location.get("id"),
                location_name=raw_location.get("name"),
                parameter=param.get("name"),
                units=param.get("units"),
                lat=coords.get("latitude"),
                lon=coords.get("longitude"),
                updated_at=now,
            ))
        except ValidationError:
            continue
    return sensors


def _error_code(err: ValidationError) -> str:
    """Ma loi ngan cho DLQ, vi du value_not_numeric, missing_sensor_id."""
    first = err.errors()[0]
    field = first["loc"][0] if first["loc"] else "payload"
    if first["type"] == "missing" or first.get("input", "") is None:
        return f"missing_{field}"
    if field == "value":
        return "value_not_numeric"
    return f"invalid_{field}"


def parse_measurements(raw_results: list, location_id: int, lat, lon, endpoint: str,
                       now: str = None) -> tuple[list[Measurement], list[DLQMessage]]:
    """Ket qua /v3/locations/{id}/latest -> (ban do hop le, ban ghi DLQ)."""
    now = now or utc_now_str()
    ok_list, dlq_list = [], []
    for item in raw_results:
        if not isinstance(item, dict):
            dlq_list.append(DLQMessage(raw_payload=json.dumps(item, default=str), error="not_an_object",
                                       endpoint=endpoint, failed_at=now))
            continue
        dt = item.get("datetime")
        dt = dt if isinstance(dt, dict) else {}
        coords = item.get("coordinates") or {}
        payload = {
            "sensor_id": item.get("sensorsId"),
            "location_id": item.get("locationsId") or location_id,
            "value": item.get("value"),
            "datetime_utc": dt.get("utc"),
            "datetime_local": dt.get("local"),
            "lat": coords.get("latitude", lat),
            "lon": coords.get("longitude", lon),
            "ingested_at": now,
            "source": "openaq-api",
        }
        try:
            ok_list.append(Measurement(**payload))
        except ValidationError as e:
            dlq_list.append(DLQMessage(raw_payload=json.dumps(item, default=str), error=_error_code(e),
                                       endpoint=endpoint, failed_at=now))
    return ok_list, dlq_list
