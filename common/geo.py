"""Bbox NYC và gán borough bằng point-in-polygon (config/nyc_boroughs.geojson)."""
import json
from typing import Optional, Tuple

from shapely.geometry import Point, shape
from shapely.prepared import prep

from common.config import CONFIG_DIR, DEFAULT_BBOX

BOROUGH_NAMES = ("Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island")


def parse_bbox(bbox: str = DEFAULT_BBOX) -> Tuple[float, float, float, float]:
    """'min_lon,min_lat,max_lon,max_lat' -> tuple."""
    min_lon, min_lat, max_lon, max_lat = (float(x) for x in bbox.split(","))
    return min_lon, min_lat, max_lon, max_lat


def in_bbox(lat, lon, bbox: str = DEFAULT_BBOX) -> bool:
    if lat is None or lon is None:
        return False
    min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon


def _load_boroughs():
    with open(CONFIG_DIR / "nyc_boroughs.geojson", "r", encoding="utf-8") as f:
        data = json.load(f)
    out = []
    for feature in data["features"]:
        props = feature.get("properties", {})
        name = props.get("boro_name") or props.get("BoroName")
        out.append((name, prep(shape(feature["geometry"]))))
    return out


_BOROUGHS = None


def get_borough(lat, lon) -> Optional[str]:
    """Tên borough chứa điểm; None nếu ngoài NYC (bbox có cả một phần New Jersey)."""
    global _BOROUGHS
    if lat is None or lon is None:
        return None
    if _BOROUGHS is None:
        _BOROUGHS = _load_boroughs()
    pt = Point(lon, lat)
    for name, geom in _BOROUGHS:
        if geom.contains(pt):
            return name
    return None
