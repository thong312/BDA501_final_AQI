import json
import os
from pathlib import Path

# Đọc geojson tự động nếu có shapely
try:
    import shapely.geometry
    SHAPELY_INSTALLED = True
except ImportError:
    SHAPELY_INSTALLED = False

def load_boroughs():
    if not SHAPELY_INSTALLED:
        return []
    geojson_path = Path(__file__).parent.parent / "config" / "nyc_boroughs.geojson"
    if not geojson_path.exists():
        return []
    with open(geojson_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    boroughs = []
    for feature in data.get("features", []):
        name = feature.get("properties", {}).get("boro_name") or feature.get("properties", {}).get("BoroName")
        geom = shapely.geometry.shape(feature["geometry"])
        boroughs.append({"name": name, "geom": geom})
    return boroughs

BOROUGHS = load_boroughs()

def get_borough(lat: float, lon: float) -> str:
    if not SHAPELY_INSTALLED or lat is None or lon is None:
        return "Unknown"
    pt = shapely.geometry.Point(lon, lat)
    for b in BOROUGHS:
        if b["geom"].contains(pt):
            return b["name"]
    return "Unknown"
