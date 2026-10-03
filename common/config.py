"""Đường dẫn và hằng số cấu hình dùng chung (ARCHITECTURE mục 11)."""
import os
from pathlib import Path

import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT_DIR / "config"
REPORTS_DIR = ROOT_DIR / "reports"

TIMEZONE = "America/New_York"
DEFAULT_BBOX = "-74.26,40.49,-73.70,40.92"

LAKE = "s3a://aq-lake"
CHECKPOINTS = "s3a://aq-checkpoints"
BRONZE_PATH = f"{LAKE}/bronze/openaq/measurements/"
SILVER_PATH = f"{LAKE}/silver/measurements/"
GOLD_PATH = f"{LAKE}/gold/"
VALIDATION_PATH = f"{LAKE}/validation/"

TOPIC_MEASUREMENTS = "aq.openaq.measurements.v1"
TOPIC_SENSORS = "aq.openaq.sensors.v1"
TOPIC_DLQ = "aq.openaq.measurements.v1.dlq"
TOPIC_ALERTS = "aq.alerts.level-changed.v1"


def load_yaml(name):
    with open(CONFIG_DIR / name, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def env(name, default=None):
    return os.getenv(name, default)
