"""Đếm số bản ghi Silver. Chạy: $(SUBMIT) /opt/aq/count_silver.py (xem Makefile)."""
from common.config import SILVER_PATH
from common.spark_utils import create_spark_session

spark = create_spark_session("CountSilver")
print("TOTAL_SILVER_RECORDS=", spark.read.parquet(SILVER_PATH).count())
