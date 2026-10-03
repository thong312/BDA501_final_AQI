"""Đếm số bản ghi Bronze. Chạy: $(SUBMIT) /opt/aq/count_bronze.py (xem Makefile)."""
from common.config import BRONZE_PATH
from common.spark_utils import create_spark_session

spark = create_spark_session("CountBronze")
# Đọc theo glob partition: đọc thẳng thư mục gốc thì Spark chỉ thấy file streaming đã commit
# trong _spark_metadata và bỏ sót phần backfill ghi bằng batch.
print("TOTAL_RECORDS=", spark.read.text(f"{BRONZE_PATH}ingest_date=*/").count())
