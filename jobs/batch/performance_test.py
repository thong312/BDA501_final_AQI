"""Thi nghiem hieu nang (M12): broadcast bat/tat va so partition, ket qua reports/perf/perf_results.csv."""
import csv
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql.functions import broadcast

from common.config import GOLD_PATH, REPORTS_DIR, SILVER_PATH
from common.spark_utils import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    spark = create_spark_session("AQ_Performance_Test")
    
    # Truy xuat du lieu (can chay M7, M9 truoc)
    silver_path = SILVER_PATH
    dim_path = f"{GOLD_PATH}dim_station"
    
    logger.info("Dang doc du lieu cho thi nghiem...")
    try:
        df_silver = spark.read.parquet(silver_path)
        df_dim = spark.read.parquet(dim_path)
    except Exception as e:
        logger.error("Du lieu Silver/Gold chua co san de do hieu nang. Vui long chay luong Data Pipeline truoc.")
        sys.exit(1)

    results = []

    # BAI TEST 1: KHONG DUNG BROADCAST (SHUFFLE JOIN)
    logger.info("Test 1: Shuffle Join (Tat Broadcast)...")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    start = time.time()
    df_join_1 = df_silver.join(df_dim, "location_id")
    c1 = df_join_1.count() # Ep Spark thuc thi
    end = time.time()
    results.append({"experiment": "No_Broadcast_Shuffle", "time_seconds": round(end - start, 2), "records": c1})

    # BAI TEST 2: DUNG BROADCAST JOIN
    logger.info("Test 2: Broadcast Hash Join...")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") # 10MB
    start = time.time()
    df_join_2 = df_silver.join(broadcast(df_dim), "location_id")
    c2 = df_join_2.count()
    end = time.time()
    results.append({"experiment": "With_Broadcast", "time_seconds": round(end - start, 2), "records": c2})

    # BAI TEST 3: REPARTITION LEN 20 PHAN VUNG
    logger.info("Test 3: Tang Partition len 20 + Broadcast...")
    df_silver_rep = df_silver.repartition(20)
    start = time.time()
    df_join_3 = df_silver_rep.join(broadcast(df_dim), "location_id")
    c3 = df_join_3.count()
    end = time.time()
    results.append({"experiment": "Broadcast_20_Partitions", "time_seconds": round(end - start, 2), "records": c3})

    # LUU KET QUA VAO FILE BAO CAO
    report_dir = REPORTS_DIR / "perf"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_file = report_dir / "perf_results.csv"
    
    with open(report_file, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["experiment", "time_seconds", "records"])
        writer.writeheader()
        writer.writerows(results)
        
    logger.info(f"Hoan thanh thi nghiem hieu nang. Bao cao duoc luu tai: {report_file}")
    for r in results:
        print(r)
