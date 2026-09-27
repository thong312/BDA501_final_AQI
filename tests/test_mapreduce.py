import pytest
import json
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from jobs.mapreduce.daily_stats import DailyStatsMR

def test_daily_stats_mr(tmp_path):
    mr_job = DailyStatsMR()
    
    # Tạo fake dict
    lookup_file = tmp_path / "lookup.json"
    lookup_file.write_text('{"sensor_id": 1, "parameter": "pm25"}\n')
    
    rules_file = tmp_path / "rules.yaml"
    rules_file.write_text('pm25:\n  min: 0\n  max: 1000\n')
    
    class MockOptions:
        lookup = str(lookup_file)
        rules = str(rules_file)
        bbox = "-74.26,40.49,-73.70,40.92"
        
    mr_job.options = MockOptions()
    mr_job.mapper_init()
    
    # Bản ghi thử nghiệm Bronze
    bronze_line = '{"value": "{\\"sensor_id\\": 1, \\"location_id\\": 101, \\"lat\\": 40.7, \\"lon\\": -73.9, \\"value\\": 120.0, \\"datetime_local\\": \\"2026-09-26T10:00:00-04:00\\", \\"datetime_utc\\": \\"2026-09-26T14:00:00Z\\"}"}'
    
    # Test Mapper
    map_res = list(mr_job.mapper(None, bronze_line))
    assert len(map_res) == 1
    key, val = map_res[0]
    
    assert key == [101, "pm25", "2026-09-26"]
    assert val == ["2026-09-26T14:00:00Z", 120.0]
    
    # Test Reducer (Dedupe và tính Avg/Hours_over_100)
    # Gửi 2 bản ghi trùng nhau hoàn toàn về datetime_utc
    red_res = list(mr_job.reducer(key, [val, val])) 
    assert len(red_res) == 1
    r_key, r_stats = red_res[0]
    
    assert r_stats["count"] == 1 # Dedupe thành công, chỉ còn 1
    assert r_stats["sum"] == 120.0
    assert r_stats["hours_over_100"] == 1 # Do > 100
