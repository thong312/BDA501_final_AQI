from mrjob.job import MRJob
import json
import yaml
import os

class DailyStatsMR(MRJob):
    
    def configure_args(self):
        super(DailyStatsMR, self).configure_args()
        self.add_file_arg('--lookup', help='Path to validation/sensor_lookup.json')
        self.add_file_arg('--rules', help='Path to config/quality_rules.yaml')
        self.add_passthru_arg('--bbox', default='-74.26,40.49,-73.70,40.92')

    def mapper_init(self):
        # Nạp lookup dictionary từ file output của Spark Làm Sạch
        self.lookup = {}
        if self.options.lookup:
            try:
                with open(self.options.lookup, 'r') as f:
                    for line in f:
                        if line.strip():
                            d = json.loads(line)
                            self.lookup[d['sensor_id']] = d['parameter']
            except Exception as e:
                pass
                
        # Nạp rules
        self.rules = {}
        if self.options.rules:
            try:
                with open(self.options.rules, 'r') as f:
                    self.rules = yaml.safe_load(f)
            except:
                pass

        parts = self.options.bbox.split(',')
        self.min_lon, self.min_lat = float(parts[0]), float(parts[1])
        self.max_lon, self.max_lat = float(parts[2]), float(parts[3])

    def mapper(self, _, line):
        try:
            # Parse cấu trúc JSON Lines của Bronze
            row = json.loads(line)
            data_str = row.get("value")
            if not data_str: return
            
            data = json.loads(data_str)
            
            # 1. Lọc theo Bounding Box
            lat = data.get("lat")
            lon = data.get("lon")
            if lat is None or lon is None: return
            if not (self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon): return
                
            # 2. Bỏ giá trị âm
            val = data.get("value")
            if val is None or val < 0: return
                
            sensor_id = data.get("sensor_id")
            param = data.get("parameter")
            # Trích xuất parameter từ lookup nếu trong data chưa có (VD data từ stream không join trực tiếp)
            if not param:
                param = self.lookup.get(sensor_id)
            if not param: return
                
            # 3. Lọc vượt ngưỡng
            rule = self.rules.get(param, {})
            if val < rule.get("min", -999) or val > rule.get("max", 99999): return
                
            # Trích xuất ngày
            dt_local = data.get("datetime_local")
            date_local = dt_local[:10] if dt_local else None
            dt_utc = data.get("datetime_utc")
            loc_id = data.get("location_id")
            
            if not (loc_id and date_local and dt_utc): return
                
            # Emit
            yield (loc_id, param, date_local), (dt_utc, val)
        except Exception:
            pass

    # KHÔNG DÙNG COMBINER
    # Lý do đưa vào báo cáo kiến trúc:
    # Combiner làm tổng hợp cục bộ tại Mapper, tuy nhiên yêu cầu hệ thống là cần 
    # dedupe (xoá trùng lặp) theo trường `datetime_utc`.
    # Nếu dùng Combiner, nó sẽ tự tính sum/count cục bộ và ta mất đi thông tin datetime_utc để lọc.
    # Hai bản ghi trùng ở 2 mapper khác nhau sẽ cùng được cộng vào sum, gây sai lệch dữ liệu.

    def reducer(self, key, values):
        loc_id, param, date_local = key
        
        # Dedupe theo datetime_utc (trường hợp bị duplicate)
        unique_measurements = {}
        for dt_utc, val in values:
            unique_measurements[dt_utc] = val
            
        vals = list(unique_measurements.values())
        if not vals:
            return
            
        count = len(vals)
        total = sum(vals)
        max_val = max(vals)
        # Ngưỡng tượng trưng là 100 để báo cáo
        hours_over_100 = sum(1 for v in vals if v > 100)
        
        yield key, {
            "count": count,
            "sum": total,
            "max": max_val,
            "avg": total / count,
            "hours_over_100": hours_over_100
        }

if __name__ == '__main__':
    DailyStatsMR.run()
