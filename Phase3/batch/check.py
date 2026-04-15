import redis
from datetime import datetime

print("🔍 ĐANG SOI BỘ NHỚ REDIS...")
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Tìm tất cả các key chứa index nến 1 phút
keys = r.keys("crypto:*:1m:index")

if not keys:
    print("❌ Báo cáo: Redis hiện tại hoàn toàn TRỐNG RỖNG!")
else:
    for key in keys:
        ts_list = r.zrange(key, 0, -1)
        print(f"\n📦 Tìm thấy key: {key} (Chứa {len(ts_list)} cây nến)")
        
        if ts_list:
            first_time = datetime.fromtimestamp(int(ts_list[0]) / 1000)
            last_time = datetime.fromtimestamp(int(ts_list[-1]) / 1000)
            
            print(f"   ⏱️ Nến cũ nhất: {first_time}")
            print(f"   ⏱️ Nến mới nhất: {last_time}")