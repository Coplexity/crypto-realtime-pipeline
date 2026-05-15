import json
import os
import time
import redis
from confluent_kafka import Consumer

# ─── Cấu hình ───
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "redis123")

# Kết nối Redis
r = redis.Redis(host=REDIS_HOST, port=6379, password=REDIS_PASSWORD, decode_responses=True)

# Cấu hình Kafka Consumer
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'fastapi_realtime_updater',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
# Lắng nghe cả 3 luồng dữ liệu cùng lúc
consumer.subscribe(['crypto.klines', 'crypto.trades', 'crypto.orderbook'])

print("🚀 Ticker Updater đang chạy và cập nhật Redis Real-time...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Lỗi Kafka: {msg.error()}")
        continue

    try:
        topic = msg.topic()
        val = json.loads(msg.value().decode('utf-8'))
        symbol = val.get("symbol")
        if not symbol: 
            continue

        # 1. Xử lý Nến (Kline)
        if topic == 'crypto.klines':
            interval = val.get("interval")
            # LƯU CHUNG CHO MỌI INTERVAL (1m, 5m, 1h...)
            r.set(f"crypto:{symbol}:{interval}:latest", json.dumps(val))
            
            if interval == "1m":
                # Key để cập nhật giá trên thanh Ticker Bar
                r.hset(f"ticker:{symbol}", mapping={"price": str(val["close"])})
        
        # 2. Xử lý Sổ lệnh (Orderbook)
        elif topic == 'crypto.orderbook':
            # Sửa lỗi logic của main.py: bắt buộc phải có lastUpdateId thì WebSocket mới chịu gửi data
            val["lastUpdateId"] = val.get("timestamp", int(time.time() * 1000))
            r.set(f"orderbook:{symbol}:latest", json.dumps(val))
        
        # 3. Xử lý Lịch sử giao dịch (Trades)
        elif topic == 'crypto.trades':
            score = val.get("trade_time", int(time.time() * 1000))
            key = f"trades:{symbol}:list"
            # Thêm vào Sorted Set của Redis theo thứ tự thời gian
            r.zadd(key, {json.dumps(val): score})
            # Chỉ giữ lại 100 giao dịch mới nhất cho nhẹ RAM
            r.zremrangebyrank(key, 0, -101)

    except Exception as e:
        print(f"Lỗi xử lý tin nhắn: {e}")