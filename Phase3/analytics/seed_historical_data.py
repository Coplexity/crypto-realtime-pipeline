import requests
import time
from pymongo import MongoClient

# Cấu hình MongoDB
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "CRYPTO"
COLLECTION = "1h_kline"

SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]

def fetch_binance_history():
    print("⏳ Đang kéo dữ liệu lịch sử 30 ngày từ Binance...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[COLLECTION]
    
    # Tạo Index cho chắc kèo
    col.create_index([("symbol", 1), ("interval", 1), ("openTime", 1)], unique=True)
    
    for symbol in SYMBOLS:
        # Gọi API của Binance (interval 1h, limit 720 nến = 30 ngày)
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1h&limit=720"
        response = requests.get(url)
        data = response.json()
        
        docs = []
        for row in data:
            docs.append({
                "symbol": symbol,
                "interval": "1h",
                "openTime": int(row[0]),
                "closeTime": int(row[6]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "quoteVolume": float(row[7]),
                "trades": int(row[8])
            })
            
        # Bơm thẳng vào MongoDB
        try:
            col.insert_many(docs, ordered=False) # ordered=False giúp bỏ qua lỗi trùng lặp
        except Exception as e:
            pass # Lờ đi nếu có nến đã tồn tại
            
        print(f"✅ Đã bơm xong 720 cây nến 1h cho {symbol}")
        time.sleep(1) # Nghỉ 1 giây tránh bị Binance chặn IP
        
    print("🎉 Hoàn tất Backfilling! Kho dữ liệu đã ngập tràn Data 30 ngày qua!")
    client.close()

if __name__ == "__main__":
    fetch_binance_history()