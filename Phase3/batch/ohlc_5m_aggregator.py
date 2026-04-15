#!/usr/bin/env python3
"""
OHLC 5m Aggregator - Spark Batch Job
Lấy dữ liệu nến 1m từ Redis, gom thành nến 5m và lưu vào MongoDB.
"""
import os
import sys
# Ép Spark sử dụng đúng Python của môi trường ảo (venv)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import json,os
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, last, max as spark_max, min as spark_min, sum as spark_sum
import redis
from pymongo import MongoClient
from pymongo.errors import OperationFailure

# Cấu hình Database (Local)
# Cấu hình Redis (Local)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")  # Bỏ pass Redis

# Cấu hình MongoDB (Local)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "CRYPTO")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "5m_kline")

def get_active_symbols(mongo_uri, db_name, collection_name):
    """Tự động quét MongoDB để tìm tất cả các đồng coin đang có dữ liệu"""
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        # Dùng lệnh distinct để lấy ra tất cả các symbol khác nhau
        active_symbols = db[collection_name].distinct("symbol")
        client.close()
        
        return active_symbols
    except Exception as e:
        print(f"⚠️ Lỗi quét danh sách coin: {e}. Quay về danh sách mặc định 5 đồng.")
        # Đã sửa lại chuẩn 5 đồng cho khớp tuyệt đối với phương án dự phòng của Kafka
        return ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT"]

def main():
    print("=" * 70)
    print("🚀 Bắt đầu Job: Gom nến 1 phút thành 5 phút (OHLC 5m Aggregator)")
    print("=" * 70)

    # TỰ ĐỘNG LẤY DANH SÁCH COIN TỪ KHO DỮ LIỆU
    SYMBOLS = get_active_symbols(MONGO_URI, MONGO_DB, "1m_kline") 
    
    print(f"🔍 Phát hiện {len(SYMBOLS)} đồng coin đang có dữ liệu trong Database!")
    print(f"   Danh sách: {', '.join(SYMBOLS[:10])} ...")

    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("OHLC-5m-Aggregator") \
        .config("spark.sql.session.timeZone", "UTC")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 2. Kết nối MongoDB và TẠO INDEX
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    mongo_collection = mongo_db[MONGO_COLLECTION]
    
    print("  Đang kiểm tra Index cho MongoDB...")
    index_fields = [("symbol", 1), ("interval", 1), ("openTime", 1)]
    existing_indexes = mongo_collection.list_indexes()
    index_exists = False
    
    for idx in existing_indexes:
        idx_key = idx.get("key", {})
        if (idx_key.get("symbol") == 1 and 
            idx_key.get("interval") == 1 and 
            idx_key.get("openTime") == 1):
            index_exists = True
            print(f"  ℹ️  Index đã tồn tại: {idx.get('name', 'unnamed')}")
            break
    
    if not index_exists:
        try:
            mongo_collection.create_index(
                index_fields,
                unique=True,
                name="symbol_interval_openTime_unique"
            )
            print("  ✅ Tạo Index thành công")
        except OperationFailure as e:
            if e.code == 85:
                print(f"  ℹ️  Index đã tồn tại với tên khác, bỏ qua tạo mới")
            else:
                print(f"  ⚠️  Lỗi MongoDB khi tạo index (code {e.code}): {e}")
        except Exception as e:
            print(f"  ⚠️  Lỗi không xác định khi tạo index: {e}")

    # 3. Tính toán khung thời gian (Lấy 5 phút vừa chốt sổ)
    now = datetime.now(timezone.utc)
    current_minute = now.minute
    rounded_minute = (current_minute // 5) * 5
    window_end = now.replace(minute=rounded_minute, second=0, microsecond=0)
    window_start = window_end - timedelta(minutes=5)
    
    start_timestamp = int(window_start.timestamp() * 1000)
    end_timestamp = int(window_end.timestamp() * 1000)
    
    print(f"📅 Time window: {window_start} to {window_end}")
    print(f"   Timestamp: {start_timestamp} - {end_timestamp}")

    # 4. Kết nối Redis và lấy dữ liệu 1m
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    all_1m_klines = []
    
    for symbol in SYMBOLS:
        index_key = f"crypto:{symbol}:1m:index"
        timestamps = redis_client.zrangebyscore(index_key, start_timestamp, end_timestamp - 1)
        
        if not timestamps:
            print(f"  ⚠️  Không tìm thấy dữ liệu 1m cho {symbol} trong khoảng thời gian này.")
            continue
            
        print(f"  📥 Lấy được {len(timestamps)} nến 1m của {symbol} từ Redis.")
        
        for ts in sorted(timestamps):
            key = f"crypto:{symbol}:1m:{int(ts)}"
            data = redis_client.get(key)
            if data:
                kline = json.loads(data)
                if kline.get("x", False):
                    kline["symbol"] = symbol
                    all_1m_klines.append(kline)
                
    redis_client.close()

    if not all_1m_klines:
        print("❌ Không có dữ liệu để xử lý. Dừng Job.")
        spark.stop()
        mongo_client.close()
        return

    # 5. Đưa dữ liệu vào Spark DataFrame để tính toán
    df = spark.createDataFrame(all_1m_klines)
    
    # Convert string columns to appropriate types
    df = df.withColumn("open", col("open").cast("double")) \
           .withColumn("high", col("high").cast("double")) \
           .withColumn("low", col("low").cast("double")) \
           .withColumn("close", col("close").cast("double")) \
           .withColumn("volume", col("volume").cast("double")) \
           .withColumn("quoteVolume", col("quoteVolume").cast("double")) \
           .withColumn("trades", col("trades").cast("integer")) \
           .withColumn("openTime", col("openTime").cast("long")) \
           .withColumn("closeTime", col("closeTime").cast("long"))

    # Calculate 5m window start time for grouping (floor to 5m intervals)
    df = df.withColumn(
        "window_start",
        (col("openTime") / 300000).cast("long") * 300000
    )

    # Group by symbol and 5m window, aggregate OHLC
    aggregated_df = df.groupBy("symbol", "window_start").agg(
        first("open").alias("open"),              
        spark_max("high").alias("high"),          
        spark_min("low").alias("low"),            
        last("close").alias("close"),             
        spark_sum("volume").alias("volume"),      
        spark_sum("quoteVolume").alias("quoteVolume"),
        spark_sum("trades").alias("trades"),
        first("openTime").alias("openTime"),      
        last("closeTime").alias("closeTime")      
    )
    
    results = aggregated_df.collect()
    print(f"📊 Đã tính toán xong nến 5m cho {len(results)} đồng coin.")

    # 7. Lưu kết quả vào MongoDB
    total_inserted = 0
    total_skipped = 0
    
    for row in results:
        # Check if already exists
        existing = mongo_collection.find_one({
            "symbol": row["symbol"],
            "interval": "5m",
            "openTime": row["openTime"]
        })
        
        if existing:
            print(f"  ℹ️  5m OHLC already exists for {row['symbol']} (openTime: {row['openTime']})")
            total_skipped += 1
            continue
            
        mongo_doc = {
            "symbol": row["symbol"],
            "interval": "5m",
            "openTime": row["openTime"],
            "closeTime": row["closeTime"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "quoteVolume": row["quoteVolume"],
            "trades": row["trades"],
            "createdAt": datetime.now(),
            "source": "spark_5m_batch"
        }
        
        try:
            mongo_collection.update_one(
                {
                    "symbol": mongo_doc["symbol"],
                    "interval": mongo_doc["interval"],
                    "openTime": mongo_doc["openTime"]
                },
                {"$set": mongo_doc},
                upsert=True
            )
            print(f"  ✅ {row['symbol']}: 5m OHLC saved (O:{row['open']:.4f}, H:{row['high']:.4f}, L:{row['low']:.4f}, C:{row['close']:.4f})")
            total_inserted += 1
        except Exception as e:
            print(f"  ❌ Error saving {row['symbol']}: {e}")
            
    mongo_client.close()
    spark.stop()
    print("=" * 70)
    print(f"✅ Hoàn tất! Đã lưu: {total_inserted}, Bỏ qua (đã tồn tại): {total_skipped}")
    print("=" * 70)

if __name__ == "__main__":
    main()