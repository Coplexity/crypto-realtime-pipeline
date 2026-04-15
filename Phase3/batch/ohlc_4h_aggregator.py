"""
OHLC 4h Aggregator - Spark Batch Job
Lấy 4 nến 1h từ MongoDB gom thành nến 4h (Khớp chuẩn khung giờ UTC của Binance)
(Phiên bản Enterprise)
"""
import os
import sys

# ĐÁNH BAY LỖI WINDOWS
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, last, max as spark_max, min as spark_min, sum as spark_sum
from pymongo import MongoClient
from pymongo.errors import OperationFailure

# CẤU HÌNH LOCAL
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "CRYPTO")
SOURCE_COLLECTION = "1h_kline" 
TARGET_COLLECTION = "4h_kline"   

def main():
    print("="*70)
    print("🚀 Khởi động luồng Spark: Gom nến 1h -> 4h (Phiên bản Enterprise)")
    print("="*70)

    # 1. Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("OHLC-4h-Aggregator") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    # 2. Kết nối MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    source_collection = mongo_db[SOURCE_COLLECTION]
    target_collection = mongo_db[TARGET_COLLECTION]
    
    # 3. KIỂM TRA INDEX 
    index_fields = [("symbol", 1), ("interval", 1), ("openTime", 1)]
    existing_indexes = target_collection.list_indexes()
    index_exists = False
    
    print("  Đang kiểm tra Index cho MongoDB (4h_kline)...")
    for idx in existing_indexes:
        idx_key = idx.get("key", {})
        if idx_key.get("symbol") == 1 and idx_key.get("interval") == 1 and idx_key.get("openTime") == 1:
            index_exists = True
            print(f"  ℹ️ Index 4h_kline đã tồn tại: {idx.get('name', 'unnamed')}")
            break
    
    if not index_exists:
        try:
            target_collection.create_index(index_fields, unique=True, name="symbol_interval_openTime_unique")
            print("  ✅ Tạo Index 4h_kline thành công!")
        except OperationFailure as e:
            if e.code == 85:
                print("  ℹ️ Index 4h_kline đã tồn tại với tên khác, bỏ qua việc tạo mới.")
            else:
                print(f"  ⚠️ Lỗi tạo Index MongoDB (code {e.code}): {e}")
        except Exception as e:
            print(f"  ⚠️ Lỗi không xác định khi tạo Index: {e}")
    
    # 4. THUẬT TOÁN KHUNG GIỜ 4H CHUẨN BINANCE
    # Các mốc giờ nến 4h: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00
    now = datetime.now(timezone.utc)
    current_block_hour = (now.hour // 4) * 4  # Ép giờ hiện tại về mốc chia hết cho 4 gần nhất
    
    # Đây là thời điểm KẾT THÚC của cây nến 4h vừa đóng sổ
    window_end = now.replace(hour=current_block_hour, minute=0, second=0, microsecond=0)
    # Lùi lại 4 tiếng để ra thời điểm BẮT ĐẦU
    window_start = window_end - timedelta(hours=4)
    
    start_timestamp = int(window_start.timestamp() * 1000)
    end_timestamp = int(window_end.timestamp() * 1000)
    
    print(f"📅 Time window (UTC): {window_start} to {window_end}")
    print(f"   Timestamp: {start_timestamp} - {end_timestamp}")
    
    # Lấy danh sách các đồng coin đang có dữ liệu trong 4 giờ này
    symbols_in_db = source_collection.distinct("symbol", {"openTime": {"$gte": start_timestamp, "$lt": end_timestamp}})
    
    if not symbols_in_db:
        print("  ⚠️ Không tìm thấy nến 1h nào trong khung giờ 4h này.")
        spark.stop()
        mongo_client.close()
        return

    # 5. Kéo dữ liệu BSON từ Mongo lên
    query = {
        "symbol": {"$in": symbols_in_db},
        "interval": "1h",
        "openTime": {"$gte": start_timestamp, "$lt": end_timestamp}
    }
    klines = list(source_collection.find(query).sort("openTime", 1)) 
    
    print(f"  📥 Lấy được {len(klines)} nến 1h từ MongoDB. Bắt đầu đẩy vào Spark...")
    
    # 6. Tiền xử lý (Lập trình phòng thủ)
    from bson import Int64, Decimal128
    normalized_klines = []
    for kline in klines:
        if not isinstance(kline, dict): continue
        nk = dict(kline)
        nk.pop("_id", None)
        
        for key in ["openTime", "closeTime", "trades"]:
            if key in nk and isinstance(nk[key], Int64): nk[key] = int(nk[key])
                
        for key in ["open", "high", "low", "close", "volume", "quoteVolume"]:
            if key in nk:
                if isinstance(nk[key], (Int64, Decimal128)): nk[key] = float(nk[key])
                elif nk[key] is not None:
                    try: nk[key] = float(nk[key])
                    except (ValueError, TypeError): pass
                        
        for key in ["symbol", "interval"]:
            if key in nk and nk[key] is not None: nk[key] = str(nk[key])
                
        normalized_klines.append(nk)
    
    # 7. Đưa vào Spark DataFrame và ép kiểu cột
    df = spark.createDataFrame(normalized_klines)
    df = df.withColumn("open", col("open").cast("double")) \
           .withColumn("high", col("high").cast("double")) \
           .withColumn("low", col("low").cast("double")) \
           .withColumn("close", col("close").cast("double")) \
           .withColumn("volume", col("volume").cast("double")) \
           .withColumn("quoteVolume", col("quoteVolume").cast("double")) \
           .withColumn("trades", col("trades").cast("integer")) \
           .withColumn("openTime", col("openTime").cast("long")) \
           .withColumn("closeTime", col("closeTime").cast("long"))
    
    # 8. Sức mạnh của Spark: Gom nhóm theo Symbol và Time (14400000 mili-giây = 4 Giờ)
    df = df.withColumn("window_start", (col("openTime") / 14400000).cast("long") * 14400000)
    
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
    
    # 9. Lưu kết quả lại vào MongoDB
    total_inserted = 0
    total_skipped = 0
    
    for row in results:
        existing = target_collection.find_one({
            "symbol": row["symbol"],
            "interval": "4h",
            "openTime": row["openTime"]
        })
        
        if existing:
            print(f"  ⏭️ 4h OHLC already exists for {row['symbol']} (openTime: {row['openTime']})")
            total_skipped += 1
            continue
            
        mongo_doc = {
            "symbol": row["symbol"],
            "interval": "4h",
            "openTime": row["openTime"],
            "closeTime": row["closeTime"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "quoteVolume": row["quoteVolume"],
            "trades": row["trades"],
            "createdAt": datetime.now(timezone.utc).isoformat(),
            "source": "spark_4h_aggregator"
        }
        
        try:
            target_collection.update_one(
                {"symbol": mongo_doc["symbol"], "interval": mongo_doc["interval"], "openTime": mongo_doc["openTime"]},
                {"$set": mongo_doc},
                upsert=True
            )
            print(f"  ✅ {row['symbol']}: 4h OHLC saved (O:{row['open']:.2f}, C:{row['close']:.2f})")
            total_inserted += 1
        except Exception as e:
            print(f"  ❌ Error saving {row['symbol']}: {e}")
            
    print("\n" + "="*70)
    print(f"✅ Hoàn tất Job Spark! Đã xử lý và lưu: {total_inserted} cây nến 4h.")
    print("="*70)
    
    spark.stop()
    mongo_client.close()

if __name__ == "__main__":
    main()