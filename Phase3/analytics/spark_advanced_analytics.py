#!/usr/bin/env python3
"""
Job Phân tích Nâng cao (Spark Advanced Analytics)
Tính toán các chỉ báo Phân tích Kỹ thuật chuyên sâu (RSI, SMA, Bollinger Bands, MACD)
Cung cấp dữ liệu làm giàu (Enriched Data) cho Dashboard và Web/App
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, avg, stddev, when, lit, greatest, least, abs as spark_abs
)
from pyspark.sql.window import Window
from pymongo import MongoClient

# ==========================================
# CẤU HÌNH HỆ THỐNG
# ==========================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "CRYPTO")
SOURCE_COLLECTION = "1h_kline"            # Dùng nến 1h để phân tích xu hướng là chuẩn nhất
TARGET_COLLECTION = "market_analytics"    # Lưu vào một kho riêng chuyên cho Dashboard

# Lấy dữ liệu 30 ngày gần nhất (Đủ sâu để tính SMA 50)
LOOKBACK_DAYS = 30

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
    sys.stdout.flush()
    start_time = datetime.now(timezone.utc)
    print("=" * 80)
    print("📈 BẮT ĐẦU CHU TRÌNH PHÂN TÍCH THỊ TRƯỜNG TÀI CHÍNH (TECHNICAL ANALYTICS)")
    print(f"⏰ Thời gian: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)
    sys.stdout.flush()

    # Sửa lỗi Worker cho Windows
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    # TỰ ĐỘNG LẤY DANH SÁCH COIN TỪ KHO DỮ LIỆU
    SYMBOLS = get_active_symbols(MONGO_URI, MONGO_DB, SOURCE_COLLECTION)
    
    print(f"🔍 Phát hiện {len(SYMBOLS)} đồng coin đang có dữ liệu trong Database!")
    print(f"   Danh sách: {', '.join(SYMBOLS[:10])} ...")

    # 1. Khởi tạo Spark
    print("\n[1/5] 🔧 Đang khởi tạo Spark Session...")
    sys.stdout.flush()
    spark = SparkSession.builder \
        .appName("Crypto-Technical-Analytics") \
        .config("spark.mongodb.read.connection.uri", MONGO_URI) \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Đã khởi tạo Spark Session thành công")

    # 2. Kéo dữ liệu từ MongoDB
    print(f"\n[2/5] 📥 Đang kéo lịch sử {LOOKBACK_DAYS} ngày của nến 1h...")
    sys.stdout.flush()
    
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    source_col = db[SOURCE_COLLECTION]
    
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    start_ts = int(start_dt.timestamp() * 1000)
    
    query = {"symbol": {"$in": SYMBOLS}, "openTime": {"$gte": start_ts}}
    raw_data = list(source_col.find(query).sort("openTime", 1))
    
    if not raw_data:
        print("❌ Không có dữ liệu 1h nào trong DB. Hãy chạy gom nến trước!")
        spark.stop()
        client.close()
        return
        
    print(f"✅ Lấy thành công {len(raw_data)} cây nến. Đang chuẩn hóa đẩy vào Spark...")
    
    # Chuẩn hóa BSON
    normalized_data = []
    for d in raw_data:
        if not isinstance(d, dict): continue
        nd = dict(d)
        nd.pop("_id", None)
        for key in ["openTime", "closeTime"]:
            if key in nd: nd[key] = int(nd[key])
        for key in ["open", "high", "low", "close", "volume"]:
            if key in nd:
                try: nd[key] = float(nd[key])
                except: pass
        normalized_data.append(nd)
        
    df = spark.createDataFrame(normalized_data)
    
    # 3. KỸ THUẬT PHÂN TÍCH CHUYÊN SÂU (ADVANCED ANALYTICS)
    print("\n[3/5] 🧠 Đang tính toán các Bộ chỉ báo (RSI, Trend, Bollinger Bands)...")
    sys.stdout.flush()
    
    window_spec = Window.partitionBy("symbol").orderBy("openTime")
    
    # --- A. Nhận diện Xu hướng bằng Đường Trung bình (SMA 20 & SMA 50) ---
    df = df.withColumn("SMA_20", avg("close").over(window_spec.rowsBetween(-19, 0)))
    df = df.withColumn("SMA_50", avg("close").over(window_spec.rowsBetween(-49, 0)))
    df = df.withColumn("Trend", when(col("SMA_20") > col("SMA_50"), lit("Bullish (Tăng)")).otherwise(lit("Bearish (Giảm)")))

    # --- B. Dải Bollinger Bands (Đo độ biến động & Quá Mua/Quá Bán) ---
    df = df.withColumn("StdDev_20", stddev("close").over(window_spec.rowsBetween(-19, 0)))
    df = df.withColumn("Bollinger_Upper", col("SMA_20") + (col("StdDev_20") * 2))
    df = df.withColumn("Bollinger_Lower", col("SMA_20") - (col("StdDev_20") * 2))
    
    df = df.withColumn("BB_Signal", 
                       when(col("close") >= col("Bollinger_Upper"), lit("Overbought (Quá Mua)"))
                       .when(col("close") <= col("Bollinger_Lower"), lit("Oversold (Quá Bán)"))
                       .otherwise(lit("Neutral (Bình thường)")))

    # --- C. Chỉ số RSI (14) ---
    df = df.withColumn("prev_close", lag("close", 1).over(window_spec))
    df = df.withColumn("price_change", col("close") - col("prev_close"))
    df = df.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
    df = df.withColumn("loss", when(col("price_change") < 0, spark_abs(col("price_change"))).otherwise(0))
    
    df = df.withColumn("avg_gain_14", avg("gain").over(window_spec.rowsBetween(-13, 0)))
    df = df.withColumn("avg_loss_14", avg("loss").over(window_spec.rowsBetween(-13, 0)))
    
    df = df.withColumn("RS", when(col("avg_loss_14") == 0, lit(100)).otherwise(col("avg_gain_14") / col("avg_loss_14")))
    df = df.withColumn("RSI_14", when(col("avg_loss_14") == 0, lit(100)).otherwise(100 - (100 / (1 + col("RS")))))

    # 4. Trích xuất cây nến mới nhất
    print("\n[4/5] 📊 Đang trích xuất Báo cáo mới nhất cho toàn thị trường...")
    from pyspark.sql.functions import row_number
    window_latest = Window.partitionBy("symbol").orderBy(col("openTime").desc())
    
    df_latest = df.withColumn("rn", row_number().over(window_latest)) \
                  .filter(col("rn") == 1) \
                  .drop("rn", "prev_close", "price_change", "gain", "loss", "avg_gain_14", "avg_loss_14", "RS", "StdDev_20")
    
    print("\n" + "=" * 90)
    print(f"{'Coin':<10} | {'Giá Hiện Tại':<14} | {'RSI':<6} | {'Trạng Thái (Bollinger)':<25} | {'Xu hướng (Trend)':<15}")
    print("-" * 90)
    
    results = df_latest.collect()
    for row in results:
        symbol = row["symbol"]
        price = float(row["close"])
        rsi = float(row["RSI_14"]) if row["RSI_14"] is not None else 50.0
        bb_signal = row["BB_Signal"]
        trend = row["Trend"]
        
        trend_icon = "🚀 " + trend if "Bullish" in trend else "🐻 " + trend
        print(f"{symbol:<10} | ${price:<13.2f} | {rsi:.1f:<6} | {bb_signal:<25} | {trend_icon:<15}")
    print("=" * 90)

    # 5. Lưu vào MongoDB
    print(f"\n[5/5] 💾 Đang lưu {len(results)} bản ghi phân tích vào collection '{TARGET_COLLECTION}'...")
    target_col = db[TARGET_COLLECTION]
    target_col.create_index([("symbol", 1)], unique=True)
    
    inserted_count = 0
    for row in results:
        doc = row.asDict()
        doc["updated_at"] = datetime.now(timezone.utc).isoformat()
        try:
            target_col.update_one({"symbol": doc["symbol"]}, {"$set": doc}, upsert=True)
            inserted_count += 1
        except Exception as e:
            print(f"  ❌ Lỗi lưu {doc['symbol']}: {e}")
            
    print(f"✅ Đã lưu thành công {inserted_count} bản báo cáo!")
    
    spark.stop()
    client.close()

if __name__ == "__main__":
    main()