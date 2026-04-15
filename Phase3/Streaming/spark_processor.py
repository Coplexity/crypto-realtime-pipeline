"""
Spark Structured Streaming Processor - ĐÃ NÂNG CẤP TOÀN DIỆN (Chỉ tính 1 phút)

Xử lý dữ liệu stream real-time từ Kafka topic để tính toán OHLCV 1m.
Đã bổ sung quoteVolume, trades và tự động tạo Index MongoDB.
"""

import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, max, min, avg, sum, count,
    first, last
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
import redis
from pymongo import MongoClient
from pymongo.errors import OperationFailure

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cấu hình Database (Nhớ kiểm tra lại mk MongoDB của máy bạn)
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MONGO_URI = "mongodb://localhost:27017/" 
MONGO_DB = "CRYPTO"

def create_spark_session(app_name: str = "CryptoProcessor-1m") -> SparkSession:
    """Create optimized Spark session for streaming."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def get_schema() -> StructType:
    """Define JSON schema for trade messages."""
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("trade_id", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("buyer_order_id", LongType(), True),
        StructField("seller_order_id", LongType(), True),
        StructField("trade_time", LongType(), True),
        StructField("is_buyer_maker", StringType(), True),
        StructField("is_best_match", StringType(), True),
        StructField("ingestion_timestamp", LongType(), True),
    ])

def write_to_databases(batch_df, batch_id):
    """
    Hàm được gọi mỗi khi có Micro-batch 1 phút mới.
    Ghi vào Redis (Cache real-time, giữ 24h) và MongoDB (Lưu trữ vĩnh viễn).
    """
    pdf = batch_df.toPandas()
    if pdf.empty:
        return

    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB]
        mongo_collection = mongo_db["1m_kline"]
        pipe = redis_client.pipeline()

        for index, row in pdf.iterrows():
            symbol = row['symbol']
            
            ts_start = int(row['start_time'].timestamp() * 1000)
            ts_end = int(row['end_time'].timestamp() * 1000)
            
            data_dict = {
                "symbol": symbol,
                "interval": "1m",
                "openTime": ts_start,
                "closeTime": ts_end,
                "open": float(row['open_price']),
                "high": float(row['high_price']),
                "low": float(row['low_price']),
                "close": float(row['close_price']),
                "volume": float(row['volume']),
                "quoteVolume": float(row['quoteVolume']), # Đã thêm cột này
                "trades": int(row['trades']),             # Đã đổi tên chuẩn xác
                "x": True # Đánh dấu nến đã đóng
            }
            
            # --- 1. GHI VÀO REDIS ---
            redis_key = f"crypto:{symbol}:1m:{ts_start}"
            index_key = f"crypto:{symbol}:1m:index"
            
            pipe.setex(redis_key, 86400, json.dumps(data_dict)) # Tự hủy sau 24h
            pipe.zadd(index_key, {str(ts_start): ts_start}) # Lưu mốc thời gian làm Index
            
            # Dọn dẹp index cũ hơn 24h
            twenty_four_hours_ago = ts_start - (24 * 60 * 60 * 1000)
            pipe.zremrangebyscore(index_key, "-inf", twenty_four_hours_ago)
            
            # --- 2. GHI VÀO MONGODB ---
            mongo_collection.update_one(
                {"symbol": symbol, "interval": "1m", "openTime": ts_start},
                {"$set": data_dict},
                upsert=True
            )

        pipe.execute()
        redis_client.close()
        mongo_client.close()
        logger.info(f"Batch {batch_id}: Đã lưu {len(pdf)} nến 1 phút vào Redis & MongoDB")

    except Exception as e:
        logger.error(f"Lỗi khi ghi vào Database tại Batch {batch_id}: {e}")

def main():
    logger.info("=" * 70)
    logger.info("PHASE 3: Spark Streaming Processor (Chỉ OHLCV 1m)")
    logger.info("=" * 70)
    
    # --- ĐOẠN KIỂM TRA VÀ TẠO INDEX MONGODB TỰ ĐỘNG ---
    logger.info("Đang kiểm tra và tạo Index cho MongoDB (1m_kline)...")
    try:
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB]
        mongo_collection = mongo_db["1m_kline"]
        
        index_fields = [("symbol", 1), ("interval", 1), ("openTime", 1)]
        existing_indexes = mongo_collection.list_indexes()
        index_exists = False
        
        for idx in existing_indexes:
            idx_key = idx.get("key", {})
            if (idx_key.get("symbol") == 1 and 
                idx_key.get("interval") == 1 and 
                idx_key.get("openTime") == 1):
                index_exists = True
                logger.info(f"  ℹ️  Index đã tồn tại: {idx.get('name', 'unnamed')}")
                break
                
        if not index_exists:
            try:
                mongo_collection.create_index(
                    index_fields,
                    unique=True,
                    name="symbol_interval_openTime_unique"
                )
                logger.info("  ✅ Tạo Index thành công!")
            except OperationFailure as e:
                if e.code == 85:
                    logger.info("  ℹ️  Index đã tồn tại với tên khác, bỏ qua.")
                else:
                    logger.warning(f"  ⚠️  Lỗi tạo index MongoDB (code {e.code}): {e}")
        mongo_client.close()
    except Exception as e:
        logger.error(f"  ❌ Không thể kết nối MongoDB để tạo index: {e}")
    # --------------------------------------------------

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Hứng dữ liệu từ Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "binance_trades") \
        .option("startingOffsets", "latest") \
        .load()
    
    schema = get_schema()
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    # Tiền xử lý: Ép kiểu thời gian và tính giá trị giao dịch của từng lệnh (quote_asset_volume)
    processed_df = parsed_df \
        .withColumn("timestamp", (col("trade_time") / 1000).cast("timestamp")) \
        .withColumn("quote_asset_volume", col("price") * col("quantity")) \
        .withWatermark("timestamp", "10 seconds")
    
    logger.info("Bắt đầu tính toán nến OHLCV 1m...")

    # Tính toán Nến 1 Phút
    ohlcv_1m_df = processed_df \
        .groupBy(window(col("timestamp"), "1 minute"), col("symbol")) \
        .agg(
            first("price").alias("open_price"),
            max("price").alias("high_price"),
            min("price").alias("low_price"),
            last("price").alias("close_price"),
            sum("quantity").alias("volume"),
            sum("quote_asset_volume").alias("quoteVolume"), # MỚI: Tính tổng USDT
            count("trade_id").alias("trades"),              # SỬA: Đổi tên thành trades
        ).selectExpr(
            "window.start as start_time",
            "window.end as end_time",
            "symbol", "open_price", "high_price", "low_price", "close_price", 
            "volume", "quoteVolume", "trades"
        )
    
    # Đẩy kết quả vào Databases
    query = ohlcv_1m_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_databases) \
        .start()
    
    logger.info("Bắt đầu luồng ghi dữ liệu... (Ctrl+C để dừng)")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Dừng processing...")
    finally:
        query.stop()
        spark.stop()
        logger.info("✓ Spark session đóng")

if __name__ == "__main__":
    main()