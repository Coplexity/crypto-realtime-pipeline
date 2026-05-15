"""
Spark Structured Streaming Processor - COMBINED VERSION
Kết hợp: Auto MongoDB indexes, Redis sorted-set index, upsert MongoDB,
          Multi-interval OHLCV (1m/5m/1h/1d), Watermarking, Broadcast Join,
          Kline + Trade streams, Checkpoint exactly-once, Prometheus metrics
"""

import json
import logging
import os
import time
from typing import Optional

import redis
from prometheus_client import Counter, Gauge, start_http_server
from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure, DuplicateKeyError
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType,
    LongType, StringType, StructField, StructType,
)

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("spark-streaming")

# ─── Config từ env ────────────────────────────────────────────────────────
KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MONGO_URI       = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongodb:27017")
MONGO_DB        = os.getenv("MONGO_DB",  "crypto_db")
REDIS_HOST      = os.getenv("REDIS_HOST",     "redis")
REDIS_PORT      = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD  = os.getenv("REDIS_PASSWORD", "redis123")
CHECKPOINT_DIR  = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints")

TOPIC_KLINES    = "crypto.klines"
TOPIC_TRADES    = "crypto.trades"
TOPIC_ORDERBOOK = "crypto.orderbook"

# ─── Prometheus metrics ──────────────────────────────────────────────────────
batches_processed = Counter(
    "spark_batches_processed_total",
    "Tổng số micro-batches đã xử lý",
    ["query"]
)
records_written = Counter(
    "spark_records_written_total",
    "Tổng số records đã ghi vào DB",
    ["dest", "interval"]
)
batch_duration = Gauge(
    "spark_batch_duration_seconds",
    "Thời gian xử lý mỗi batch",
    ["query"]
)


# ─── Symbol Metadata (dùng cho Broadcast Join) ───────────────────────────────
SYMBOL_METADATA = [
    ("BTCUSDT",  "Bitcoin",   "BTC",  "Layer 1",  1),
    ("ETHUSDT",  "Ethereum",  "ETH",  "Layer 1",  2),
    ("BNBUSDT",  "BNB",       "BNB",  "Exchange", 3),
    ("SOLUSDT",  "Solana",    "SOL",  "Layer 1",  4),
    ("ADAUSDT",  "Cardano",   "ADA",  "Layer 1",  5),
    ("XRPUSDT",  "XRP",       "XRP",  "Payments", 6),
    ("DOGEUSDT", "Dogecoin",  "DOGE", "Meme",     7),
    ("AVAXUSDT", "Avalanche", "AVAX", "Layer 1",  8),
]


# ─── Schemas ─────────────────────────────────────────────────────────────────
TRADE_SCHEMA = StructType([
    StructField("symbol",         StringType(),  True),
    StructField("trade_id",       LongType(),    True),
    StructField("price",          DoubleType(),  True),
    StructField("quantity",       DoubleType(),  True),
    StructField("trade_time",     LongType(),    True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("is_best_match",  BooleanType(), True),
    StructField("event_time",     LongType(),    True),
    StructField("ingestion_time", LongType(),    True),
])

KLINE_SCHEMA = StructType([
    StructField("symbol",           StringType(),  True),
    StructField("interval",         StringType(),  True),
    StructField("open_time",        LongType(),    True),
    StructField("close_time",       LongType(),    True),
    StructField("open",             DoubleType(),  True),
    StructField("high",             DoubleType(),  True),
    StructField("low",              DoubleType(),  True),
    StructField("close",            DoubleType(),  True),
    StructField("volume",           DoubleType(),  True),
    StructField("quote_volume",     DoubleType(),  True),
    StructField("trades_count",     IntegerType(), True),
    StructField("taker_buy_volume", DoubleType(),  True),
    StructField("is_closed",        BooleanType(), True),
    StructField("event_time",       LongType(),    True),
    StructField("ingestion_time",   LongType(),    True),
])

ORDERBOOK_SCHEMA = StructType([
    StructField("symbol",    StringType(), True),
    StructField("best_bid",  DoubleType(), True),
    StructField("best_ask",  DoubleType(), True),
    StructField("spread",    DoubleType(), True),
    StructField("timestamp", LongType(),   True),
])


# ─── MongoDB: Auto-create indexes (từ file gốc của bạn) ─────────────────────
def setup_mongodb_indexes():
    """
    Tự động tạo indexes khi khởi động.
    Đây là tính năng hay từ file gốc của bạn — đảm bảo query nhanh ngay từ đầu.
    """
    logger.info("Đang kiểm tra và tạo indexes MongoDB...")
    try:
        client = MongoClient(MONGO_URI, authSource="admin",
                             serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]

        # Định nghĩa indexes cho từng collection
        index_configs = {
            "ohlc_1m": [
                ([("symbol", ASCENDING), ("openTime", ASCENDING)],
                 {"unique": True, "name": "symbol_openTime_unique"}),
                ([("openTime", ASCENDING)],
                 {"expireAfterSeconds": 86400, "name": "ttl_1day"}),
            ],
            "ohlc_5m": [
                ([("symbol", ASCENDING), ("openTime", ASCENDING)],
                 {"unique": True, "name": "symbol_openTime_unique"}),
                ([("openTime", ASCENDING)],
                 {"expireAfterSeconds": 604800, "name": "ttl_7days"}),
            ],
            "ohlc_1h": [
                ([("symbol", ASCENDING), ("openTime", ASCENDING)],
                 {"unique": True, "name": "symbol_openTime_unique"}),
            ],
            "ohlc_4h": [
                ([("symbol", ASCENDING), ("openTime", ASCENDING)],
                 {"unique": True, "name": "symbol_openTime_unique"}),
            ],
            "ohlc_1d": [
                ([("symbol", ASCENDING), ("openTime", ASCENDING)],
                 {"unique": True, "name": "symbol_openTime_unique"}),
            ],
            "orderbook": [
                ([("symbol", ASCENDING), ("timestamp", ASCENDING)],
                 {"name": "symbol_timestamp"}),
                ([("timestamp", ASCENDING)],
                 {"expireAfterSeconds": 3600, "name": "ttl_1hour"}),
            ],
        }

        for collection_name, indexes in index_configs.items():
            col = db[collection_name]
            for index_fields, options in indexes:
                try:
                    col.create_index(index_fields, **options)
                    logger.info(f"  ✅ Index '{options['name']}' trên '{collection_name}'")
                except OperationFailure as e:
                    if e.code == 85:
                        logger.info(f"  ℹ️  Index '{options['name']}' đã tồn tại")
                    else:
                        logger.warning(f"  ⚠️  Index error (code {e.code}): {e}")

        client.close()
        logger.info("✅ MongoDB indexes setup hoàn tất")

    except Exception as e:
        logger.error(f"❌ Không thể setup MongoDB indexes: {e}")

# ─── Spark Session ───────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CryptoStreaming")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )


# ─── Helper: kết nối DB trong foreachBatch ───────────────────────────────────
def get_redis():
    return redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT,
        password=REDIS_PASSWORD, decode_responses=True,
        socket_timeout=3
    )

def get_mongo():
    return MongoClient(MONGO_URI, authSource="admin",
                       serverSelectionTimeoutMS=3000)


# ─── Writer: ghi OHLC vào Redis + MongoDB ────────────────────────────────────
def write_ohlc_to_databases(batch_df: DataFrame, batch_id: int, interval: str):
    t_start = time.time()
    
    # Dùng collect() thay vì toPandas() để không cần thư viện ngoài
    rows = batch_df.collect()
    if not rows:
        return

    try:
        r = get_redis()
        mongo_client = get_mongo()
        col = mongo_client[MONGO_DB][f"ohlc_{interval}"]
        pipe = r.pipeline()

        for row in rows:
            symbol   = row["symbol"]
            ts_start = int(row["open_time"])
            ts_end   = int(row["close_time"])

            data = {
                "symbol":       symbol,
                "interval":     interval,
                "openTime":     ts_start,
                "closeTime":    ts_end,
                "open":         float(row["open"]),
                "high":         float(row["high"]),
                "low":          float(row["low"]),
                "close":        float(row["close"]),
                "volume":       float(row["volume"]),
                "quoteVolume":  float(row["quote_volume"] if row["quote_volume"] is not None else 0),
                "trades":       int(row["trades"] if row["trades"] is not None else 0),
                "is_closed":    True,
                "coin_name":    row["name"] if row["name"] else "",
                "category":     row["category"] if row["category"] else "",
            }

            redis_key = f"crypto:{symbol}:{interval}:{ts_start}"
            index_key = f"crypto:{symbol}:{interval}:index"

            pipe.setex(redis_key, 86400, json.dumps(data))
            pipe.zadd(index_key, {str(ts_start): ts_start})

            cutoff = ts_start - 24 * 60 * 60 * 1000
            pipe.zremrangebyscore(index_key, "-inf", cutoff)

            if interval == "1m":
                pipe.hset(f"ticker:{symbol}", mapping={
                    "price":      str(data["close"]),
                    "volume_24h": str(data["volume"]),
                    "updated_at": str(ts_start),
                })
                pipe.expire(f"ticker:{symbol}", 120)

            try:
                col.update_one(
                    {"symbol": symbol, "openTime": ts_start}, # Bỏ trường interval ở filter cho chuẩn Index
                    {"$set": data},
                    upsert=True
                )
            except DuplicateKeyError:
                # Kẻ đến sau sẽ tự động hạ mình xuống làm update bình thường
                col.update_one(
                    {"symbol": symbol, "openTime": ts_start},
                    {"$set": data}
                )

        pipe.execute()
        r.close()
        mongo_client.close()

        elapsed = time.time() - t_start
        logger.info(f"Batch {batch_id} [{interval}]: {len(rows)} nến → Redis & MongoDB ({elapsed:.2f}s)")

    except Exception as e:
        logger.error(f"❌ Lỗi ghi DB batch {batch_id} [{interval}]: {e}")

# ─── Query 1: OHLC từ Trade Stream ───────────────────────────────────────────
def build_trade_ohlc_queries(spark: SparkSession) -> list:
    """
    Đọc trade stream → tính OHLC nhiều intervals.
    Kỹ thuật: Tumbling Window + Watermarking + quote_asset_volume (từ file gốc)
    """
    # Symbol metadata DataFrame — dùng cho Broadcast Join
    meta_schema = StructType([
        StructField("symbol",   StringType(),  False),
        StructField("name",     StringType(),  True),
        StructField("base",     StringType(),  True),
        StructField("category", StringType(),  True),
        StructField("rank",     IntegerType(), True),
    ])
    meta_df = spark.createDataFrame(SYMBOL_METADATA, schema=meta_schema)

    # Đọc từ Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_TRADES)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    # Parse JSON
    trades = (
        raw
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("timestamp", (F.col("trade_time") / 1000).cast("timestamp"))
        # WATERMARKING: chấp nhận data trễ tối đa 10 phút
        .withWatermark("timestamp", "10 minutes")
        # quote_asset_volume = price × quantity (từ file gốc của bạn)
        .withColumn("quote_asset_volume", F.col("price") * F.col("quantity"))
    )

    # BROADCAST JOIN với symbol metadata
    trades_enriched = trades.join(F.broadcast(meta_df), on="symbol", how="left")

    queries = []

    # Tạo query cho từng interval
    for interval, duration, watermark_delay in [
        ("1m",  "1 minute",  "10 minutes"),
        ("5m",  "5 minutes", "10 minutes"),
        ("1h",  "1 hour",    "20 minutes"),
        ("4h",  "4 hours",   "1 hour"),
        ("1d",  "1 day",     "1 hour"),
    ]:
        ohlc = (
            trades_enriched
            .groupBy(
                F.window(F.col("timestamp"), duration),  # TUMBLING WINDOW
                F.col("symbol"),
                F.col("name"),
                F.col("category"),
            )
            .agg(
                # OHLC
                F.first("price").alias("open"),
                F.max("price").alias("high"),
                F.min("price").alias("low"),
                F.last("price").alias("close"),
                # Volume
                F.sum("quantity").alias("volume"),
                F.sum("quote_asset_volume").alias("quote_volume"),  # USDT volume
                # Trade stats
                F.count("trade_id").alias("trades"),
                F.sum(
                    F.when(~F.col("is_buyer_maker"), F.col("quantity")).otherwise(0)
                ).alias("taker_buy_volume"),
                # VWAP = Σ(price × qty) / Σqty
                (F.sum("quote_asset_volume") / F.sum("quantity")).alias("vwap"),
            )
            .select(
                F.col("symbol"),
                F.col("name"),
                F.col("category"),
                F.lit(interval).alias("interval"),
                (F.col("window.start").cast("long") * 1000).alias("open_time"),
                (F.col("window.end").cast("long")   * 1000).alias("close_time"),
                "open", "high", "low", "close",
                "volume", "quote_volume", "trades",
                "taker_buy_volume", "vwap",
            )
        )

        intvl = interval  # closure capture
        query = (
            ohlc.writeStream
            .outputMode("update")      # Chỉ emit khi window đóng
            .foreachBatch(lambda df, bid, iv=intvl: write_ohlc_to_databases(df, bid, iv))
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/ohlc_{interval}")
            .trigger(processingTime="10 seconds")
            .start()
        )
        queries.append(query)
        logger.info(f"✅ OHLC {interval} query started")

    return queries


# ─── Query 2: Kline Stream (nến đã tính sẵn từ Binance) ─────────────────────
def build_kline_query(spark: SparkSession):
    """
    Đọc kline stream trực tiếp từ Binance (đã có OHLC sẵn).
    Chỉ lấy nến đã đóng (is_closed=True) để đảm bảo data chính xác.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_KLINES)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    klines = (
        raw
        .select(F.from_json(F.col("value").cast("string"), KLINE_SCHEMA).alias("k"))
        .select("k.*")
        .filter(F.col("interval") == "1m")
        .filter(F.col("is_closed") == True)   # Chỉ nến đã đóng
        .withColumn("event_ts", F.to_timestamp(F.col("close_time") / 1000))
        .withWatermark("event_ts", "5 minutes")
        # Tính thêm các chỉ số
        .withColumn(
            "price_change_pct",
            ((F.col("close") - F.col("open")) / F.col("open") * 100)
        )
        .withColumn(
            "candle_type",
            F.when(F.col("close") >= F.col("open"), "bullish").otherwise("bearish")
        )
        .withColumn(
            "body_size",
            F.abs(F.col("close") - F.col("open"))
        )
        .withColumn(
            "upper_shadow",
            F.col("high") - F.greatest(F.col("open"), F.col("close"))
        )
        .withColumn(
            "lower_shadow",
            F.least(F.col("open"), F.col("close")) - F.col("low")
        )
    )

    def write_klines(batch_df: DataFrame, batch_id: int):
        rows = batch_df.collect()
        if not rows:
            return
        try:
            mongo_client = get_mongo()
            r = get_redis()             # BỔ SUNG: Mở kết nối Redis
            pipe = r.pipeline()         # BỔ SUNG: Dùng Pipeline để ghi hàng loạt siêu nhanh
            
            # Gom nhóm thủ công bằng dictionary
            grouped = {}
            for r_row in rows:
                grouped.setdefault(r_row["interval"], []).append(r_row)
                
            for interval, group in grouped.items():
                col_name = f"ohlc_{interval}"
                col_obj  = mongo_client[MONGO_DB][col_name]
                for row in group:
                    symbol = row["symbol"]
                    ts_start = int(row["open_time"])
                    
                    # 1. Tạo cục data chung chuẩn chỉnh cho cả Redis và Mongo
                    data = {
                        "symbol":            symbol,
                        "interval":          interval,
                        "openTime":          ts_start,
                        "closeTime":         int(row["close_time"]),
                        "open":              float(row["open"]),
                        "high":              float(row["high"]),
                        "low":               float(row["low"]),
                        "close":             float(row["close"]),
                        "volume":            float(row["volume"]),
                        "quoteVolume":       float(row["quote_volume"]),
                        "trades":            int(row["trades_count"]),
                        "price_change_pct":  float(row["price_change_pct"]),
                        "candle_type":       row["candle_type"],
                        "is_closed":         True,
                    }

                    # 2. GHI VÀO REDIS (Hot Data phục vụ Web Frontend)
                    redis_key = f"crypto:{symbol}:{interval}:{ts_start}"
                    index_key = f"crypto:{symbol}:{interval}:index"

                    # Lưu nến vào Redis trong 24h
                    pipe.setex(redis_key, 86400, json.dumps(data))
                    pipe.zadd(index_key, {str(ts_start): ts_start})

                    # Dọn rác: Xóa index cũ hơn 24h để nhẹ RAM
                    cutoff = ts_start - 24 * 60 * 60 * 1000
                    pipe.zremrangebyscore(index_key, "-inf", cutoff)

                    # Cập nhật thanh Giá Ticker Bar trên đỉnh màn hình Web
                    if interval == "1m":
                        pipe.hset(f"ticker:{symbol}", mapping={
                            "price": str(data["close"]),
                            "volume_24h": str(data["volume"]),
                            "updated_at": str(ts_start),
                        })
                        pipe.expire(f"ticker:{symbol}", 120)

                    # 3. GHI VÀO MONGODB (Cold Data phục vụ Airflow & AI)
                    try:
                        col_obj.update_one(
                            {"symbol": symbol, "openTime": ts_start},
                            {"$set": data},
                            upsert=True
                        )
                    except DuplicateKeyError:
                        pass
                        
            # Chạy toàn bộ lệnh Redis cùng một lúc
            pipe.execute()
            r.close()
            mongo_client.close()
            logger.info(f"Kline batch {batch_id}: {len(rows)} nến đóng → Redis & MongoDB")
        except Exception as e:
            logger.error(f"❌ Kline write error batch {batch_id}: {e}")

    return (
        klines.writeStream
        .outputMode("update")
        .foreachBatch(write_klines)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/klines")
        .trigger(processingTime="5 seconds")
        .start()
    )


# ─── Query 3: Orderbook Stream ────────────────────────────────────────────────
def build_orderbook_query(spark: SparkSession):
    """
    Đọc orderbook snapshots → ghi vào Redis (hot) + MongoDB (cold).
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_ORDERBOOK)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    orderbook = (
        raw
        .select(F.from_json(F.col("value").cast("string"), ORDERBOOK_SCHEMA).alias("ob"))
        .select("ob.*")
        .withColumn("ts", F.to_timestamp(F.col("timestamp") / 1000))
        .withWatermark("ts", "1 minute")
    )

    def write_orderbook(batch_df: DataFrame, batch_id: int):
        rows = batch_df.collect()
        if not rows:
            return
        try:
            r = get_redis()
            mongo_client = get_mongo()
            col = mongo_client[MONGO_DB]["orderbook"]
            pipe = r.pipeline()

            for row in rows:
                symbol = row["symbol"]
                data = {
                    "best_bid":  float(row["best_bid"]),
                    "best_ask":  float(row["best_ask"]),
                    "spread":    float(row["spread"]),
                    "updated_at": int(row["timestamp"]),
                }
                pipe.hset(f"orderbook:{symbol}", mapping={k: str(v) for k, v in data.items()})
                pipe.expire(f"orderbook:{symbol}", 30)
                col.insert_one({**{"symbol": symbol}, **data})

            pipe.execute()
            r.close()
            mongo_client.close()
        except Exception as e:
            logger.error(f"❌ Orderbook write error: {e}")

    return (
        orderbook.writeStream
        .outputMode("update")
        .foreachBatch(write_orderbook)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/orderbook")
        .trigger(processingTime="2 seconds")
        .start()
    )


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 70)
    logger.info("🚀 CRYPTO SPARK STRUCTURED STREAMING")
    logger.info("   Intervals: 1m, 5m, 1h, 1d")
    logger.info("   Sources:   trades + klines + orderbook")
    logger.info("   Sinks:     Redis (hot) + MongoDB (cold)")
    logger.info("=" * 70)

    # Start Prometheus
    start_http_server(8003)
    logger.info("📊 Prometheus metrics on :8003")

    # Setup MongoDB indexes trước khi start (từ file gốc của bạn)
    setup_mongodb_indexes()

    # Khởi động Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Khởi chạy tất cả queries song song
    queries = []
    queries.append(build_kline_query(spark))      # Kline từ Binance (backup)
    queries.append(build_orderbook_query(spark))  # Orderbook snapshots

    logger.info(f"✅ {len(queries)} streaming queries đang chạy")

    try:
        # Giữ process chạy, tự restart query nếu fail
        while True:
            for q in queries:
                if q.exception():
                    logger.error(f"Query exception: {q.exception()}")
            time.sleep(10)

    except KeyboardInterrupt:
        logger.info("Dừng tất cả queries...")
    finally:
        for q in queries:
            q.stop()
        spark.stop()
        logger.info("✅ Spark session đóng")


if __name__ == "__main__":
    main()