"""
Các hàm dùng chung cho tất cả Spark Batch Jobs
"""
import logging
import os
import sys
from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("spark-batch")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongodb:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "crypto_db")

def create_spark_session(app_name: str):
    from pyspark.sql import SparkSession
    os.environ["PYSPARK_PYTHON"]        = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )

def get_mongo():
    return MongoClient(MONGO_URI, authSource="admin",
                       serverSelectionTimeoutMS=5000)

def get_active_symbols(collection_name: str) -> list:
    """Tự động quét MongoDB tìm tất cả coins đang có data"""
    try:
        client = get_mongo()
        symbols = client[MONGO_DB][collection_name].distinct("symbol")
        client.close()
        logger.info(f"Phát hiện {len(symbols)} symbols từ {collection_name}")
        return symbols
    except Exception as e:
        logger.warning(f"Lỗi quét symbols: {e}. Dùng fallback.")
        return ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT",
                "ADAUSDT","XRPUSDT","DOGEUSDT","AVAXUSDT"]

def ensure_index(collection_name: str):
    """Tự động tạo index nếu chưa có"""
    try:
        client = get_mongo()
        col = client[MONGO_DB][collection_name]
        index_name = "symbol_interval_openTime_unique"
        existing = {idx.get("name") for idx in col.list_indexes()}
        if index_name not in existing:
            col.create_index(
                [("symbol", ASCENDING),("interval", ASCENDING),("openTime", ASCENDING)],
                unique=True, name=index_name
            )
            logger.info(f"  ✅ Index tạo xong cho {collection_name}")
        else:
            logger.info(f"  ℹ️  Index đã tồn tại cho {collection_name}")
        client.close()
    except OperationFailure as e:
        if e.code == 85:
            logger.info("  ℹ️  Index tồn tại với tên khác, bỏ qua")
        else:
            logger.warning(f"  ⚠️  Lỗi index: {e}")

def normalize_docs(docs: list) -> list:
    """Chuyển BSON → Python thuần để Spark đọc được"""
    try:
        from bson import Int64, Decimal128
        has_bson = True
    except ImportError:
        has_bson = False

    normalized = []
    for doc in docs:
        if not isinstance(doc, dict): continue
        nd = dict(doc)
        nd.pop("_id", None)
        for key in ["openTime","closeTime","trades"]:
            if key in nd:
                try:
                    val = nd[key]
                    if has_bson and isinstance(val, Int64): val = int(val)
                    nd[key] = int(val)
                except: pass
        for key in ["open","high","low","close","volume","quoteVolume"]:
            if key in nd:
                try:
                    val = nd[key]
                    if has_bson: val = float(val)
                    nd[key] = float(val)
                except: pass
        for key in ["symbol","interval"]:
            if key in nd and nd[key] is not None:
                nd[key] = str(nd[key])
        normalized.append(nd)
    return normalized

def aggregate_ohlc(spark, docs: list, window_ms: int):
    """Gom nến dùng chung cho mọi interval"""
    from pyspark.sql import functions as F
    df = spark.createDataFrame(docs)
    df = (df
        .withColumn("open",        F.col("open").cast("double"))
        .withColumn("high",        F.col("high").cast("double"))
        .withColumn("low",         F.col("low").cast("double"))
        .withColumn("close",       F.col("close").cast("double"))
        .withColumn("volume",      F.col("volume").cast("double"))
        .withColumn("quoteVolume", F.col("quoteVolume").cast("double"))
        .withColumn("trades",      F.col("trades").cast("integer"))
        .withColumn("openTime",    F.col("openTime").cast("long"))
        .withColumn("closeTime",   F.col("closeTime").cast("long"))
        .withColumn("window_start",
            (F.col("openTime") / window_ms).cast("long") * window_ms)
    )
    
    # [FIX CỰC KỲ QUAN TRỌNG]: Dùng F.struct để ép Spark gắn chặt giá trị Open/Close 
    # với thời gian tương ứng. Dù Spark có xáo trộn (shuffle) dữ liệu trong lúc 
    # groupBy thì kết quả Open/Close vẫn lấy chuẩn xác theo thời gian thực tế.
    return df.groupBy("symbol","window_start").agg(
        # Tìm nến có openTime nhỏ nhất -> lấy giá open của nến đó
        F.min(F.struct("openTime", "open")).getField("open").alias("open"),
        
        # High/Low thì vẫn dùng min/max bình thường
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        
        # Tìm nến có openTime lớn nhất -> lấy giá close của nến đó
        F.max(F.struct("openTime", "close")).getField("close").alias("close"),
        
        F.sum("volume").alias("volume"),
        F.sum("quoteVolume").alias("quoteVolume"),
        F.sum("trades").alias("trades"),
        F.min("openTime").alias("openTime"),
        F.max("closeTime").alias("closeTime"),
    )

def upsert_ohlc(collection_name: str, interval: str, results, source_tag: str):
    """Upsert kết quả vào MongoDB"""
    from datetime import datetime, timezone
    client = get_mongo()
    col = client[MONGO_DB][collection_name]
    inserted = 0
    for row in results:
        doc = {
            "symbol":      row["symbol"],     "interval":    interval,
            "openTime":    row["openTime"],   "closeTime":   row["closeTime"],
            "open":        float(row["open"]),
            "high":        float(row["high"]),
            "low":         float(row["low"]),
            "close":       float(row["close"]),
            "volume":      float(row["volume"]),
            "quoteVolume": float(row["quoteVolume"]),
            "trades":      int(row["trades"]),
            "createdAt":   datetime.now(timezone.utc).isoformat(),
            "source":      source_tag,
        }
        try:
            col.update_one(
                {"symbol": doc["symbol"],"interval": interval,"openTime": doc["openTime"]},
                {"$set": doc}, upsert=True
            )
            logger.info(f"  ✅ {row['symbol']}: {interval} (O:{row['open']:.2f} C:{row['close']:.2f})")
            inserted += 1
        except Exception as e:
            logger.error(f"  ❌ {row['symbol']}: {e}")
    client.close()
    return inserted
