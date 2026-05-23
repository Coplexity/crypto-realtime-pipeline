"""Gom nến 5m từ MongoDB → 1h"""
from datetime import datetime, timedelta, timezone
from utils import (create_spark_session, get_mongo, ensure_index,
                   normalize_docs, aggregate_ohlc, upsert_ohlc, logger, MONGO_DB)

def main():
    print("="*70)
    print("🚀 Gom nến 5m → 1h")
    print("="*70)

    spark = create_spark_session("OHLC-1h-Aggregator")
    spark.sparkContext.setLogLevel("WARN")
    ensure_index("ohlc_1h")

    now = datetime.now(timezone.utc)
    window_end   = now.replace(minute=0, second=0, microsecond=0)
    window_start = window_end - timedelta(hours=1)
    ts_start = int(window_start.timestamp() * 1000)
    ts_end   = int(window_end.timestamp()   * 1000)
    print(f"📅 {window_start} → {window_end}")

    client  = get_mongo()
    col     = client[MONGO_DB]["ohlc_5m"]
    symbols = col.distinct("symbol", {"openTime": {"$gte": ts_start, "$lt": ts_end}})
    if not symbols:
        print("⚠️  Không có data."); spark.stop(); client.close(); return

    docs = list(col.find({
        "symbol": {"$in": symbols}, "interval": "5m",
        "openTime": {"$gte": ts_start, "$lt": ts_end}
    }).sort("openTime", 1))
    client.close()
    print(f"📥 {len(docs)} nến 5m từ MongoDB")

    docs     = normalize_docs(docs)
    results  = aggregate_ohlc(spark, docs, 3_600_000).collect()
    inserted = upsert_ohlc("ohlc_1h", "1h", results, "spark_1h_aggregator")
    print(f"\n✅ Đã lưu {inserted} nến 1h vào MongoDB")
    spark.stop()

if __name__ == "__main__":
    main()
