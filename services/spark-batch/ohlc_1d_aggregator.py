"""Gom nến 4h từ MongoDB → 1d (chốt sổ 00:00 UTC)"""
from datetime import datetime, timedelta, timezone
from utils import (create_spark_session, get_mongo, ensure_index,
                   normalize_docs, aggregate_ohlc, upsert_ohlc, logger, MONGO_DB)

def main():
    print("="*70)
    print("🚀 Gom nến 4h → 1d")
    print("="*70)

    spark = create_spark_session("OHLC-1d-Aggregator")
    spark.sparkContext.setLogLevel("WARN")
    ensure_index("ohlc_1d")

    now          = datetime.now(timezone.utc)
    window_end   = now.replace(hour=0, minute=0, second=0, microsecond=0)
    window_start = window_end - timedelta(days=1)
    ts_start = int(window_start.timestamp() * 1000)
    ts_end   = int(window_end.timestamp()   * 1000)
    print(f"📅 {window_start} → {window_end}")

    client  = get_mongo()
    col     = client[MONGO_DB]["ohlc_4h"]
    symbols = col.distinct("symbol", {"openTime": {"$gte": ts_start, "$lt": ts_end}})
    if not symbols:
        print("⚠️  Không có data."); spark.stop(); client.close(); return

    docs = list(col.find({
        "symbol": {"$in": symbols}, "interval": "4h",
        "openTime": {"$gte": ts_start, "$lt": ts_end}
    }).sort("openTime", 1))
    client.close()
    print(f"📥 {len(docs)} nến 4h từ MongoDB")

    docs     = normalize_docs(docs)
    results  = aggregate_ohlc(spark, docs, 86_400_000).collect()
    inserted = upsert_ohlc("ohlc_1d", "1d", results, "spark_1d_aggregator")
    print(f"\n✅ Đã lưu {inserted} nến 1d vào MongoDB")
    spark.stop()

if __name__ == "__main__":
    main()
