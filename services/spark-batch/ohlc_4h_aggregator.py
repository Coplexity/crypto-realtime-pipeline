"""Gom nến 1h từ MongoDB → 4h (chuẩn UTC Binance)"""
from datetime import datetime, timedelta, timezone
from utils import (create_spark_session, get_mongo, ensure_index,
                   normalize_docs, aggregate_ohlc, upsert_ohlc, logger, MONGO_DB)

def main():
    print("="*70)
    print("🚀 Gom nến 1h → 4h")
    print("="*70)

    spark = create_spark_session("OHLC-4h-Aggregator")
    spark.sparkContext.setLogLevel("WARN")
    ensure_index("ohlc_4h")

    # Khung 4h chuẩn Binance: 00/04/08/12/16/20 UTC
    now          = datetime.now(timezone.utc)
    block_hour   = (now.hour // 4) * 4
    window_end   = now.replace(hour=block_hour, minute=0, second=0, microsecond=0)
    window_start = window_end - timedelta(hours=4)
    ts_start = int(window_start.timestamp() * 1000)
    ts_end   = int(window_end.timestamp()   * 1000)
    print(f"📅 {window_start} → {window_end}")

    client  = get_mongo()
    col     = client[MONGO_DB]["ohlc_1h"]
    symbols = col.distinct("symbol", {"openTime": {"$gte": ts_start, "$lt": ts_end}})
    if not symbols:
        print("⚠️  Không có data."); spark.stop(); client.close(); return

    docs = list(col.find({
        "symbol": {"$in": symbols}, "interval": "1h",
        "openTime": {"$gte": ts_start, "$lt": ts_end}
    }).sort("openTime", 1))
    client.close()
    print(f"📥 {len(docs)} nến 1h từ MongoDB")

    docs     = normalize_docs(docs)
    results  = aggregate_ohlc(spark, docs, 14_400_000).collect()
    inserted = upsert_ohlc("ohlc_4h", "4h", results, "spark_4h_aggregator")
    print(f"\n✅ Đã lưu {inserted} nến 4h vào MongoDB")
    spark.stop()

if __name__ == "__main__":
    main()
