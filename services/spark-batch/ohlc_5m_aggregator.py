"""Gom nến 1m từ MongoDB → 5m"""
from datetime import datetime, timedelta, timezone
from utils import (create_spark_session, get_mongo, ensure_index,
                   normalize_docs, aggregate_ohlc, upsert_ohlc, logger, MONGO_DB)

def main():
    print("="*70)
    print("🚀 Gom nến 1m → 5m (Từ MongoDB)")
    print("="*70)

    spark = create_spark_session("OHLC-5m-Aggregator")
    spark.sparkContext.setLogLevel("WARN")
    ensure_index("ohlc_5m")

    # Khung 5 phút vừa đóng
    now = datetime.now(timezone.utc)
    rounded_m    = (now.minute // 5) * 5
    window_end   = now.replace(minute=rounded_m, second=0, microsecond=0)
    window_start = window_end - timedelta(minutes=5)
    ts_start = int(window_start.timestamp() * 1000)
    ts_end   = int(window_end.timestamp()   * 1000)
    print(f"📅 {window_start} → {window_end}")

    client  = get_mongo()
    col     = client[MONGO_DB]["ohlc_1m"]
    
    # Tìm các đồng coin có giao dịch trong 5 phút qua
    symbols = col.distinct("symbol", {"openTime": {"$gte": ts_start, "$lt": ts_end}})
    
    if not symbols:
        print("⚠️  Không có data.")
        spark.stop()
        client.close()
        return

    # Kéo nến 1m từ MongoDB
    docs = list(col.find({
        "symbol": {"$in": symbols}, "interval": "1m",
        "openTime": {"$gte": ts_start, "$lt": ts_end}
    }).sort("openTime", 1))
    client.close()
    
    print(f"📥 {len(docs)} nến 1m từ MongoDB")

    # Gom thành nến 5m bằng Spark và upsert lại vào MongoDB
    docs     = normalize_docs(docs)
    results  = aggregate_ohlc(spark, docs, 300_000).collect()
    inserted = upsert_ohlc("ohlc_5m", "5m", results, "spark_5m_aggregator")
    
    print(f"\n✅ Đã lưu {inserted} nến 5m vào MongoDB")
    spark.stop()

if __name__ == "__main__":
    main()