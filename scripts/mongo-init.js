// ─────────────────────────────────────────────────────────────
// mongo-init.js
// Khởi tạo database, collections và indexes cho crypto_db
// Chạy tự động khi MongoDB container khởi động lần đầu
// ─────────────────────────────────────────────────────────────

const db = db.getSiblingDB("crypto_db");

print("🔧 Khởi tạo crypto_db...");

// ════════════════════════════════
// 1. Collection: ohlc_1m
// Lưu nến 1 phút từ Spark Streaming
// ════════════════════════════════
db.createCollection("ohlc_1m");
db.ohlc_1m.createIndex(
  { symbol: 1, open_time: -1 },
  { name: "idx_symbol_time", background: true }
);
db.ohlc_1m.createIndex(
  { open_time: -1 },
  { 
    name: "idx_ttl",
    expireAfterSeconds: 86400,  // Tự xóa sau 1 ngày
    background: true 
  }
);
print("  ✅ Collection ohlc_1m created");

// ════════════════════════════════
// 2. Collection: ohlc_5m
// Lưu nến 5 phút
// ════════════════════════════════
db.createCollection("ohlc_5m");
db.ohlc_5m.createIndex(
  { symbol: 1, open_time: -1 },
  { name: "idx_symbol_time", background: true }
);
db.ohlc_5m.createIndex(
  { open_time: -1 },
  {
    name: "idx_ttl",
    expireAfterSeconds: 604800,  // Tự xóa sau 7 ngày
    background: true
  }
);
print("  ✅ Collection ohlc_5m created");

// ════════════════════════════════
// 3. Collection: ohlc_1h
// Lưu nến 1 giờ (từ Spark Batch)
// ════════════════════════════════
db.createCollection("ohlc_1h");
db.ohlc_1h.createIndex(
  { symbol: 1, open_time: -1 },
  { name: "idx_symbol_time", background: true }
);
print("  ✅ Collection ohlc_1h created");

// ════════════════════════════════
// 4. Collection: ohlc_1d
// Lưu nến 1 ngày (từ Spark Batch)
// ════════════════════════════════
db.createCollection("ohlc_1d");
db.ohlc_1d.createIndex(
  { symbol: 1, open_time: -1 },
  { name: "idx_symbol_time", background: true }
);
print("  ✅ Collection ohlc_1d created");

// ════════════════════════════════
// 5. Collection: orderbook
// Lưu snapshot orderbook real-time
// ════════════════════════════════
db.createCollection("orderbook");
db.orderbook.createIndex(
  { symbol: 1, timestamp: -1 },
  { name: "idx_symbol_time", background: true }
);
db.orderbook.createIndex(
  { timestamp: -1 },
  {
    name: "idx_ttl",
    expireAfterSeconds: 3600,  // Tự xóa sau 1 giờ
    background: true
  }
);
print("  ✅ Collection orderbook created");

// ════════════════════════════════
// 6. Collection: trades
// Lưu lịch sử giao dịch
// ════════════════════════════════
db.createCollection("trades");
db.trades.createIndex(
  { symbol: 1, trade_time: -1 },
  { name: "idx_symbol_time", background: true }
);
db.trades.createIndex(
  { trade_time: -1 },
  {
    name: "idx_ttl",
    expireAfterSeconds: 86400,  // Tự xóa sau 1 ngày
    background: true
  }
);
print("  ✅ Collection trades created");

// ════════════════════════════════
// 7. Collection: ml_predictions
// Lưu kết quả dự đoán ML
// ════════════════════════════════
db.createCollection("ml_predictions");
db.ml_predictions.createIndex(
  { symbol: 1, predicted_at: -1 },
  { name: "idx_symbol_time", background: true }
);
db.ml_predictions.createIndex(
  { predicted_at: -1 },
  {
    name: "idx_ttl",
    expireAfterSeconds: 86400,
    background: true
  }
);
print("  ✅ Collection ml_predictions created");

// ════════════════════════════════
// 8. Collection: symbol_metadata
// Thông tin cố định về từng coin
// ════════════════════════════════
db.createCollection("symbol_metadata");
db.symbol_metadata.createIndex(
  { symbol: 1 },
  { name: "idx_symbol", unique: true, background: true }
);

// Insert dữ liệu tĩnh về các coin
db.symbol_metadata.insertMany([
  { symbol: "BTCUSDT",  name: "Bitcoin",   base: "BTC",  category: "Layer 1",  rank: 1 },
  { symbol: "ETHUSDT",  name: "Ethereum",  base: "ETH",  category: "Layer 1",  rank: 2 },
  { symbol: "BNBUSDT",  name: "BNB",       base: "BNB",  category: "Exchange", rank: 3 },
  { symbol: "SOLUSDT",  name: "Solana",    base: "SOL",  category: "Layer 1",  rank: 4 },
  { symbol: "ADAUSDT",  name: "Cardano",   base: "ADA",  category: "Layer 1",  rank: 5 },
  { symbol: "XRPUSDT",  name: "XRP",       base: "XRP",  category: "Payments", rank: 6 },
  { symbol: "DOGEUSDT", name: "Dogecoin",  base: "DOGE", category: "Meme",     rank: 7 },
  { symbol: "AVAXUSDT", name: "Avalanche", base: "AVAX", category: "Layer 1",  rank: 8 },
]);
print("  ✅ Collection symbol_metadata created + seeded");

// ════════════════════════════════
// 9. Collection: daily_summary
// Tổng hợp hàng ngày từ Spark Batch
// ════════════════════════════════
db.createCollection("daily_summary");
db.daily_summary.createIndex(
  { date: -1, symbol: 1 },
  { name: "idx_date_symbol", unique: true, background: true }
);
print("  ✅ Collection daily_summary created");

print("");
print("✅ crypto_db khởi tạo hoàn tất!");
print("   Collections: ohlc_1m, ohlc_5m, ohlc_1h, ohlc_1d");
print("   Collections: orderbook, trades, ml_predictions");
print("   Collections: symbol_metadata, daily_summary");
