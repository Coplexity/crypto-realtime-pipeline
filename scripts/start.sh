#!/bin/bash
# ─────────────────────────────────────────────────────────────
# Start script: Khởi động toàn bộ Crypto Analytics Platform
# ─────────────────────────────────────────────────────────────
set -e

GREEN='\033[0;32m'; BLUE='\033[0;34m'
YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

log()  { echo -e "${BLUE}[$(date +%H:%M:%S)]${NC} $*"; }
ok()   { echo -e "${GREEN}  ✅ $*${NC}"; }
warn() { echo -e "${YELLOW}  ⚠️  $*${NC}"; }
err()  { echo -e "${RED}  ❌ $*${NC}"; exit 1; }

cd "$(dirname "$0")/.."

# ── Bước 1: Kiểm tra .env ───────────────────────────────────
if [ ! -f .env ]; then
  warn ".env chưa có, tạo từ mẫu..."
  cat > .env << 'ENVEOF'
MONGO_USER=admin
MONGO_PASSWORD=admin123
REDIS_PASSWORD=redis123
GRAFANA_PASSWORD=admin123
BINANCE_API_KEY=
CRYPTO_SYMBOLS=BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,ADAUSDT,XRPUSDT,DOGEUSDT,AVAXUSDT
ENVEOF
  ok ".env đã tạo"
fi

# ── Bước 2: Khởi động infrastructure ───────────────────────
log "🐳 Khởi động Zookeeper, Kafka, MongoDB, Redis..."
docker compose up -d zookeeper mongodb redis
sleep 5
docker compose up -d kafka
sleep 15
ok "Infrastructure đã khởi động"

# ── Bước 3: Tạo Kafka topics ───────────────────────────────
log "🔧 Tạo Kafka topics..."
for topic in crypto.klines crypto.trades crypto.orderbook; do
  docker exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic $topic \
    --partitions 6 \
    --replication-factor 1 2>/dev/null && ok "Topic $topic" || warn "Topic $topic đã tồn tại"
done

# ── Bước 4: Khởi động tất cả services ─────────────────────
log "🚀 Khởi động tất cả services..."
docker compose up -d
sleep 20
ok "Tất cả services đã khởi động"

# ── Bước 5: Kiểm tra health ────────────────────────────────
log "🔍 Kiểm tra backend health..."
for i in {1..15}; do
  if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    ok "Backend healthy!"
    break
  fi
  sleep 3
  [ $i -eq 15 ] && warn "Backend chưa healthy sau 45s, kiểm tra logs"
done

# ── Bước 6: Submit Spark Streaming ────────────────────────
log "⚡ Submit Spark Streaming job..."
sleep 10
docker exec spark-master /spark/bin/spark-submit \
  --master local[4] \
  --conf spark.sql.session.timeZone=UTC \
  /opt/spark-jobs/streaming/streaming_job.py > /tmp/spark-streaming.log 2>&1 &
ok "Spark Streaming job submitted (log: /tmp/spark-streaming.log)"

# ── Kết quả ────────────────────────────────────────────────
echo ""
echo -e "${GREEN}══════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ Crypto Analytics Platform đang chạy!     ${NC}"
echo -e "${GREEN}══════════════════════════════════════════════${NC}"
echo ""
echo -e "  🌐 Dashboard   → http://localhost:3000"
echo -e "  📡 API Docs    → http://localhost:8000/docs"
echo -e "  📊 Grafana     → http://localhost:3001  (admin/admin123)"
echo -e "  🔥 Prometheus  → http://localhost:9090"
echo -e "  ✈️  Airflow     → http://localhost:8088  (admin/admin123)"
echo -e "  ⚡ Kafka UI    → http://localhost:8090"
echo -e "  🔥 Spark UI    → http://localhost:8080"
echo ""
echo -e "  📋 Xem logs    → docker compose logs -f [service]"
echo -e "  🛑 Dừng        → docker compose down"
echo ""
