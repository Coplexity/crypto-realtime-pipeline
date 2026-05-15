"""
Binance WebSocket → Apache Kafka Producer
Kết hợp: Dynamic pair discovery, Exponential backoff, Per-pair stats,
          Async WebSocket, Confluent Kafka, Multi-stream (klines+trades+orderbook),
          Prometheus metrics, Structured logging
"""

import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime
from typing import Dict, Optional, Set

import requests
import websockets
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import Counter, Gauge, start_http_server

# ─── Logging: file + console ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("producer.log"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger("binance-producer")

# ─── Config ─────────────────────────────────────────────────────────────────
KAFKA_SERVERS        = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SYMBOLS_ENV          = os.getenv("CRYPTO_SYMBOLS", "")
PAIR_MIN_VOLUME      = float(os.getenv("PAIR_MIN_VOLUME", "500000000")) 
PAIR_UPDATE_INTERVAL = int(os.getenv("PAIR_UPDATE_INTERVAL", "3600")) 
MAX_PAIRS            = int(os.getenv("MAX_PAIRS", "20"))
BINANCE_REST         = "https://api.binance.com/api/v3"
BINANCE_WS           = "wss://stream.binance.com:9443/stream"

# Kafka topics
TOPIC_KLINES    = "crypto.klines"
TOPIC_ORDERBOOK = "crypto.orderbook"
TOPIC_TRADES    = "crypto.trades"

# Kline intervals cần stream
KLINE_INTERVALS = ["1m", "5m", "1h", "1d"]

# ─── Prometheus metrics ─────────────────────────────────────────────────────
msg_produced = Counter(
    "kafka_messages_produced_total",
    "Tổng messages đã gửi vào Kafka",
    ["topic", "symbol"]
)
ws_connected = Gauge(
    "websocket_connected",
    "WebSocket đang kết nối (1=có, 0=không)"
)
pair_last_price = Gauge(
    "pair_last_price",
    "Giá giao dịch cuối cùng",
    ["symbol"]
)

# ─── Dynamic Pair Discovery ──────────────────────────────────────────────────
class PairDiscovery:
    @staticmethod
    def fetch(min_volume: float = PAIR_MIN_VOLUME, max_pairs: int = MAX_PAIRS) -> Set[str]:
        if SYMBOLS_ENV:
            pairs = {s.lower() for s in SYMBOLS_ENV.split(",") if s.strip()}
            logger.info(f"Dùng symbols từ .env: {pairs}")
            return pairs

        try:
            logger.info("Fetching trading pairs từ Binance REST API...")
            resp = requests.get(f"{BINANCE_REST}/ticker/24hr", timeout=10)
            resp.raise_for_status()
            tickers = resp.json()

            candidates = []
            for t in tickers:
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                try:
                    vol = float(t.get("quoteVolume", 0))
                    if vol >= min_volume:
                        candidates.append((sym.lower(), vol))
                except (ValueError, TypeError):
                    continue

            candidates.sort(key=lambda x: x[1], reverse=True)
            pairs = {sym for sym, _ in candidates[:max_pairs]}

            logger.info(f"Tìm được {len(pairs)} pairs có volume > {min_volume:,.0f} USDT")
            return pairs

        except Exception as e:
            logger.error(f"Lỗi fetch pairs: {e}. Dùng fallback.")
            return {
                "btcusdt", "ethusdt", "bnbusdt", "solusdt",
                "adausdt", "xrpusdt", "dogeusdt", "avaxusdt"
            }

# ─── Kafka Producer ──────────────────────────────────────────────────────────
class CryptoKafkaProducer:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": KAFKA_SERVERS,
            "acks": "1",
            "retries": 3,
            "retry.backoff.ms": 200,
            "compression.type": "gzip",
            "linger.ms": 5,
            "max.in.flight.requests.per.connection": 5,
        })
        self._ensure_topics()
        self.message_count = 0
        self.error_count   = 0

    def _ensure_topics(self):
        admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS})
        topics = [
            NewTopic(TOPIC_KLINES,    num_partitions=6, replication_factor=1,
                     config={"retention.ms": str(7 * 24 * 3600 * 1000)}),
            NewTopic(TOPIC_ORDERBOOK, num_partitions=4, replication_factor=1,
                     config={"retention.ms": str(3600 * 1000)}),
            NewTopic(TOPIC_TRADES,    num_partitions=6, replication_factor=1,
                     config={"retention.ms": str(24 * 3600 * 1000)}),
        ]
        results = admin.create_topics(topics)
        for topic, fut in results.items():
            try:
                fut.result()
                logger.info(f"Topic '{topic}' created")
            except Exception as e:
                if "already exists" not in str(e):
                    logger.warning(f"Topic '{topic}': {e}")

    def send(self, topic: str, key: str, value: dict, symbol: str = ""):
        try:
            self.producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(value).encode("utf-8"),
                on_delivery=self._on_delivery,
            )
            self.producer.poll(0)
            msg_produced.labels(topic=topic, symbol=symbol).inc()
        except Exception as e:
            self.error_count += 1
            logger.error(f"Kafka send error: {e}")

    def _on_delivery(self, err, msg):
        if err:
            self.error_count += 1
            logger.error(f"Delivery failed: {err}")
        else:
            self.message_count += 1
            if self.message_count % 1000 == 0:
                logger.info(
                    f"✓ {self.message_count} messages sent | "
                    f"Errors: {self.error_count}"
                )

    def flush(self):
        self.producer.flush(timeout=10)

# ─── Transform ──────────────────────────────────────────────────────────────
def parse_kline(data: dict) -> dict:
    k = data["k"]
    return {
        "symbol":           k["s"],
        "interval":         k["i"],
        "open_time":        k["t"],
        "close_time":       k["T"],
        "open":             float(k["o"]),
        "high":             float(k["h"]),
        "low":              float(k["l"]),
        "close":            float(k["c"]),
        "volume":           float(k["v"]),
        "quote_volume":     float(k["q"]),
        "trades_count":     int(k["n"]),
        "taker_buy_volume": float(k["V"]),
        "is_closed":        k["x"],
        "event_time":       data["E"],
        "ingestion_time":   int(time.time() * 1000),
    }

def parse_trade(data: dict) -> dict:
    return {
        "symbol":         data["s"],
        "trade_id":       data["t"],
        "price":          float(data["p"]),
        "quantity":       float(data["q"]),
        "trade_time":     data["T"],
        "is_buyer_maker": data["m"],
        "is_best_match":  data.get("M", False),
        "event_time":     data["E"],
        "ingestion_time": int(time.time() * 1000),
    }

def parse_depth(data: dict, symbol: str) -> dict:
    bids = [[float(p), float(q)] for p, q in data.get("bids", [])[:20]]
    asks = [[float(p), float(q)] for p, q in data.get("asks", [])[:20]]
    return {
        "symbol":    symbol.upper(),
        "bids":      bids,
        "asks":      asks,
        "best_bid":  bids[0][0] if bids else 0,
        "best_ask":  asks[0][0] if asks else 0,
        "spread":    (asks[0][0] - bids[0][0]) if (bids and asks) else 0,
        "timestamp": int(time.time() * 1000),
    }

# ─── WebSocket Streamer ───────────────────────────────────────────────────────
class BinanceStreamer:
    MAX_RECONNECT  = 10
    BASE_DELAY     = 1.0
    MAX_DELAY      = 300.0

    def __init__(self, producer: CryptoKafkaProducer):
        self.producer   = producer
        self.running    = True
        self.pairs: Set[str] = set()
        self.pair_stats: Dict[str, Dict] = {}
        self.active_ws = None # [FIX] Lưu trữ websocket connection hiện tại để chủ động đóng

    def _build_stream_url(self) -> str:
        streams = []
        for sym in sorted(self.pairs):
            for interval in KLINE_INTERVALS:
                streams.append(f"{sym}@kline_{interval}")
            streams.append(f"{sym}@depth20@100ms")
            streams.append(f"{sym}@trade")

        url = f"{BINANCE_WS}?streams={'/'.join(streams)}"
        logger.info(f"URL built: {len(streams)} streams cho {len(self.pairs)} pairs")
        return url

    def _update_pair_stats(self, symbol: str, price: float):
        if symbol not in self.pair_stats:
            self.pair_stats[symbol] = {"count": 0, "last_price": 0.0}
        self.pair_stats[symbol]["count"]      += 1
        self.pair_stats[symbol]["last_price"]  = price
        pair_last_price.labels(symbol=symbol).set(price)

    def _print_stats(self):
        if not self.pair_stats:
            return
        logger.info("=" * 60)
        logger.info("PAIR STATISTICS")
        logger.info("=" * 60)
        total = 0
        for sym, stats in sorted(self.pair_stats.items()):
            logger.info(
                f"  {sym:12s} | Messages: {stats['count']:8,d} | "
                f"Last Price: ${stats['last_price']:12.4f}"
            )
            total += stats["count"]
        logger.info(f"  TOTAL: {total:,} messages")
        logger.info(
            f"  Kafka: {self.producer.message_count:,} sent | "
            f"{self.producer.error_count} errors"
        )
        logger.info("=" * 60)

    # [FIX] Đổi từ Thread sang Async Task
    async def _refresh_pairs_loop(self):
        """Background task refresh danh sách pairs định kỳ"""
        while self.running:
            await asyncio.sleep(PAIR_UPDATE_INTERVAL)
            try:
                # Tránh block Event Loop bằng to_thread cho requests.get
                new_pairs = await asyncio.to_thread(PairDiscovery.fetch)
                
                if new_pairs != self.pairs:
                    logger.info(f"🔄 Pairs thay đổi (Cũ: {len(self.pairs)}, Mới: {len(new_pairs)}).")
                    self.pairs = new_pairs
                    
                    # [FIX] Cắt đứt kết nối hiện hành để vòng lặp run() tạo URL mới
                    if self.active_ws and not self.active_ws.closed:
                        logger.info("Ngắt WebSocket cũ để stream danh sách Pairs mới...")
                        await self.active_ws.close()
            except Exception as e:
                logger.error(f"Lỗi khi refresh pairs: {e}")

    async def _listen(self, ws):
        async for raw in ws:
            if not self.running:
                break
            try:
                msg        = json.loads(raw)
                data       = msg.get("data", msg)
                event_type = data.get("e", "")

                if event_type == "kline":
                    kline = parse_kline(data)
                    self.producer.send(
                        topic=TOPIC_KLINES,
                        key=f"{kline['symbol']}_{kline['interval']}",
                        value=kline,
                        symbol=kline["symbol"],
                    )
                    if kline["interval"] == "1m":
                        self._update_pair_stats(kline["symbol"], kline["close"])

                elif event_type == "trade":
                    trade = parse_trade(data)
                    self.producer.send(
                        topic=TOPIC_TRADES,
                        key=trade["symbol"],
                        value=trade,
                        symbol=trade["symbol"],
                    )
                    self._update_pair_stats(trade["symbol"], trade["price"])

                elif "bids" in data and "asks" in data:
                    stream = msg.get("stream", "")
                    symbol = stream.split("@")[0].upper()
                    depth  = parse_depth(data, symbol)
                    self.producer.send(
                        topic=TOPIC_ORDERBOOK,
                        key=depth["symbol"],
                        value=depth,
                        symbol=depth["symbol"],
                    )

            except (json.JSONDecodeError, KeyError) as e:
                pass # Bỏ qua log warning để tránh rác log

    async def run(self):
        # Lấy danh sách pairs lần đầu bằng to_thread để không block
        self.pairs = await asyncio.to_thread(PairDiscovery.fetch)
        if not self.pairs:
            logger.error("Không có pairs nào. Dừng.")
            return

        # [FIX] Khởi tạo Async Task thay vì Thread
        refresh_task = asyncio.create_task(self._refresh_pairs_loop())
        logger.info(f"Pair refresh task started (every {PAIR_UPDATE_INTERVAL}s)")

        attempt = 0
        logger.info("=" * 60)
        logger.info("🚀 Starting Binance → Kafka Streaming")
        logger.info(f"   Pairs: {sorted(self.pairs)}")
        logger.info("=" * 60)

        while self.running:
            attempt += 1
            logger.info(f"Kết nối lần #{attempt}...")

            try:
                url = self._build_stream_url()
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self.active_ws = ws # [FIX] Lưu tham chiếu
                    ws_connected.set(1)
                    logger.info("✅ WebSocket connected!")
                    attempt = 0
                    
                    await self._listen(ws)

            except websockets.exceptions.ConnectionClosed as e:
                ws_connected.set(0)
                logger.warning(f"Connection closed: {e.code} - {e.reason}")

            except Exception as e:
                ws_connected.set(0)
                logger.error(f"Stream error: {e}")

            if not self.running:
                break

            if attempt >= self.MAX_RECONNECT:
                logger.error(f"Đã thử {self.MAX_RECONNECT} lần. Dừng.")
                break

            wait = min(self.BASE_DELAY * (2 ** attempt), self.MAX_DELAY)
            logger.info(f"Chờ {wait:.1f}s trước khi reconnect...")
            await asyncio.sleep(wait)

        # Hủy task background khi dừng
        refresh_task.cancel()
        
        self._print_stats()
        self.producer.flush()
        logger.info("Service stopped.")

    def stop(self):
        logger.info("Bắt đầu dừng Streamer...")
        self.running = False
        if self.active_ws:
            asyncio.create_task(self.active_ws.close())


# ─── Main ────────────────────────────────────────────────────────────────────
async def main():
    start_http_server(8001)
    logger.info("📊 Prometheus metrics server on :8001")

    producer = CryptoKafkaProducer()
    streamer = BinanceStreamer(producer)

    # [FIX] Đăng ký tín hiệu tắt chuẩn cho Asyncio
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, streamer.stop)

    await streamer.run()

if __name__ == "__main__":
    asyncio.run(main())