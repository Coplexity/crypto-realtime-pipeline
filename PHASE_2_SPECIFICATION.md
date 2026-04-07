# PHASE 2: DATA COLLECTION - Technical Specification

**Scope:** Binance WebSocket → Kafka → Consumer Verification  
**Tasks:** IT-11, IT-12, IT-13, IT-14

---

## Overview

Phase 2 establishes the real-time data ingestion pipeline from Binance Exchange through Apache Kafka with quality verification.

### Architecture

```
Binance WebSocket (BTC/USDT @trade stream)
    ↓
Binance WebSocket Handler (IT-12)
    - Parse trade data
    - Extract all fields
    - Auto-reconnect on failure
    ↓
Kafka Producer (IT-13)
    - Serialize to JSON
    - Send with reliability settings: acks=all, retries=3
    - Compress with Snappy
    ↓
Kafka Topic: binance_trades
    - Partitions: 3
    - Replication: 1
    - Retention: 30 days
    ↓
Consumers (fan-out pattern)
    - Redis Consumer (cache)
    - Spark Consumer (aggregation)
    - Verification Consumer (quality check - IT-14)
```

---

## IT-11: Giai đoạn 2 - Thu thập dữ liệu

Phase label encompassing all data collection activities.

**Objectives:**
- Establish real-time data source
- Implement reliable message transport
- Set up scalable topic infrastructure
- Verify data quality

---

## IT-12: Binance WebSocket Connection

**Requirement:** Connect to Binance WebSocket API for BTC/USDT trade stream

**Implementation:** `binance_to_kafka.py`

### Features
- WebSocket connection to Binance `btcusdt@trade` stream
- Real-time trade extraction (30+ trades/sec)
- Auto-reconnection: exponential backoff, max 10 attempts
- Error handling: network errors, parse errors, timeouts
- Logging: file (`binance_producer.log`) + console
- Statistics: message counter, error tracking

### Trade Data Structure
```json
{
    "symbol": "BTCUSDT",
    "trade_id": 123456789,
    "price": 45123.45,
    "quantity": 0.5,
    "buyer_order_id": 789,
    "seller_order_id": 790,
    "trade_time": 1701234567000,
    "is_buyer_maker": false,
    "is_best_match": true,
    "ingestion_timestamp": 1701234567890
}
```

### Connection Settings
- Stream URL: `wss://stream.binance.com:9443/ws/btcusdt@trade`
- Ping interval: 30 seconds
- Timeout: 10 seconds
- Max reconnect attempts: 10

---

## IT-13: Kafka Topic & Producer

**Requirement 1: Initialize Kafka Topic**

Topic Configuration:
- Name: `binance_trades`
- Partitions: 3 (for parallel consumers)
- Replication Factor: 1 (development environment)
- Retention: 30 days (2592000000 ms)
- Compression: Snappy
- Min In-Sync Replicas: 1

Setup: `setup_kafka_topics.sh` or manual docker command

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic binance_trades \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=2592000000 \
  --config compression.type=snappy
```

**Requirement 2: Kafka Producer**

Implementation: `binance_to_kafka.py` (producer section)

### Producer Configuration
- `acks=all`: Wait for all replicas to acknowledge
- `retries=3`: Automatic retry on failure
- `max_in_flight_requests_per_connection=1`: Maintain message order
- `compression_type`: Snappy (2.5:1 compression ratio)
- `request_timeout_ms`: 10000
- `batch_size`: Default 16384

### Delivery Guarantees
- At-least-once delivery (with retries)
- Message ordering preserved
- No data loss (acks=all)
- Async sending with callbacks

### Error Handling
- Connection failures: auto-retry before topic send
- Network timeouts: retry with backoff
- Serialization errors: logged, message skipped
- Producer failures: logged to file

---

## IT-14: Kafka Consumer Verification

**Requirement:** Consumer to verify output data stream quality

Implementation: `kafka_consumer_verification.py`

### Validation Checks
- Required fields present: symbol, price, quantity, trade_id
- Data types: float for numeric values, string for symbol
- Value ranges: price > 0, quantity > 0
- Timestamp format: milliseconds

### Statistics Tracked (every 10 seconds)
- Messages received: total count
- Messages processed: successfully validated
- Messages errors: validation failures
- Success rate: (processed/received) * 100%
- Symbol breakdown: count per symbol
- Price range: min/max observed
- Total volume: sum of quantities
- Data latency: message age in seconds

### Consumer Group
- Group ID: `verification-consumer-group`
- Auto offset reset: `latest`
- Max poll interval: 5 seconds

---

## Setup Instructions

### Prerequisites
- Docker & Docker Compose installed
- Python 3.9+ with pip
- Kafka container running

### Step 1: Environment Setup

```bash
# Navigate to project
cd d:\crypto-realtime-pipeline

# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate
# Activate (Linux/Mac)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configuration

```bash
# Copy environment template
cp .env.example .env

# Content should be:
# KAFKA_BOOTSTRAP_SERVERS=localhost:29092
# KAFKA_TOPIC_NAME=binance_trades
# FASTAPI_PORT=8000
```

### Step 3: Start Infrastructure

```bash
# Start Kafka & Zookeeper
docker compose up -d zookeeper kafka

# Verify containers running
docker compose ps

# Create topic
./setup_kafka_topics.sh
# Or manual:
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Step 4: Run Components

**Terminal 1: Producer (IT-12, IT-13)**
```bash
.venv\Scripts\activate  # Windows
python binance_to_kafka.py
```

**Terminal 2: Verification Consumer (IT-14)**
```bash
.venv\Scripts\activate  # Windows
python kafka_consumer_verification.py
```

**Terminal 3: Backend (Optional)**
```bash
.venv\Scripts\activate  # Windows
python -m uvicorn main:app --reload
# Access: http://localhost:8000/docs
```

---

## Verification

### Test 1: Topic Created
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic binance_trades
```

Expected output shows 3 partitions, retention=30 days, compression=snappy

### Test 2: Producer Output
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic binance_trades --from-beginning --max-messages 5
```

Expected JSON messages from Binance

### Test 3: Verification Consumer
Consumer should display:
- Messages being received
- Statistics every 10 seconds
- 100% success rate
- 0 errors

### Performance Metrics
| Metric | Target | Typical |
|--------|--------|---------|
| Trades/sec | 10-50 | ~30 |
| Latency | <100ms | 50-80ms |
| Success rate | >99% | 100% |
| Message size | ~300 bytes | ~280 bytes |
| Compression ratio | 2-3:1 | 2.5:1 |

---

## Troubleshooting

### Connection Refused to Kafka
**Cause:** Kafka container not running  
**Solution:**
```bash
docker compose up -d zookeeper kafka
docker logs kafka  # Check logs
```

### Module Not Found
**Cause:** Dependencies not installed  
**Solution:**
```bash
pip install -r requirements.txt
```

### Binance WebSocket Timeout
**Cause:** Network issue  
**Solution:**
```bash
ping stream.binance.com
# Check internet connection
# Restart producer
```

### No Messages Appearing
**Cause:** Producer not sending or consumer not connected  
**Solution:**
```bash
# Check producer logs
tail -f binance_producer.log

# Check Kafka topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Manually check messages
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic binance_trades --from-beginning
```

### Kafka Topic Already Exists
**Solution:**
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic binance_trades
# Wait 2 seconds, then recreate topic
```

### Docker Out of Memory
**Solution:**
- Increase Docker memory allocation via Docker Desktop Settings
- Resources > Memory: set to 4GB or more
- Restart Docker

---

## Log Files

- **Producer:** `binance_producer.log`
  - Connection logs
  - Message send confirmations (every 100 messages)
  - Error logs
  - Statistics on shutdown

- **Verification Consumer:** `kafka_consumer_verification.log`
  - Consumer startup logs
  - Message validation logs (first 5 + every 100th)
  - Statistics snapshots (every 10 seconds)
  - Error logs

---

## Next Phase

After Phase 2 is running successfully:

**Phase 3: Spark Processing**
- Read from Kafka `binance_trades` topic
- Aggregate to OHLCV (1m, 5m, 1h, 4h, 1d)
- Apply window functions
- Write to storage (MongoDB, HDFS)

---

**Status:** Specification Complete  
**Files:** binance_to_kafka.py, kafka_consumer_verification.py, setup_kafka_topics.sh
