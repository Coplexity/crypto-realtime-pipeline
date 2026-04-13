# KIẾN TRÚC HỆ THỐNG

## Tổng Quan Kiến Trúc (Phase 2 - Multi-Pair)

Hệ thống sử dụng kiến trúc **event-driven, microservices** với thành phần core là **Apache Kafka** - message broker đắc lực cho xử lý stream data real-time. Hiện tại hỗ trợ **200+ cryptocurrency trading pairs** via Binance Multistream WebSocket.

```
┌───────────────────────────────────────────────────────────────────┐
│                      BINANCE EXCHANGE API                         │
│   REST API: Pair discovery + volume filtering                     │
│   WebSocket Multistream: Real-time trade events (200+ pairs)      │
│   Example: btcusdt@trade, ethusdt@trade, bnbusdt@trade, ...      │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                    Real-time Trade Events
                    (1000+ trades/second)
                               │
        ┌──────────────────────▼──────────────────────┐
        │   BINANCE_TO_KAFKA.PY (Producer)            │
        │   IT-12 & IT-13: Multi-Pair Edition         │
        │                                              │
        │  ┌─ Pair Discovery ────────────────────┐   │
        │  │ • REST API fetch: GET /exchangeInfo │   │
        │  │ • Filter: volume threshold          │   │
        │  │ • Refresh: hourly or on-demand      │   │
        │  └─────────────────────────────────────┘   │
        │                                              │
        │  ┌─ WebSocket Multistream ──────────────┐   │
        │  │ • wss://.../stream?streams=...       │   │
        │  │ • Up to 200 pairs per connection     │   │
        │  │ • Auto-reconnect with backoff       │   │
        │  └─────────────────────────────────────┘   │
        │                                              │
        │  ┌─ Data Processing ────────────────────┐   │
        │  │ • Parser: Extract per-symbol trades │   │
        │  │ • Serializer: JSON → bytes          │   │
        │  │ • Sender: Kafka with acks=all       │   │
        │  │ • Statistics: Track per-pair        │   │
        │  └─────────────────────────────────────┘   │
        └──────────────────────┬──────────────────────┘
                               │
                        JSON Messages
                      (200+ symbols)
                      Snappy Compressed
                               │
        ┌──────────────────────▼──────────────────────┐
        │     KAFKA BROKER (Port 9092 - 3 partitions) │
        │   ┌────────────────────────────────────┐    │
        │   │  Topic: binance_trades             │    │
        │   │  Partitions: 3                     │    │
        │   │  Retention: 30 days                │    │
        │   │  Compression: snappy               │    │
        │   │                                    │    │
        │   │  ┌─────┬─────┬─────┐              │    │
        │   │  │ P-0 │ P-1 │ P-2 │ (Partitions)│    │
        │   │  └─────┴─────┴─────┘              │    │
        │   │  (Key-based: symbol → partition)  │    │
        │   └────────────────────────────────────┘    │
        └──────────────────────┬──────────────────────┘
                               │
            ┌──────────────────┼──────────────────┐
            │                  │                  │
       Consumer 1          Consumer 2        Consumer 3
       (Redis Cache)    (Spark Stream)   (Verification)
            │                  │                  │
        ┌───▼──┐           ┌───▼──┐        ┌────▼────────┐
        │Redis │           │Spark │        │VERIFICATION.│
        │      │           │      │        │PY (IT-14)   │
        │-hot  │           │-OHLCV│        │             │
        │-fast │           │-any  │        │-symbol check│
        │-multi│           │-symbol        │-schema val. │
        │ pair │           │-aggr │        │-per-pair st.│
        └──────┘           └──────┘        └─────────────┘
            │                  │                  │
            │ Phase 3+         │ Phase 3+        │ Phase 2
```

## Data Flow: Multi-Pair Architecture

**Step 1: Pair Discovery (Startup + Periodic Refresh)**
```
Config: BINANCE_PAIR_FILTER = "hot|all|whitelist"
        BINANCE_PAIR_MIN_VOLUME = 100000
        BINANCE_PAIR_WHITELIST = "btcusdt,ethusdt,..."

         ↓

Call REST API: GET /api/v3/exchangeInfo
Result: List of all USDT trading pairs

         ↓

Filter & Sort by volumeActive pairs: {btcusdt, ethusdt, bnbusdt, ...}
```

**Step 2: WebSocket Multistream Connection**
```
Pairs: {btcusdt, ethusdt, bnbusdt, adausdt, ... (up to 200)}

         ↓

Build URL: wss://stream.binance.com:9443/stream?streams=
           btcusdt@trade/ethusdt@trade/bnbusdt@trade/...

         ↓

Connect to WebSocket
```

**Step 3: Trade Event Processing**
```
Incoming Multistream Message:
{
    "stream": "btcusdt@trade",
    "data": {
        "e": "trade",
        "s": "BTCUSDT",
        "t": 12345,
        "p": "45000.00",
        "q": "0.1",
        ...
    }
}

         ↓

Extract symbol from "stream" field
Parse data payload
Add ingestion timestamp
Update per-pair statistics

         ↓

Serialize to JSON bytes

         ↓

Send to Kafka topic: binance_trades
(Key: symbol for partition assignment)
```

## Thành Phần Chi Tiết

### 1. BINANCE REST API Client (Pair Discovery)

**Endpoint:** `GET https://api.binance.com/api/v3/exchangeInfo`

**Mục Đích:** Lấy danh sách tất cả trading pairs

**Response Sample:**
```json
{
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            ...
        },
        {
            "symbol": "ETHUSDT",
            "status": "TRADING",
            ...
        },
        ...
    ]
}
```

**Filtering Logic (3 modes):**

1. **Mode: "hot"** (Volume-based)
   ```
   Fetch: GET /api/v3/ticker/24hr
   Filter: quoteAssetVolume >= MIN_VOLUME
   Result: Top liquid pairs only
   ```

2. **Mode: "all"**
   ```
   Use all USDT pairs from exchangeInfo
   No volume filtering
   Result: ALL active USDT pairs
   ```

3. **Mode: "whitelist"**
   ```
   Use explicit list from BINANCE_PAIR_WHITELIST
   Example: btcusdt,ethusdt,bnbusdt,adausdt
   Result: Only specified pairs
   ```

**Refresh Logic:**
- Call every `BINANCE_PAIR_UPDATE_INTERVAL` seconds (default: 3600)
- Background thread (daemon mode)
- Update active_pairs in-memory
- Log changes

### 2. BINANCE WebSocket Multistream (IT-12)

**Endpoint:** `wss://stream.binance.com:9443/stream?streams=symbol1@trade/symbol2@trade/...`

**Thông Số Kỹ Thuật:**
- Max streams per connection: **200**
- If >200 pairs needed: Spawn multiple producer instances
- Ping interval: 30 giây
- Timeout: 10 giây
- Throughput: 1000+ trades/second

**Message Format (Multistream):**
```json
{
    "stream": "btcusdt@trade",
    "data": {
        "e": "trade",
        "E": 1701234567890,
        "s": "BTCUSDT",
        "t": 123456789,
        "p": "45123.45",
        "q": "0.5",
        "b": 789,
        "a": 790,
        "T": 1701234567000,
        "m": false,
        "M": true
    }
}
```

**Error Handling:**
- Network timeout → Auto-reconnect (exponential backoff)
- Parse error → Log & skip
- Connection lost → Retry up to 10 times
- After 10 retries → Exit with error

### 3. Kafka Producer (IT-13)

**Topic Configuration:**
compression_type: snappy            # Giảm 60-70% dung lượng
min_insync_replicas: 1              # Tối thiểu 1 replica xác nhận trước khi ack
```

**Partition Strategy (Round-robin):**
```
P0: Messages 0, 3, 6, 9, ...
P1: Messages 1, 4, 7, 10, ...
P2: Messages 2, 5, 8, 11, ...
```

**Lợi Ích 3 Partitions:**
- Consumer 1 (Redis) - partition 0
- Consumer 2 (Spark) - partition 1
- Consumer 3 (Verification) - partition 2
- Parallel consumption, no contention

### 4. Kafka Consumer - Verification (IT-14)

**Mục Đích:** Xác minh chất lượng dữ liệu real-time

**Validation Rules:**

| Field | Type | Constraint |
|-------|------|-----------|
| symbol | string | = "BTCUSDT" |
| trade_id | int | > 0 |
| price | float | > 0 |
| quantity | float | > 0 |
| trade_time | int | milliseconds since epoch |
| is_buyer_maker | bool | true or false |
| ingestion_timestamp | int | ≥ trade_time |

**Statistics (Updated every 10 seconds):**
- Messages received: cumulative count
- Messages processed: successfully validated
- Messages errors: failed validation count
- Success rate: (processed / received) * 100%
- Symbol count: {BTCUSDT: N}
- Price range: min/max observed
- Total volume: sum of quantities
- Latency: message age in seconds

**Error Detection:**
- Missing required fields
- Type conversion errors  
- Invalid value ranges
- Duplicate trade_id (nếu có)

### 5. Supporting Infrastructure

#### Docker Compose Services

```yaml
zookeeper:
  Port: 2181
  Role: Kafka coordinator
  
kafka:
  Ports: 9092 (internal), 29092 (external)
  Topics: binance_trades (+ future topics)
  
namenode (HDFS):
  Port: 9870
  Role: Distributed storage coordinator
  
datanode (HDFS):
  Port: 9864
  Role: Distributed storage worker
  
spark-master:
  Port: 8080 (Web UI), 7077 (submit)
  Role: Cluster coordinator
  
spark-worker:
  Port: 8081 (Web UI)
  Role: Task executor
```

#### Network
- Driver: bridge (bigdata-network)
- Internal DNS: Using container names
- Example: `kafka:9092` (internal communication)

#### Volumes (Persistence)
- `hdfs_namenode` - HDFS metadata
- `hdfs_datanode` - HDFS data blocks
- `mongo_data` - MongoDB data
- `grafana_data` - Grafana dashboards

## Data Flow Model

### Luồng Dữ Liệu Chi Tiết

```
TIME = T0:
  Binance WebSocket
    → trigger on_message(raw_trade)
    
TIME = T0 + 2ms (Parser)
  Parse: raw_trade JSON
    → validate fields
    → convert types
    
TIME = T0 + 3ms (Enrich)
  Add metadata:
    → ingestion_timestamp = now()
    → producer_id = "binance-1"
    
TIME = T0 + 5ms (Producer)
  Kafka producer.send():
    → Serialize to JSON
    → Compress with Snappy
    → Select partition (round-robin)
    
TIME = T0 + 10ms (Kafka Write)
  Kafka broker:
    → Write to partition log
    → Replicate (if enabled)
    → Ack when acks=all satisfied
    
TIME = T0 + 15ms (Consumers Read begins)
  Consumer 1 (Redis):
    → Fetch from partition 0
    → Deserialize JSON
    → Cache in Redis
    
  Consumer 2 (Spark):
    → Fetch from partition 1
    → Process for aggregation
    
  Consumer 3 (Verification):
    → Fetch from partition 2
    → Validate
    → Update statistics
```

### Latency Budget (Phase 2)

| Component | Latency | Details |
|-----------|---------|---------|
| Binance → WebSocket | 50-100ms | Network + API |
| WebSocket parse | 2-3ms | JSON parsing |
| Kafka send | 3-5ms | Serialization + network |
| Kafka write | 5-10ms | Broker processing |
| Partition replica | 0-5ms | Write to segment |
| **Total Latency** | **60-130ms** | End-to-end |
| **Target** | **<150ms** | ✅ Achieved |

## Scalability & Performance

### Current Capacity (Phase 2)
- Throughput: 30-50 trades/second
- Message size: ~280 bytes
- Producer throughput: ~10 MB/s
- Consumer lag: <1 second

### Scaling Strategy (Phases 3+)
1. **Horizontal scaling:**
   - Add more Kafka partitions (now: 3)
   - Add consumer instances (now: 1-3)
   - Add Spark executors

2. **Vertical scaling:**
   - Increase broker heap
   - Increase producer batch size
   - Increase consumer fetch.min.bytes

3. **Optimization:**
   - Tune JVM settings
   - Optimize Snappy compression level
   - Cache frequently accessed data

## Deployment Tiers

### Development (Current)
- Single-node Kafka
- Single Spark master + 1 worker
- In-memory operations
- No replication

### Staging
- 3-node Kafka cluster
- Replication factor: 3
- Spark HA with multiple masters
- MongoDB replica set

### Production (Future)
- Managed Kafka service (AWS MSK, Confluent)
- Kubernetes orchestration
- Multi-master Spark
- Monitoring stack (Prometheus, Grafana)
- Backup & disaster recovery

## Monitoring & Observability

### Metrics Collection Points
1. **Producer Metrics:**
   - Messages sent/sec
   - Bytes sent/sec
   - Error rate
   - Batch size
   - Compression ratio

2. **Kafka Metrics:**
   - Topic size
   - Partition lag
   - Consumer lag
   - Replication factor health

3. **Consumer Metrics:**
   - Messages consumed/sec
   - Processing latency
   - Error rate
   - Validation success rate

### Logging Strategy
- **Producer:** `binance_producer.log` (INFO level)
- **Consumer:** `kafka_consumer_verification.log` (INFO level)
- **Kafka:** `docker logs kafka` (WARN level)
- **Retention:** 30 days in files, 7 days in Docker

## Security Considerations

### Current (Development)
- No authentication required
- Internal network only
- .env file contains secrets

### Production Improvements (Future)
- Kafka ACLs (Access Control Lists)
- SSL/TLS encryption
- SASL authentication
- Secret management (Vault)
- Network policies

---

**Phiên Bản:** 2.0  
**Cục Bộ:** April 7, 2026
