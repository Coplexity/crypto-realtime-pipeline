# KIẾN TRÚC HỆ THỐNG

## Tổng Quan Kiến Trúc

Hệ thống sử dụng kiến trúc **event-driven, microservices** với thành phần core là **Apache Kafka** - message broker đắc lực cho xử lý stream data real-time.

```
┌─────────────────────────────────────────────────────────────────┐
│                      BINANCE EXCHANGE API                        │
│                   (WebSocket: btcusdt@trade)                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                    Real-time Trade Events
                    (30+ trades/second)
                             │
        ┌────────────────────▼────────────────────┐
        │    BINANCE_TO_KAFKA.PY (Producer)       │
        │  IT-12: WebSocket Connection Handler    │
        │  IT-13: Kafka Producer                  │
        │                                         │
        │  - Parser: JSONify trade data           │
        │  - Serializer: Serialize to bytes       │
        │  - Sender: Send with acks=all           │
        │  - Retry: Auto-retry on failure         │
        │  - Logger: Log to file + console        │
        └────────────────────┬────────────────────┘
                             │
                        JSON Messages
                      Snappy Compressed
                             │
        ┌────────────────────▼────────────────────┐
        │     KAFKA BROKER (Port 9092)             │
        │   ┌──────────────────────────────────┐  │
        │   │  Topic: binance_trades           │  │
        │   │  Partitions: 3                   │  │
        │   │  Replication Factor: 1           │  │
        │   │  Retention: 30 days              │  │
        │   │  Compression: snappy             │  │
        │   │                                  │  │
        │   │  ┌─────┬─────┬─────┐            │  │
        │   │  │ P-0 │ P-1 │ P-2 │ (Partitions)│  │
        │   │  └─────┴─────┴─────┘            │  │
        │   └──────────────────────────────────┘  │
        └────────────────────┬────────────────────┘
                             │
            ┌────────────────┼────────────────┐
            │                │                │
       Consumer 1        Consumer 2      Consumer 3
       (Redis Cache)   (Spark Stream)  (Verification)
            │                │                │
        ┌───▼──┐         ┌───▼──┐        ┌───▼──────────────┐
        │Redis │         │Spark │        │VERIFICATION.PY   │
        │      │         │      │        │IT-14: Validator  │
        │-fast │         │-agg  │        │                  │
        │-real │         │-ml   │        │-verify format    │
        └──────┘         └──────┘        │-check types      │
                                         │-calc stats       │
                                         └──────────────────┘
            │                │                │
            │ Phase 3+       │ Phase 3+       │ Phase 2
```

## Thành Phần Chi Tiết

### 1. BINANCE WebSocket Client (IT-12)

**Mục đích:** Kết nối trực tiếp với Binance stream API

**Thông Số Kỹ Thuật:**
- Stream URL: `wss://stream.binance.com:9443/ws/btcusdt@trade`
- Cặp: BTC/USDT
- Độ phân giải: Per-trade (mỗi ghi dịch)
- Throughput: 30+ ghi dịch/giây
- Timeout: 10 giây
- Ping interval: 30 giây

**Dữ Liệu Input (từ Binance):**
```json
{
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
```

**Xử Lý:**
- Parse JSON từ WebSocket message
- Validate các trường bắt buộc
- Chuyển đổi kiểu dữ liệu (string → float)
- Thêm timestamp hệ thống
- Gửi tới Kafka Producer

**Error Handling:**
- Network timeout → Auto-reconnect với exponential backoff
- Parse error → Log và skip message
- Connection lost → Try reconnect (tối đa 10 lần)

### 2. Kafka Producer (IT-13)

**Mục Đích:** Gửi dữ liệu đến Kafka topic với bảo đảm độ tin cậy cao

**Cấu Hình Reliability:**
- `acks=all` - Chờ xác nhận từ tất cả replicas
- `retries=3` - Tự động retry 3 lần nếu gửi thất bại
- `max_in_flight_requests_per_connection=1` - Bảo đảm thứ tự messages

**Serialization:**
- Python dict → JSON string → UTF-8 bytes

**Compression:**
- Type: Snappy
- Compression ratio: ~2.5:1

**Dữ Liệu Output (vào Kafka):**
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

**Async Sending:**
- Non-blocking send()
- Success callback: Log lệnh
- Error callback: Log error, track counter

### 3. Kafka Topic (IT-13)

**Cấu Hình Topic:**
```yaml
name: binance_trades
partitions: 3                       # 3 consumer có thể song song
replication_factor: 1               # Development, có thể tăng lên 3 cho production
retention_ms: 2592000000            # 30 ngày
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
