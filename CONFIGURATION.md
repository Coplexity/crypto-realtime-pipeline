# HƯỚNG DẨN CẤU HÌNH

## Biến Môi Trường

### .env Template

Tạo file `.env` từ `.env.example`:

```bash
# ============================================
# KAFKA CONFIGURATION
# ============================================

# Kafka broker address
# Default: localhost:9092 (development)
# Production: kafka.example.com:9092
KAFKA_BROKER=localhost:9092

# Kafka topic name
KAFKA_TOPIC=binance_trades

# Consumer group ID (must be unique per consumer instance)
KAFKA_CONSUMER_GROUP=verification-consumer-group

# Number of partitions to consume from
# Default: 3 (matches topic partition count)
KAFKA_PARTITIONS=3

# ============================================
# BINANCE CONFIGURATION
# ============================================

# Binance WebSocket endpoint
# BTC/USDT trading pair
BINANCE_STREAM_URL=wss://stream.binance.com:9443/ws/btcusdt@trade

# Reconnect settings
# Maximum reconnection attempts (before giving up)
BINANCE_MAX_RECONNECT_ATTEMPTS=10

# Exponential backoff for reconnection
# Initial delay: 1 second, max: BINANCE_RECONNECT_MAX_DELAY
BINANCE_RECONNECT_INITIAL_DELAY=1
BINANCE_RECONNECT_MAX_DELAY=30

# WebSocket timeout in seconds
# Connection timeout when no ping/pong from server
BINANCE_WEBSOCKET_TIMEOUT=10

# ============================================
# LOGGING CONFIGURATION
# ============================================

# Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
# Best practice: INFO for production, DEBUG for development
LOG_LEVEL=INFO

# Log file paths
LOG_FILE_PRODUCER=binance_producer.log
LOG_FILE_CONSUMER=kafka_consumer_verification.log

# Log file max size (bytes)
# Default: 10MB
LOG_MAX_BYTES=10485760

# Number of backup log files to keep
LOG_BACKUP_COUNT=5

# ============================================
# PERFORMANCE TUNING
# ============================================

# Producer batch size (messages)
# Default: 100
# Higher = better throughput, higher latency
PRODUCER_BATCH_SIZE=100

# Producer flush interval (milliseconds)
# Default: 1000 (1 second)
PRODUCER_FLUSH_INTERVAL=1000

# Consumer fetch size (bytes)
# Default: 1MB
# Higher = more memory usage, lower latency
CONSUMER_FETCH_MIN_BYTES=1048576

# Consumer poll timeout (milliseconds)
# Default: 5000 (5 seconds)
CONSUMER_POLL_TIMEOUT=5000

# ============================================
# STATISTICS & MONITORING
# ============================================

# Statistics display interval (seconds)
# How often to print statistics
STATS_INTERVAL=10

# Enable detailed metrics
ENABLE_DETAILED_METRICS=true

# ============================================
# VALIDATION RULES (for consumer)
# ============================================

# Validate required fields (comma-separated)
REQUIRED_FIELDS=symbol,trade_id,price,quantity,trade_time

# Min/max price boundaries for BTC/USDT
PRICE_MIN=1000
PRICE_MAX=1000000

# Min/max quantity boundaries
QUANTITY_MIN=0.001
QUANTITY_MAX=1000

# ============================================
# DOCKER & INFRASTRUCTURE
# ============================================

# Docker network name
DOCKER_NETWORK=bigdata-network

# Zookeeper address (for Kafka coordination)
ZOOKEEPER_HOST=zookeeper
ZOOKEEPER_PORT=2181

# MongoDB configuration (Phase 3+)
MONGODB_URI=mongodb://mongo:27017
MONGODB_DB=crypto_trades

# Redis configuration (Phase 3+)
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# ============================================
# DEBUGGING & DEVELOPMENT
# ============================================

# Enable debug mode (verbose logging)
DEBUG_MODE=false

# Enable test mode (use mock data instead of real Binance)
TEST_MODE=false

# Number of messages to process in test mode
TEST_MESSAGE_COUNT=1000
```

### Tạo .env File

```bash
# Copy template
cp .env.example .env

# Edit các giá trị cần thiết
nano .env  # Linux/Mac
notepad .env  # Windows
```

---

## Cấu Hình Kafka

### docker-compose.yml (Services)

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_SYNC_LIMIT: 2
      ZOOKEEPER_INIT_LIMIT: 5
    ports:
      - "2181:2181"
    volumes:
      - zookeeper_data:/var/lib/zookeeper/data
      - zookeeper_logs:/var/lib/zookeeper/log
    networks:
      - bigdata-network

  kafka:
    image: confluentinc/cp-kafka:7.3.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      # Kafka broker configuration
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      
      # Listener configuration
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      
      # Performance tuning
      KAFKA_NUM_NETWORK_THREADS: 8
      KAFKA_NUM_IO_THREADS: 8
      KAFKA_SOCKET_SEND_BUFFER_BYTES: 102400
      KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: 102400
      KAFKA_SOCKET_REQUEST_MAX_BYTES: 104857600
      
      # Log retention
      KAFKA_LOG_RETENTION_HOURS: 720  # 30 days
      KAFKA_LOG_RETENTION_BYTES: -1
      KAFKA_LOG_SEGMENT_BYTES: 1073741824  # 1GB
      
      # Compression
      KAFKA_COMPRESSION_TYPE: snappy
      
      # Heap size
      KAFKA_HEAP_OPTS: "-Xms1G -Xmx2G"
      
      # JVM settings
      KAFKA_JVM_PERFORMANCE_OPTS: "-XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35"
    volumes:
      - kafka_data:/var/lib/kafka/data
    networks:
      - bigdata-network

volumes:
  zookeeper_data:
    driver: local
  zookeeper_logs:
    driver: local
  kafka_data:
    driver: local

networks:
  bigdata-network:
    driver: bridge
```

### Kafka Topic Configuration

#### Via Command Line

```bash
# Create topic with optimal settings
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic binance_trades \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --config segment.ms=86400000 \
  --config compression.type=snappy \
  --config min.insync.replicas=1 \
  --config cleanup.policy=delete
```

#### Describe Topic

```bash
docker exec kafka kafka-topics \
  --describe \
  --bootstrap-server localhost:9092 \
  --topic binance_trades

# Output:
# Topic: binance_trades PartitionCount: 3 ReplicationFactor: 1
#   Topic: binance_trades Partition: 0
#     Leader: 1    Replicas: [1]   Isr: [1]
#   Topic: binance_trades Partition: 1
#     Leader: 1    Replicas: [1]   Isr: [1]
#   Topic: binance_trades Partition: 2
#     Leader: 1    Replicas: [1]   Isr: [1]
```

#### Modify Topic

```bash
# Increase partitions (if needed)
docker exec kafka kafka-topics --alter \
  --bootstrap-server localhost:9092 \
  --topic binance_trades \
  --partitions 6

# Change retention
docker exec kafka kafka-configs --alter \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name binance_trades \
  --add-config retention.ms=5184000000  # 60 days
```

---

## Cấu Hình Producer

### binance_to_kafka.py Settings

```python
# Kafka Producer Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'acks': 'all',                          # Wait for all replicas
    'retries': 3,                           # Retry failed sends
    'max_in_flight_requests_per_connection': 1,  # Ordered delivery
    'compression_type': 'snappy',           # Compression
    'batch_size': 16384,                    # 16KB batch
    'linger_ms': 10,                        # Wait 10ms for batching
    'buffer_memory': 33554432,              # 32MB buffer
    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
}

# WebSocket Configuration
BINANCE_CONFIG = {
    'stream_url': 'wss://stream.binance.com:9443/ws/btcusdt@trade',
    'max_reconnect_attempts': 10,
    'initial_reconnect_delay': 1,           # 1 second
    'max_reconnect_delay': 30,              # 30 seconds
    'timeout': 10,                          # Connection timeout
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'file': 'binance_producer.log',
    'max_bytes': 10485760,                  # 10MB
    'backup_count': 5,
}
```

### Performance Tuning untuk Producer

| Setting | Value | Impact | Trade-off |
|---------|-------|--------|-----------|
| `batch_size` | 16KB | Throughput ↑ | Latency ↑ |
| `linger_ms` | 10ms | Throughput ↑ | Latency ↑ |
| `buffer_memory` | 32MB | Concurrency ↑ | Memory ↑ |
| `acks` | all | Reliability ↑ | Latency ↑ |
| `retries` | 3 | Reliability ↑ | Complexity ↑ |

---

## Cấu Hình Consumer

### kafka_consumer_verification.py Settings

```python
# Kafka Consumer Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'verification-consumer-group',
    'auto_offset_reset': 'latest',          # Start from latest offset
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 5000,        # Commit every 5 seconds
    'max_poll_records': 100,                # Fetch up to 100 records
    'session_timeout_ms': 10000,            # 10 second session timeout
    'value_deserializer': lambda v: json.loads(v.decode('utf-8')),
}

# Validation Rules
VALIDATION_RULES = {
    'required_fields': ['symbol', 'trade_id', 'price', 'quantity', 'trade_time', 'is_buyer_maker'],
    'field_types': {
        'symbol': str,
        'trade_id': int,
        'price': float,
        'quantity': float,
        'trade_time': int,
        'is_buyer_maker': bool,
    },
    'value_ranges': {
        'price': (1000, 1000000),           # BTC price range
        'quantity': (0.001, 1000),          # Trade size range
        'symbol': ['BTCUSDT'],              # Allowed pairs
    }
}

# Statistics Configuration
STATS_CONFIG = {
    'display_interval': 10,                 # Print every 10 seconds
    'track_latency': True,
    'track_volume': True,
    'enable_detailed_metrics': True,
}
```

---

## Database Configuration (Phase 3+)

### MongoDB

```yaml
# docker-compose.yml
mongodb:
  image: mongo:5.0
  ports:
    - "27017:27017"
  environment:
    MONGO_INITDB_ROOT_USERNAME: root
    MONGO_INITDB_ROOT_PASSWORD: change_me
    MONGO_INITDB_DATABASE: crypto_trades
  volumes:
    - mongo_data:/data/db
  networks:
    - bigdata-network
```

### Redis

```yaml
# docker-compose.yml
redis:
  image: redis:7-alpine
  ports:
    - "6379:6379"
  command: redis-server --appendonly yes --maxmemory 512mb --maxmemory-policy allkeys-lru
  volumes:
    - redis_data:/data
  networks:
    - bigdata-network
```

---

## Network Configuration

### Port Mapping Reference

| Service | Port | Protocol | Purpose |
|---------|------|----------|---------|
| Kafka Broker | 9092 | TCP | Internal broker communication |
|  | 29092 | TCP | External broker communication |
| Zookeeper | 2181 | TCP | Coordination server |
| HDFS NameNode | 9870 | HTTP | Web UI |
| HDFS DataNode | 9864 | HTTP | Web UI |
| Spark Master | 7077 | TCP | Submit applications |
|  | 8080 | HTTP | Web UI |
| Spark Worker | 8081 | HTTP | Web UI |
| MongoDB | 27017 | TCP | Database |
| Redis | 6379 | TCP | Cache |

### Firewall Rules (Production)

```bash
# Linux (iptables)
sudo iptables -A INPUT -p tcp --dport 9092 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 2181 -j ACCEPT

# UFW (Ubuntu)
sudo ufw allow 9092/tcp
sudo ufw allow 2181/tcp

# Windows (PowerShell)
New-NetFirewallRule -DisplayName "Kafka" -Direction Inbound -LocalPort 9092 -Protocol TCP -Action Allow
```

---

## Performance Tuning

### CPU Optimization

```bash
# Set CPU affinity (Linux)
taskset -c 0,1,2,3 python binance_to_kafka.py

# Monitor CPU usage
top -p $(pgrep -f "binance_to_kafka")
```

### Memory Optimization

```bash
# Monitor memory
docker stats kafka

# Adjust Kafka heap
export KAFKA_HEAP_OPTS="-Xms2G -Xmx4G"
```

### Network Optimization

```bash
# Monitor network throughput
iftop -i eth0

# Adjust socket buffer
docker exec kafka kafka-configs --alter \
  --bootstrap-server localhost:9092 \
  --entity-type brokers \
  --entity-name 1 \
  --add-config socket.send.buffer.bytes=102400
```

---

## Monitoring Configuration

### Metrics Export

```python
# Enable Prometheus metrics
from prometheus_client import start_http_server, Counter

messages_total = Counter('kafka_messages_total', 'Total messages')
errors_total = Counter('kafka_errors_total', 'Total errors')

# Start HTTP server on port 8000
start_http_server(8000)
```

### Health Check

```bash
# Check Kafka broker health
curl -s localhost:9092/metrics | grep kafka_broker_info

# Check topic status
docker exec kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic binance_trades
```

---

## Security Configuration

### Authentication (Future)

```yaml
# SASL/SCRAM configuration
kafka:
  environment:
    KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: SCRAM-SHA-256
    KAFKA_SECURITY_INTER_BROKER_PROTOCOL: SASL_PLAINTEXT
    KAFKA_SASL_JAAS_CONFIG: |
      org.apache.kafka.common.security.scram.ScramLoginModule required
      username="admin" password="admin-secret";
```

### SSL/TLS (Future)

```yaml
# SSL configuration
kafka:
  environment:
    KAFKA_LISTENERS: SSL://0.0.0.0:9093
    KAFKA_ADVERTISED_LISTENERS: SSL://kafka:9093
    KAFKA_SECURITY_PROTOCOL: SSL
    KAFKA_SSL_KEYSTORE_LOCATION: /var/secrets/kafka.keystore
    KAFKA_SSL_KEYSTORE_PASSWORD: keystore_password
```

---

## Backup & Recovery

### Backup Kafka Topics

```bash
# Export topic data
docker exec kafka kafka-console-consumer \
  --topic binance_trades \
  --bootstrap-server localhost:9092 \
  --from-beginning > backup_trades.json

# Restore from backup
cat backup_trades.json | \
docker exec -i kafka kafka-console-producer \
  --topic binance_trades \
  --bootstrap-server localhost:9092
```

### Volume Backups

```bash
# Backup Docker volumes
docker run --rm \
  -v kafka_data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/kafka_data.tar.gz /data

# Restore volume
docker run --rm \
  -v kafka_data:/data \
  -v $(pwd):/backup \
  alpine tar xzf /backup/kafka_data.tar.gz -C /
```

---

**Phiên Bản:** 2.0  
**Cập Nhật:** April 7, 2026  
**Tham Khảo:** INSTALLATION.md, TROUBLESHOOTING.md
