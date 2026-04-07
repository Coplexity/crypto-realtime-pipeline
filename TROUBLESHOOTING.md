# HƯỚNG DẨN KHẮC PHỤC SỰ CỐ

## I. Vấn Đề Docker & Infrastructure

### 1.1 Docker daemon không chạy

**Triệu chứng:**
```
Cannot connect to Docker daemon at unix:///var/run/docker.sock
```

**Nguyên Nhân:**
- Docker service chưa được khởi động
- Permission denied cho Docker socket

**Giải Pháp:**

```bash
# Linux
sudo systemctl status docker
sudo systemctl start docker

# macOS
brew services start docker

# Windows
# Mở Docker Desktop application

# Kiểm tra Docker
docker ps
```

---

### 1.2 Docker volumes không mount đúng

**Triệu chứng:**
```
Error response from daemon: invalid mount config for type "volume"
```

**Nguyên Nhân:**
- Cấu hình docker-compose.yml sai
- Volume không tồn tại
- Permission issue

**Giải Pháp:**

```bash
# Kiểm tra volumes
docker volume ls

# Xóa và tạo lại
docker volume rm kafka_data
docker-compose down -v
docker-compose up -d

# Kiểm tra log
docker logs kafka | grep -i "mount"
```

---

### 1.3 Containers không start

**Triệu chứng:**
```
ERROR: for kafka Cannot start service kafka: OCI runtime create failed
```

**Nguyên Nhân:**
- Resource không đủ (CPU, memory)
- Port bị chiếm
- Image corrupt

**Giải Pháp:**

```bash
# Kiểm tra resource
docker stats
free -h
df -h

# Kiểm tra port
netstat -tulpn | grep 9092  # Linux
lsof -iTCP -sTCP:LISTEN -P -n | grep 9092  # macOS
netstat -ano | findstr 9092  # Windows

# Giải phóng port (nếu cần)
lsof -ti:9092 | xargs kill -9  # Linux

# Rebuild containers
docker-compose down
docker image prune -a
docker-compose up -d
```

---

### 1.4 Container restart liên tục

**Triệu chứng:**
```
Container kafka exited with code 1
```

**Nguyên Nhân:**
- Configuration error trong docker-compose.yml
- Memory không đủ
- Disk space không đủ

**Giải Pháp:**

```bash
# Xem log chi tiết
docker logs -f kafka

# Kiểm tra disk space
df -h

# Thêm swap memory
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

# Kiểm tra Kafka config
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

---

## II. Vấn Đề Kafka

### 2.1 Kafka broker không accessible

**Triệu chứng:**
```
KafkaUnavailableError: Unable to connect to broker
Connection refused to host: localhost:9092
```

**Nguyên Nhân:**
- Kafka container chưa fully start
- Zookeeper chưa ready
- Network isolation (Docker desktop)

**Giải Pháp:**

```bash
# Kiểm tra container trạng thái
docker ps -a | grep -E "(kafka|zookeeper)"

# Xem Kafka logs
docker logs kafka

# Đợi Kafka ready (1-2 phút)
docker exec kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092

# Kiểm tra Zookeeper
docker exec zookeeper zkServer.sh status

# Timeout/retry issue? Reset
docker-compose restart kafka zookeeper
sleep 10

# Kiểm tra lại
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

---

### 2.2 Topic không tồn tại

**Triệu chứng:**
```
UNKNOWN_TOPIC_OR_PART (topic=binance_trades): The request is for a topic that does not exist
```

**Nguyên Nhân:**
- Topic chưa được tạo
- Topic bị xóa
- Topic name sai

**Giải Pháp:**

```bash
# Kiểm tra topics hiện có
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Tạo topic
bash setup_kafka_topics.sh

# Hoặc manual
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic binance_trades \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --config compression.type=snappy

# Kiểm tra topic info
docker exec kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic binance_trades
```

---

### 2.3 Consumer lag cao

**Triệu chứng:**
```
Consumer lag detected: 10000 messages behind
```

**Nguyên Nhân:**
- Consumer processing quá chậm
- Message processing error
- Consumer timeout/crash

**Giải Pháp:**

```bash
# Kiểm tra consumer lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group verification-consumer-group \
  --describe

# Output:
# GROUP           TOPIC       PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG
# verif...        binance...  0         12345          12400          55

# Optimization tuples
# 1. Tăng producer throughput (nếu cần)
# 2. Tối ưu consumer processing
# 3. Thêm consumer instances

# Scale consumers
# Run multiple instances with same group_id
for i in {1..3}; do
  python kafka_consumer_verification.py --consumer-id=$i &
done

# Reset offset (nếu cần chạy lại từ đầu)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group verification-consumer-group \
  --topic binance_trades \
  --reset-offsets \
  --to-earliest \
  --execute
```

---

### 2.4 Message format error

**Triệu chứng:**
```
Serialization error: expected string, got bytes
```

**Nguyên Nhân:**
- Serializer/Deserializer mismatch
- Encoding issue
- Corrupt message

**Giải Pháp:**

```python
# Producer - verify serialization
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Consumer - verify deserialization
consumer = KafkaConsumer(
    'binance_trades',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Test messages
import json
test_msg = {"symbol": "BTCUSDT", "price": 45000.0}
serialized = json.dumps(test_msg).encode('utf-8')
deserialized = json.loads(serialized.decode('utf-8'))
print(f"Match: {test_msg == deserialized}")
```

---

### 2.5 Partition reassignment

**Triệu chứng:**
```
Partition rebalancing in progress
```

**Nguyên Nhân:**
- Consumer group thay đổi
- Broker down/up
- Manual topic alter

**Giải Pháp:**

```bash
# Kiểm tra rebalancing status
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group verification-consumer-group \
  --describe --members

# Đợi rebalancing xong (1-5 phút)
# Monitor logs
docker logs -f kafka | grep -i "rebalanc"

# Manual rebalance (nếu cần)
docker exec kafka kafka-reassign-partitions \
  --bootstrap-server localhost:9092 \
  --generate \
  --topics-to-move-json-file topics.json \
  --broker-list "1,2,3"
```

---

## III. Vấn Đề Python & Dependencies

### 3.1 Module không tìm thấy

**Triệu chứng:**
```
ModuleNotFoundError: No module named 'kafka'
```

**Nguyên Nhân:**
- Virtual environment chưa activate
- Dependencies chưa cài
- Wrong Python version

**Giải Pháp:**

```bash
# Kiểm tra venv activate
which python  # Should show venv path

# Activate venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\Activate.ps1  # Windows PowerShell

# Reinstall dependencies
pip install -r requirements.txt --upgrade

# Kiểm tra installation
python -c "import kafka; print(kafka.__version__)"

# Nếu vẫn fail: create new venv
rm -rf venv
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

### 3.2 Python version incompatible

**Triệu chứng:**
```
ValueError: Python requires version 3.9+ (current: 3.8.10)
```

**Nguyên Nhân:**
- Python version quá cũ
- Wrong Python interpreter

**Giải Pháp:**

```bash
# Kiểm tra version
python --version
python3 --version
python3.11 --version

# Nếu cần update
sudo apt-get install python3.11  # Linux
brew install python@3.11  # macOS

# Create venv with specific version
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Verify
python --version  # Should be 3.9+
```

---

### 3.3 Package version conflict

**Triệu chứng:**
```
Requirement already satisfied: kafka-python==2.0.0, but will break dependencies
```

**Nguyên Nhân:**
- Requirements.txt outdated
- Breaking changes giữa versions
- Installation order issue

**Giải Pháp:**

```bash
# Kiểm tra conflicts
pip check

# Resolve: Clean install
pip freeze > current.txt
pip uninstall -r current.txt -y
pip install -r requirements.txt

# Hoặc upgrade pip/setuptools
pip install --upgrade pip setuptools wheel

# Hoặc relaxed versioning
pip install kafka-python websocket-client fastapi --no-deps

# Rebuild lock file
pip freeze > requirements.lock.txt
```

---

### 3.4 Import path issue

**Triệu chứng:**
```
ModuleNotFoundError: No module named 'config'
```

**Nguyên Nhân:**
- PYTHONPATH chưa set
- Importing from wrong directory
- Circular imports

**Giải Pháp:**

```bash
# Set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/path/to/project"

# Hoặc thêm vào code
import sys
sys.path.insert(0, '/path/to/project')
from config import KAFKA_CONFIG

# Kiểm tra import
python -c "import binance_to_kafka; print('✓ Import successful')"
```

---

## IV. Vấn Đề Binance WebSocket

### 4.1 WebSocket connection timeout

**Triệu chứng:**
```
WebSocketTimeoutException: Connection timeout
```

**Nguyên Nhân:**
- Network connectivity issue
- Firewall blocking
- Binance server down
- DNS resolution failed

**Giải Pháp:**

```bash
# Kiểm tra network
ping api.binance.com

# Kiểm tra DNS
nslookup stream.binance.com
dig stream.binance.com

# Kiểm tra firewall
curl -I https://stream.binance.com

# Test WebSocket connection
python -c "
import websocket
try:
    ws = websocket.WebSocket()
    ws.connect('wss://stream.binance.com:9443/ws/btcusdt@trade', timeout=3)
    print('✓ WebSocket connected')
    ws.close()
except Exception as e:
    print(f'✗ Error: {e}')
"

# Nếu firewall issue
# Allow outbound:
# Port 443 (HTTPS/WSS)
# Host: stream.binance.com
```

---

### 4.2 WebSocket message parse error

**Triệu chứng:**
```
JSONDecodeError: Expecting value: line 1 column 1 (char 0)
```

**Nguyên Nhân:**
- Empty message
- Invalid message format
- WebSocket frame error

**Giải Pháp:**

```python
import json
import websocket

def on_message(ws, message):
    try:
        # Validate message not empty
        if not message:
            print("Skipping empty message")
            return
        
        # Parse JSON
        data = json.loads(message)
        
        # Validate structure
        required_fields = ['e', 'E', 's', 't', 'p', 'q']
        if not all(field in data for field in required_fields):
            print(f"Skipping invalid message: missing fields")
            return
        
        print(f"✓ Valid message: {data['s']} {data['p']}")
        
    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}, message: {message[:100]}")
    except Exception as e:
        print(f"Processing error: {e}")

ws = websocket.WebSocketApp(
    'wss://stream.binance.com:9443/ws/btcusdt@trade',
    on_message=on_message
)
ws.run_forever()
```

---

### 4.3 WebSocket disconnection

**Triệu chứng:**
```
Connection closed: 1000
```

**Nguyên Nhân:**
- Network interruption
- Binance server maintenance
- Ping/pong timeout
- Rate limiting

**Giải Pháp:**

```python
import websocket
import time

def on_close(ws, close_status_code, close_msg):
    print(f"Connection closed: {close_status_code} {close_msg}")
    print("Attempting to reconnect...")
    time.sleep(5)
    # Reconnect logic here

def on_error(ws, error):
    print(f"WebSocket error: {error}")
    # Error handling

ws = websocket.WebSocketApp(
    'wss://stream.binance.com:9443/ws/btcusdt@trade',
    on_close=on_close,
    on_error=on_error
)

# Add retry logic
max_retries = 10
retry_count = 0
initial_delay = 1
max_delay = 30

while retry_count < max_retries:
    try:
        ws.run_forever()
    except Exception as e:
        retry_count += 1
        delay = min(initial_delay * (2 ** retry_count), max_delay)
        print(f"Retry {retry_count}/{max_retries} after {delay}s")
        time.sleep(delay)
```

---

## V. Vấn Đề Performance

### 5.1 High CPU usage

**Triệu chứng:**
```
CPU usage: 90%+ (continuously)
```

**Nguyên Nhân:**
- Busy loop without sleep
- Excessive logging
- Inefficient message parsing
- Memory leak causing GC thrashing

**Giải Pháp:**

```bash
# Monitor CPU
top -p $(pgrep -f "binance_to_kafka")
psutil  # Python library

# Profile code
python -m cProfile -s cumtime binance_to_kafka.py

# Optimize
# 1. Add sleep in polling loop
time.sleep(0.1)

# 2. Reduce logging frequency
# Only log every N messages
if message_count % 100 == 0:
    logger.info(f"Processed {message_count} messages")

# 3. Batch message processing
batch = []
for msg in messages:
    batch.append(msg)
    if len(batch) >= 100:
        process_batch(batch)
        batch = []
```

---

### 5.2 High memory usage

**Triệu chứng:**
```
MemoryError: Unable to allocate X MB
```

**Nguyên Nhân:**
- Message buffer growing indefinitely
- Unflushed Kafka producer
- Memory leak in consumer
- Large message processing

**Giải Pháp:**

```python
# Monitor memory
import psutil
process = psutil.Process()
print(f"Memory usage: {process.memory_info().rss / 1024**2:.2f} MB")

# Fix memory issues
# 1. Limit buffer size
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    buffer_memory=33554432,  # 32MB limit
    linger_ms=10,
)

# 2. Flush periodically
for msg in messages:
    producer.send('topic', msg)
    producer.flush(timeout=10)  # Flush every message

# 3. Use generator instead of list
def message_generator():
    for item in large_collection:
        yield item

# 4. Monitor
for msg in message_generator():
    process_message(msg)
    if process.memory_info().rss > 1024**3:  # If >1GB
        print("Memory threshold exceeded, exiting")
        break
```

---

### 5.3 High latency

**Triệu chứng:**
```
Message latency: 5000ms (should be <150ms)
```

**Nguyên Nhân:**
- Large batch_size
- High linger_ms delay
- Consumer processing slow
- Kafka broker overloaded

**Giải Pháp:**

```python
# Producer optimization
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    batch_size=4096,      # Smaller batch = lower latency
    linger_ms=5,          # Less delay
    compression_type=None,  # No compression (slower)
    acks=1,               # Faster acks
)

# Consumer optimization
consumer = KafkaConsumer(
    'binance_trades',
    bootstrap_servers=['localhost:9092'],
    max_poll_records=10,  # Smaller polls = lower latency
    session_timeout_ms=6000,
    request_timeout_ms=60000,
)

# Measure latency
import time
start = time.time()
# ... process message ...
latency = (time.time() - start) * 1000  # ms
print(f"Processing latency: {latency:.2f}ms")

# Target: <150ms
```

---

## VI. Vấn Đề Validation & Data Quality

### 6.1 Validation failures

**Triệu chứng:**
```
Validation error: missing required field 'price'
Success rate: 95.3%
```

**Nguyên Nhân:**
- Malformed Binance message
- Type conversion error
- Null/empty values

**Giải Pháp:**

```python
def verify_message(msg):
    """Enhanced validation with logging"""
    try:
        # Required fields
        required = ['symbol', 'trade_id', 'price', 'quantity']
        for field in required:
            if field not in msg:
                raise ValueError(f"Missing field: {field}")
        
        # Type conversion
        msg['price'] = float(msg['price'])
        msg['quantity'] = float(msg['quantity'])
        msg['trade_id'] = int(msg['trade_id'])
        
        # Range validation
        if not (1000 < msg['price'] < 1000000):
            raise ValueError(f"Price out of range: {msg['price']}")
        
        if not (0 < msg['quantity'] < 1000):
            raise ValueError(f"Quantity out of range: {msg['quantity']}")
        
        return True
        
    except (KeyError, TypeError, ValueError) as e:
        logger.warning(f"Validation failed: {e}, message: {msg}")
        return False

# Test
bad_msg = {"symbol": "BTCUSDT", "trade_id": "abc"}  # Missing price
result = verify_message(bad_msg)
print(f"Valid: {result}")
```

---

### 6.2 Duplicate messages

**Triệu chứng:**
```
Duplicate trade_id detected: 123456 (seen before)
```

**Nguyên Nhân:**
- Consumer offset reset
- Producer retry
- Kafka replication

**Giải Pháp:**

```python
from collections import defaultdict

# Track seen IDs (last 10000)
from collections import deque
seen_ids = deque(maxlen=10000)

def process_message(msg):
    trade_id = msg['trade_id']
    
    if trade_id in seen_ids:
        logger.warning(f"Duplicate trade_id: {trade_id}")
        return False
    
    seen_ids.append(trade_id)
    
    # Process...
    return True

# Or use database deduplication
def deduplicate_in_db(trade_id):
    """Check MongoDB for existing trade"""
    existing = db.trades.find_one({'trade_id': trade_id})
    if existing:
        logger.info(f"Already processed: {trade_id}")
        return False
    return True
```

---

## VII. Troubleshooting Checklist

### Startup Checklist

- [ ] Docker daemon running
- [ ] docker-compose.yml valid YAML
- [ ] Ports 9092, 2181 available
- [ ] Disk space >10GB
- [ ] RAM >4GB available
- [ ] Python venv activated
- [ ] Requirements installed
- [ ] .env file created

### Runtime Checklist

- [ ] Kafka broker accessible (`kafka-broker-api-versions`)
- [ ] Topic exists (`kafka-topics --list`)
- [ ] Producer messages sent (check logs)
- [ ] Consumer messages received (check logs)
- [ ] No errors in logs (`docker logs kafka`)
- [ ] CPU <80%
- [ ] Memory <80%
- [ ] Consumer lag <100

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG

# Run with verbose output
python -u binance_to_kafka.py 2>&1 | tee debug.log

# Monitor in real-time
tail -f binance_producer.log | grep -i error

# Check system resources
watch -n 1 'docker stats'
```

---

**Phiên Bản:** 2.0  
**Cập Nhật:** April 7, 2026  
**Support:** Xem ARCHITECTURE.md, CONFIGURATION.md
