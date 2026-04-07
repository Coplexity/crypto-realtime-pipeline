# HƯỚNG DẪN CÀI ĐẶT

## Yêu Cầu Hệ Thống

### Phần Mềm

| Công Cụ | Phiên Bản | Mục Đích |
|---------|----------|---------|
| **Docker** | 24.0+ | Container runtime |
| **Docker Compose** | 2.20+ | Orchestration |
| **Python** | 3.9 - 3.11 | Runtime |
| **Git** | 2.40+ | Version control |
| **Bash** (Linux/Mac) hoặc **PowerShell** (Windows) | - | Shell scripting |

### Phần Cứng

| Tài Nguyên | Tối Thiểu | Khuyến Nghị |
|-----------|----------|-----------|
| **CPU** | 2 cores | 4+ cores |
| **RAM** | 4 GB | 8+ GB |
| **Disk** | 10 GB | 50+ GB |
| **Connection** | 1 Mbps | 10+ Mbps |

### Hệ Điều Hành

- ✅ Linux (Ubuntu 20.04+, CentOS 7+)
- ✅ macOS (10.15+)
- ✅ Windows 10/11 + WSL 2 hoặc Docker Desktop

---

## Bước 1: Chuẩn Bị Môi Trường

### 1.1 Kiểm Tra Docker

```bash
# Ubuntu/Mac
docker --version
# Output: Docker version 24.0.0 (hoặc cao hơn)

docker-compose --version
# Output: Docker Compose version 2.20.0 (hoặc cao hơn)
```

**Nếu chưa cài Docker:**

**Linux (Ubuntu):**
```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose
sudo usermod -aG docker $USER
newgrp docker  # Activate group
```

**macOS:**
```bash
# Download Docker Desktop từ https://www.docker.com/products/docker-desktop
# Hoặc dùng Homebrew
brew install docker docker-compose
```

**Windows:**
- Download [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop)
- Cài đặt với WSL 2 backend
- Restart máy nếu cần

**Verify Docker installation:**
```bash
docker run hello-world
# Nên thấy "Hello from Docker!" message
```

### 1.2 Kiểm Tra Python

```bash
python --version
# Output: Python 3.9.x, 3.10.x, hoặc 3.11.x

python -m venv --help
# Xác nhận venv module sẵn có
```

**Nếu chưa cài Python:**

**Linux:**
```bash
sudo apt-get install -y python3.11 python3.11-venv python3-pip
```

**macOS:**
```bash
brew install python@3.11
```

**Windows:**
- Download từ [python.org](https://www.python.org/downloads/)
- Verify: `python --version`

### 1.3 Clone Repository

```bash
cd /path/to/workspace
git clone https://github.com/your-org/crypto-realtime-pipeline.git
cd crypto-realtime-pipeline
```

---

## Bước 2: Chuẩn Bị Python Environment

### 2.1 Tạo Virtual Environment

```bash
# Linux/Mac
python -m venv venv
source venv/bin/activate
# Output: (venv) user@machine:~/project$

# Windows (PowerShell)
python -m venv venv
.\venv\Scripts\Activate.ps1
# Output: (venv) PS C:\path\to\project>

# Windows (Command Prompt)
python -m venv venv
venv\Scripts\activate.bat
```

### 2.2 Upgrade pip
```bash
pip install --upgrade pip setuptools wheel
# Expected output: Successfully installed pip-23.x.x ...
```

### 2.3 Cài Dependencies

```bash
pip install -r requirements.txt
# Cài đặt các package: kafka-python, websocket-client, fastapi, etc.
# Expected: Successfully installed [list of packages]
```

**Verify installation:**
```bash
python -c "import kafka; import websocket; print('✓ Dependencies installed')"
# Output: ✓ Dependencies installed
```

### 2.4 Tạo .env File

```bash
# Copy template
cp .env.example .env

# Edit .env với thông số cụ thể
# Nội dung mẫu:
# KAFKA_BROKER=localhost:9092
# KAFKA_TOPIC=binance_trades
# LOG_LEVEL=INFO
```

---

## Bước 3: Khởi Động Infrastructure

### 3.1 Khởi Động Docker Services

```bash
# Check Docker daemon running
docker ps  # Nếu thành công, không lỗi

# Create and start containers
docker-compose up -d
# Output:
# [+] Running 5/5
#  ✔ Container zookeeper  Started
#  ✔ Container kafka  Started
#  ✔ Container namenode  Started
#  ✔ Container datanode  Started
#  ✔ Container spark-master  Started
```

### 3.2 Kiểm Tra Containers

```bash
docker ps

# Output nên hiển thị 5 containers:
# CONTAINER ID  IMAGE                    STATUS          PORTS
# abc123        confluentinc/cp-kafka    Up 10 seconds   9092/tcp
# def456        confluentinc/cp-zookeeper Up 10 seconds   2181/tcp
# ...etc
```

### 3.3 Kiểm Tra Kết Nối Kafka

```bash
# Cách 1: Docker logs
docker logs kafka | grep "started"
# Output: [Kafka Server] started on port: 9092

# Cách 2: Telnet
telnet localhost 9092
# Expected: Kết nối thành công (không báo lỗi)
```

### 3.4 Tạo Kafka Topics

```bash
# Option 1: Dùng script shell
bash setup_kafka_topics.sh

# Output:
# Creating partitions for topic binance_trades
# Topic binance_trades created
# Topic configuration verified

# Option 2: Manual command
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic binance_trades \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --config compression.type=snappy
```

### 3.5 Verify Kafka Topics

```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Output nên chứa:
# binance_trades
```

---

## Bước 4: Khởi Động Producer

### 4.1 Khởi Động WebSocket Producer

```bash
# Activate venv nếu chưa
source venv/bin/activate  # Linux/Mac
# hoặc
.\venv\Scripts\Activate.ps1  # Windows PowerShell

# Chạy producer
python binance_to_kafka.py

# Expected output:
# [INFO] Connecting to Binance WebSocket...
# [INFO] Connected successfully
# [INFO] Listening for trade events...
# [INFO] Received trade: BTCUSDT, price=45123.45, qty=0.5
# [INFO] Message sent to Kafka (offset=0)
```

**Xác nhận mọi 10 giây:**
```
[INFO] Producer Statistics:
  Messages sent: 150
  Errors: 0
  Throughput: 15 msg/sec
```

### 4.2 Troubleshooting Producer

| Lỗi | Nguyên Nhân | Giải Pháp |
|-----|-----------|----------|
| `Connection refused: 9092` | Kafka không chạy | Kiểm tra `docker ps` |
| `No module named 'kafka'` | Dependencies chưa cài | Chạy `pip install -r requirements.txt` |
| `WebSocket connection timeout` | Network issue | Kiểm tra internet connection |
| `Topic does not exist` | Topic chưa tạo | Chạy `bash setup_kafka_topics.sh` |

---

## Bước 5: Khởi Động Consumer

### 5.1 Khởi Động Verification Consumer

Mở terminal mới (giữ producer chạy):

```bash
source venv/bin/activate  # Linux/Mac

python kafka_consumer_verification.py

# Expected output:
# [INFO] Connecting to Kafka broker: localhost:9092
# [INFO] Connected to consumer group: verification-consumer-group
# [INFO] Subscribed to topic: binance_trades
# [INFO] Waiting for messages...
# Consumer Statistics (every 10 seconds):
#   Messages received: 100
#   Messages processed: 100
#   Messages errors: 0
#   Success rate: 100.0%
```

### 5.2 Kiểm Tra Dữ Liệu

**Terminal 3:** Consumer logs

```bash
# Trực tiếp đọc messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic binance_trades \
  --from-beginning \
  --max-messages 5

# Output:
# {"symbol":"BTCUSDT","trade_id":123456,"price":"45123.45",...}
# {"symbol":"BTCUSDT","trade_id":123457,"price":"45125.00",...}
```

---

## Bước 6: End-to-End Verification

### 6.1 Checklist Kiểm Tra

- [ ] Docker containers chạy (5 containers)
- [ ] Kafka broker accessible (`telnet localhost 9092`)
- [ ] Topics created (`kafka-topics --list`)
- [ ] Producer connected to WebSocket (messages received)
- [ ] Messages in Kafka topic (console-consumer shows data)
- [ ] Consumer processing successfully (statistics show 100% success)
- [ ] Log files created (`binance_producer.log`, `kafka_consumer_verification.log`)

### 6.2 Performance Baseline

```
Producer Performance:
  Throughput: 20-40 trades/sec ✓
  Error rate: 0% ✓
  Latency: <100ms ✓

Consumer Performance:
  Processing latency: <1 sec ✓
  Success rate: 100% ✓
  Validation errors: 0 ✓
```

### 6.3 Log Files Location

```bash
# Producer log
cat binance_producer.log

# Consumer log
cat kafka_consumer_verification.log
```

---

## Troubleshooting Chung

### Docker Issues

**Problem:** `docker: permission denied while trying to connect to the Docker daemon`

**Solution:**
```bash
# Linux
sudo usermod -aG docker $USER
newgrp docker
# hoặc
sudo chmod 666 /var/run/docker.sock
```

**Problem:** Docker services không start

**Solution:**
```bash
# Check Docker daemon
sudo systemctl status docker

# Start if not running
sudo systemctl start docker

# Rebuild containers
docker-compose down
docker-compose up -d
```

### Python Issues

**Problem:** `ModuleNotFoundError: No module named 'kafka'`

**Solution:**
```bash
# Ensure venv activated
source venv/bin/activate

# Reinstall requirements
pip install -r requirements.txt --force-reinstall
```

**Problem:** `Python version incompatible`

**Solution:**
```bash
# Check Python version
python --version  # Must be 3.9, 3.10, or 3.11

# If not, install correct version and create new venv
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Network Issues

**Problem:** `Connection refused` to Kafka

**Solution:**
```bash
# Verify Docker network
docker network ls | grep bigdata

# Check Kafka container
docker logs kafka | tail -20

# Restart Kafka
docker-compose restart kafka
sleep 5
```

**Problem:** Binance WebSocket timeout

**Solution:**
```bash
# Check internet connection
ping api.binance.com

# Check firewall
# Allow outbound connections to wss://stream.binance.com:9443

# Test WebSocket directly
python -c "
import websocket
ws = websocket.WebSocket()
ws.connect('wss://stream.binance.com:9443/ws/btcusdt@trade')
print('Connected successfully')
"
```

### Performance Issues

**Problem:** High latency (>500ms)

**Solution:**
```yaml
# Check:
1. CPU usage: top, htop, Task Manager
2. Memory: free -h, docker stats
3. Disk I/O: iostat, iotop
4. Network: netstat, iftop

Optimization:
- Increase Docker memory: docker-compose.yml
- Adjust Kafka broker heap: KAFKA_HEAP_OPTS
- Reduce consumer fetch.min.bytes
```

---

## Advanced Setup (Optional)

### GPU Support (NVIDIA)

```yaml
# docker-compose.yml addition
kafka:
  runtime: nvidia
  environment:
    - NVIDIA_VISIBLE_DEVICES=all
    - NVIDIA_DRIVER_CAPABILITIES=compute,utility
```

### Kubernetes Deployment

```bash
# Install kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"

# Create namespace
kubectl create namespace crypto-pipeline

# Deploy using Helm (future)
helm install crypto-pipeline ./k8s/helm
```

---

## Troubleshooting Quick Reference

| Symptom | Diagnosis | Fix |
|---------|-----------|-----|
| Containers don't start | `docker-compose logs` | `docker-compose down -v && docker-compose up -d` |
| No messages in Kafka | Check producer is running | `python binance_to_kafka.py` |
| Consumer lag | High processing time | Increase consumer instances |
| High memory usage | Monitor `docker stats` | Reduce batch size or add memory |
| Network timeout | Firewall blocking | Allow port 9092, 2181, 443 |

---

## Uninstall/Reset

### Dọn Dẹp Toàn Bộ

```bash
# Stop containers
docker-compose down

# Remove volumes (delete data)
docker-compose down -v

# Remove images
docker-compose down --rmi all

# Deactivate venv
deactivate

# Remove venv
rm -rf venv  # Linux/Mac
rmdir /s venv  # Windows
```

---

**Phiên Bản:** 2.0  
**Cập Nhật:** April 7, 2026  
**Hỗ Trợ:** Xem TROUBLESHOOTING.md để có thêm chi tiết
