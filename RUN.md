# Hướng Dẫn Chạy Dự Án

**Scope:** Phase 2 - Data Collection (Thu Thập Dữ Liệu)

---

## Prerequisites

- Docker Desktop (20.10+)
- Python 3.9+
- Git

Verify:
```bash
docker --version
python --version
```

---

## Quick Start

### 1. Prepare Environment

```bash
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

### 2. Configuration

```bash
# Copy environment template
cp .env.example .env

# Verify content:
cat .env
```

### 3. Start Infrastructure

```bash
# Start Kafka & Zookeeper
docker compose up -d zookeeper kafka

# Verify containers
docker compose ps

# Create Kafka topic
./setup_kafka_topics.sh
```

### 4. Start Servers

**Terminal 1: Producer** (Binance WebSocket → Kafka)
```bash
.venv\Scripts\activate
python binance_to_kafka.py
```

Expected output:
```
PHASE 2: Thu thập dữ liệu
Kết nối Binance WebSocket thành công
Kafka Producer khởi tạo
Đang gửi vào Kafka...
```

**Terminal 2: Verification Consumer**
```bash
.venv\Scripts\activate
python kafka_consumer_verification.py
```

Expected output:
```
Consumer khởi tạo thành công
Đang lắng nghe Kafka Topic
[Messages displayed]
Statistics [every 10 seconds]
```

**Terminal 3: Backend** (Optional)
```bash
.venv\Scripts\activate
python -m uvicorn main:app --reload
```

Access: `http://localhost:8000/docs`

---

## Verification

### Check Kafka Topic
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
# Should show: binance_trades

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic binance_trades
# Should show: Partitions: 3, Replication: 1, Compression: snappy
```

### Check Messages
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic binance_trades --from-beginning --max-messages 5
```

Should show JSON trade messages.

### Check Logs
```bash
# Producer logs
tail -f binance_producer.log

# Verification consumer logs
tail -f kafka_consumer_verification.log
```

---

## Common Issues

| Issue | Solution |
|-------|----------|
| Connection refused | `docker compose up -d zookeeper kafka` |
| Module not found | `pip install -r requirements.txt` |
| Topic not found | Run `./setup_kafka_topics.sh` |
| No messages | Check producer logs, check Kafka container status |
| Docker memory | Increase Docker Desktop memory to 4GB+ |

---

## Cleanup

### Stop Services
```bash
docker compose down
```

### Delete Topic
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic binance_trades
```

### Full Reset
```bash
docker compose down -v
```

---

## Documentation

- **Specification:** PHASE_2_SPECIFICATION.md
- **Code:** binance_to_kafka.py, kafka_consumer_verification.py
- **Script:** setup_kafka_topics.sh

---

## Troubleshooting

### Kafka Container Issues
```bash
docker logs kafka       # View Kafka logs
docker logs zookeeper   # View Zookeeper logs
docker ps              # List running containers
```

### Producer Not Sending
```bash
# Check if producer is running
ps aux | grep binance_to_kafka
# Or check log file
cat binance_producer.log
```

### Consumer Not Receiving
```bash
# Check consumer group
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group verification-consumer-group --describe
```

---

## System Commands Reference

```bash
# Docker
docker compose up -d                              # Start containers
docker compose down                               # Stop containers
docker compose ps                                 # List containers
docker logs kafka                                 # View container logs

# Kafka
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec kafka kafka-console-producer --broker-list localhost:9092 --topic binance_trades
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic binance_trades --from-beginning

# Python
python -m venv .venv                             # Create virtual environment
source .venv/bin/activate                        # Activate (Linux/Mac)
.venv\Scripts\activate                           # Activate (Windows)
pip install -r requirements.txt                  # Install dependencies
```

---

## Typical Workflow

1. Open 3 terminals in project directory
2. Terminal 1: Start producer → Wait for "Kết nối Binance WebSocket thành công"
3. Terminal 2: Start verification consumer → Wait for messages
4. Observe messages and statistics
5. When done: Ctrl+C to stop each terminal
6. `docker compose down` to stop infrastructure

---

## Next Phase

After Phase 2 verification is complete, proceed to Phase 3: Spark Processing

See: PHASE_2_SPECIFICATION.md for details
