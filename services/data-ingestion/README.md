# 📡 Data Ingestion Service

Data Ingestion service là thành phần đầu vào của hệ thống Crypto Analytics Platform.

Service chịu trách nhiệm:

- Kết nối Binance WebSocket API
- Thu thập dữ liệu cryptocurrency realtime
- Streaming market events
- Publish dữ liệu vào Kafka
- Cung cấp realtime source cho Spark Streaming

Đây là thành phần đầu tiên trong pipeline của kiến trúc Lambda Architecture.

---

# 🏗️ Kiến trúc Data Ingestion

```text
Binance WebSocket API
        ↓
Data Ingestion Service
        ↓
Apache Kafka
        ↓
Spark Streaming
```

---

# ⚙️ Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Language | Python |
| Streaming | WebSocket |
| Message Queue | Apache Kafka |
| API Source | Binance WebSocket API |
| Deployment | Docker + Kubernetes |

---

# 📂 Cấu trúc thư mục

```text
data-ingestion/
├── Dockerfile
├── producer.py
└── requirements.txt
```

---

# 📦 Mô tả các file chính

| File | Vai trò |
|---|---|
| producer.py | Streaming dữ liệu Binance và publish vào Kafka |
| Dockerfile | Docker image definition |
| requirements.txt | Python dependencies |

---

# 🚀 Chức năng chính

## 📡 Kết nối Binance WebSocket

Service kết nối tới Binance để nhận:

- Trades stream
- Market tickers
- Price updates
- Orderbook updates
- Volume statistics

---

## 🔄 Realtime Data Streaming

Hệ thống nhận dữ liệu realtime liên tục từ Binance:

```text
Binance Exchange
        ↓
Realtime WebSocket Stream
        ↓
JSON Market Events
```

---

## 📨 Publish vào Kafka

Sau khi nhận dữ liệu:

- Parse market events
- Chuẩn hóa dữ liệu
- Publish vào Kafka topics

---

# 📊 Data Flow

```text
Binance WebSocket
        ↓
Python Producer
        ↓
Kafka Producer
        ↓
Kafka Topics
        ↓
Spark Streaming
```

---

# 📡 Binance Integration

Service sử dụng Binance WebSocket API để:

- Subscribe market streams
- Nhận realtime updates
- Stream dữ liệu low-latency

---

## Dữ liệu thu thập

| Data Type | Nội dung |
|---|---|
| Trades | Realtime trades |
| Tickers | Market ticker updates |
| Orderbook | Bid / Ask updates |
| Market Stats | Volume & statistics |

---

# 📨 Kafka Integration

Service hoạt động như:

```text
Kafka Producer
```

---

## Kafka Topics

Dữ liệu được publish vào các topics:

| Topic | Nội dung |
|---|---|
| trades | Trade stream |
| tickers | Market ticker data |
| orderbook | Orderbook updates |
| market-stats | Market statistics |

---

# ⚡ Low Latency Streaming

Pipeline được tối ưu cho:

- Near realtime ingestion
- Continuous streaming
- Low latency transmission
- Distributed processing

---

# 🔄 Streaming Workflow

```text
Binance API
        ↓
WebSocket Connection
        ↓
Realtime Events
        ↓
Kafka Producer
        ↓
Kafka Cluster
```

---

# 📈 Market Data Pipeline

Data ingestion là điểm bắt đầu của:

```text
Binance
    ↓
Kafka
    ↓
Spark Streaming
    ↓
Redis / MongoDB
    ↓
FastAPI Backend
    ↓
Next.js Dashboard
```

---

# 🧠 Event Processing

Service xử lý:

- JSON parsing
- Event normalization
- Message serialization
- Kafka publishing

---

# 📦 Message Format

Dữ liệu thường được serialize dưới dạng:

```json
{
  "symbol": "BTCUSDT",
  "price": 65000,
  "volume": 12.5,
  "timestamp": 1710000000
}
```

---

# 🔌 WebSocket Architecture

```text
Binance WebSocket
        ↓
Persistent Connection
        ↓
Streaming Events
        ↓
Kafka Producer
```

---

# 🐳 Docker Deployment

## Build Docker Image

```bash
docker build -t crypto-data-ingestion:v1 .
```

---

## Run Container

```bash
docker run crypto-data-ingestion:v1
```

---

# ☸️ Kubernetes Deployment

Service được triển khai thông qua:

```text
k8s/apps/data-ingestion.yaml
```

---

## Kubernetes Architecture

```text
Kubernetes Deployment
        ↓
Data Ingestion Pod
        ↓
Kafka Cluster
```

---

# 🚀 Chạy local development

## Cài dependencies

```bash
pip install -r requirements.txt
```

---

## Chạy producer

```bash
python producer.py
```

---

# 📊 Realtime Processing

Hệ thống hỗ trợ:

- Continuous streaming
- Event-driven architecture
- Distributed ingestion
- High-throughput streaming

---

# 🧪 Fault Tolerance

Service hỗ trợ:

- WebSocket reconnection
- Kafka retry mechanism
- Connection recovery
- Error handling

---

# 🔒 Environment Variables

Ví dụ:

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
BINANCE_WS_URL=wss://stream.binance.com:9443/ws
```

---

# 📚 Vai trò trong Lambda Architecture

Data Ingestion đóng vai trò:

- Data Source Layer
- Realtime Event Producer
- Streaming Gateway
- Market Data Collector

---

# 📡 Integration với các Services khác

## Output Services

| Service | Vai trò |
|---|---|
| Kafka | Message queue |
| Spark Streaming | Realtime processing |

---

# 📈 Monitoring

Hệ thống hỗ trợ monitoring:

- Kafka producer status
- WebSocket connection health
- Streaming throughput
- Event processing rate
- Connection latency

---

# ⚠️ Lưu ý

- Service phụ thuộc Binance WebSocket API
- Kafka cluster cần hoạt động ổn định
- Internet connection cần ổn định để duy trì realtime streaming
- Nếu Binance disconnect, service sẽ tự reconnect

---

# 🔮 Hướng phát triển

- Multi-exchange support
- Binance Futures integration
- Advanced retry strategy
- Distributed producers
- Event compression
- Schema Registry integration
- Kafka partition optimization
- Async processing optimization
- Stream buffering
- Multi-symbol scalability