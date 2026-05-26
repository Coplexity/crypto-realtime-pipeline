# ⚡ Spark Streaming Service

Spark Streaming service là thành phần xử lý realtime cốt lõi của hệ thống Crypto Analytics Platform.

Service sử dụng **Apache Spark Structured Streaming** để:

- Consume dữ liệu realtime từ Kafka
- Xử lý ETL streaming
- Tính toán market analytics
- Đồng bộ dữ liệu vào Redis và MongoDB
- Cung cấp dữ liệu realtime cho Dashboard

Đây là thành phần chính của **Speed Layer** trong kiến trúc Lambda Architecture.

---

# 🏗️ Kiến trúc Streaming

```text
Binance WebSocket
        ↓
Kafka Topics
        ↓
Spark Structured Streaming
        ↓
Redis + MongoDB
        ↓
FastAPI Backend
        ↓
Next.js Dashboard
```

---

# ⚙️ Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Streaming Engine | Apache Spark Structured Streaming |
| Message Queue | Apache Kafka |
| Language | Python |
| Processing | PySpark |
| Storage | MongoDB |
| Cache | Redis |
| Deployment | Docker + Kubernetes |

---

# 📂 Cấu trúc thư mục

```text
spark-streaming/
├── Dockerfile
├── requirements.txt
├── streaming_job.py
└── submit.sh
```

---

# 📦 Mô tả các file chính

| File | Vai trò |
|---|---|
| streaming_job.py | Realtime streaming pipeline |
| submit.sh | Spark submit script |
| Dockerfile | Docker image definition |
| requirements.txt | Python dependencies |

---

# 🚀 Chức năng chính

## 📡 Consume dữ liệu từ Kafka

Spark Streaming subscribe các Kafka topics để nhận:

- Trades stream
- Ticker updates
- Orderbook data
- Market statistics

---

## 🔄 Realtime ETL

Pipeline thực hiện:

- Parse JSON streaming data
- Data cleaning
- Schema validation
- Feature transformation

---

## 📊 Market Analytics

Spark Streaming tính toán realtime:

- Price updates
- Volume statistics
- Market trends
- Orderbook metrics
- Trade aggregation

---

## ⚡ Redis Speed Layer

Redis được sử dụng để:

- Cache realtime data
- Giảm độ trễ truy xuất
- Cung cấp dữ liệu realtime cho frontend

---

## 🗄️ MongoDB Historical Storage

MongoDB lưu:

- Historical trades
- Aggregated market data
- Candlestick data
- Historical analytics

---

# 🔄 Streaming Data Flow

```text
Kafka Topics
    ↓
Spark Structured Streaming
    ↓
Transformation & Aggregation
    ↓
Redis + MongoDB
    ↓
FastAPI APIs
    ↓
Frontend Dashboard
```

---

# 📡 Kafka Integration

Spark Streaming kết nối với Kafka thông qua:

```text
Kafka Consumer API
```

---

## Dữ liệu Kafka bao gồm

| Topic | Nội dung |
|---|---|
| trades | Trade stream |
| tickers | Market tickers |
| orderbook | Orderbook updates |
| predictions | ML predictions |

---

# 🧠 Structured Streaming Pipeline

Pipeline realtime gồm các bước:

```text
Kafka Source
        ↓
Spark Streaming DataFrame
        ↓
JSON Parsing
        ↓
Transformation
        ↓
Aggregation
        ↓
Redis / MongoDB Sink
```

---

# 📊 Streaming Analytics

Hệ thống hỗ trợ:

- Sliding window aggregation
- Market statistics
- Realtime computation
- Stream transformation
- Event processing

---

# ⚡ Low Latency Processing

Spark Structured Streaming giúp:

- Near realtime processing
- Fault tolerance
- Distributed execution
- Horizontal scaling

---

# 🐳 Docker Deployment

## Build Docker Image

```bash
docker build -t crypto-spark-streaming:v1 .
```

---

## Run Container

```bash
docker run crypto-spark-streaming:v1
```

---

# ☸️ Kubernetes Deployment

Spark Streaming được triển khai trên Kubernetes thông qua:

```text
k8s/compute/spark-jobs/streaming-job.yaml
```

---

## Kubernetes Architecture

```text
Kubernetes Cluster
        ↓
Spark Driver Pod
        ↓
Spark Executor Pods
        ↓
Distributed Streaming Processing
```

---

# 🚀 Chạy local development

## Cài dependencies

```bash
pip install -r requirements.txt
```

---

## Chạy Spark Streaming

```bash
python streaming_job.py
```

---

## Hoặc submit Spark Job

```bash
./submit.sh
```

---

# 📈 Spark Structured Streaming

Service sử dụng:

- Streaming DataFrames
- Stateful processing
- Event-time processing
- Window operations

---

# 📊 Window Aggregation

Hệ thống hỗ trợ aggregation theo:

- 5 phút
- 1 giờ
- 4 giờ
- 1 ngày

---

# 🧪 Fault Tolerance

Spark Streaming hỗ trợ:

- Checkpointing
- Recovery
- Fault-tolerant execution
- Exactly-once processing

---

# 🔒 Environment Variables

Ví dụ:

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
MONGO_URI=mongodb://mongodb:27017
REDIS_HOST=redis
REDIS_PORT=6379
```

---

# 📚 Vai trò trong Lambda Architecture

Spark Streaming đóng vai trò:

- Speed Layer
- Realtime Processing Engine
- Streaming ETL Pipeline
- Low-latency analytics engine

---

# 📡 Integration với các Services khác

## Input Services

| Service | Vai trò |
|---|---|
| data-ingestion | Publish dữ liệu vào Kafka |

---

## Output Services

| Service | Vai trò |
|---|---|
| backend | Realtime APIs |
| frontend | Dashboard realtime |
| spark-batch | Historical data processing |

---

# 🔍 Monitoring

Hệ thống hỗ trợ monitoring:

- Spark metrics
- Kafka throughput
- Streaming latency
- Processing rate
- Executor monitoring

---

# ⚠️ Lưu ý

- Spark Streaming phụ thuộc Kafka cluster
- Redis cần hoạt động ổn định để đảm bảo low-latency
- MongoDB lưu dữ liệu lịch sử phục vụ batch processing

---

# 🔮 Hướng phát triển

- Advanced stream aggregation
- Stateful analytics
- Event-time watermarking
- Multi-topic streaming
- Distributed checkpoint storage
- Spark Operator autoscaling
- Realtime anomaly detection
- Stream enrichment
- CEP (Complex Event Processing)