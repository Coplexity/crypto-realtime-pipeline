# 🌐 Backend Service

Backend service của hệ thống Crypto Analytics Platform được xây dựng bằng **FastAPI** và đóng vai trò là API Gateway trung tâm cho toàn bộ hệ thống.

Service này chịu trách nhiệm:

- Cung cấp REST APIs
- WebSocket realtime streaming
- Đồng bộ dữ liệu từ Kafka / Redis / MongoDB
- Phân phối dữ liệu realtime đến Frontend
- Quản lý ML Predictions
- Streaming market data

---

# 🏗️ Kiến trúc Backend

```text
Frontend Dashboard
        ↓
FastAPI Backend
        ↓
Redis / MongoDB
        ↓
Kafka / Spark Streaming
```

---

# ⚙️ Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Framework | FastAPI |
| Language | Python |
| API | REST + WebSocket |
| Database | MongoDB |
| Cache | Redis |
| Streaming | Kafka |
| ASGI Server | Uvicorn |

---

# 📂 Cấu trúc thư mục

```text
backend/
├── Dockerfile
├── __init__.py
├── config.py
├── kafka_manager.py
├── main.py
├── requirements.txt
├── schemas.py
├── sync_predictions.py
├── test_api.py
└── ticker_updater.py
```

---

# 📦 Mô tả các file chính

| File | Vai trò |
|---|---|
| main.py | Entrypoint của FastAPI |
| config.py | Quản lý cấu hình hệ thống |
| kafka_manager.py | Kết nối và quản lý Kafka |
| ticker_updater.py | Cập nhật market ticker realtime |
| sync_predictions.py | Đồng bộ dữ liệu prediction |
| schemas.py | Định nghĩa request/response schemas |
| test_api.py | Test APIs |

---

# 🚀 Chức năng chính

## 📡 Realtime WebSocket Streaming

Backend hỗ trợ WebSocket để gửi dữ liệu realtime tới frontend:

- Trades stream
- Orderbook stream
- Ticker updates
- ML predictions

---

## 📊 REST APIs

Backend cung cấp các APIs cho:

- Market data
- Historical candles
- Predictions
- Pipeline status

---

## 🤖 ML Prediction Synchronization

Backend đồng bộ dữ liệu prediction từ:

```text
Spark Batch
    ↓
MongoDB / Redis
    ↓
FastAPI
    ↓
Frontend Dashboard
```

---

# 🔄 Data Flow

## Realtime Pipeline

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
Frontend
```

---

## Prediction Pipeline

```text
Spark MLlib
    ↓
Prediction Results
    ↓
MongoDB
    ↓
FastAPI APIs
    ↓
Frontend Dashboard
```

---

# 🔌 WebSocket Architecture

Backend sử dụng WebSocket để giảm độ trễ realtime.

## Luồng hoạt động

```text
Frontend WebSocket Client
        ↓
FastAPI WebSocket Endpoint
        ↓
Realtime Data Dispatcher
        ↓
Redis / Kafka Stream
```

---

# 📡 API Endpoints

## REST APIs

| Endpoint | Method | Mô tả |
|---|---|---|
| `/` | GET | Health check |
| `/api/tickers` | GET | Lấy market tickers |
| `/api/predictions` | GET | Lấy ML predictions |
| `/api/market-stats` | GET | Thống kê thị trường |

---

## WebSocket APIs

| Endpoint | Chức năng |
|---|---|
| `/ws/trades` | Stream realtime trades |
| `/ws/orderbook` | Stream orderbook |
| `/ws/tickers` | Stream ticker updates |
| `/ws/predictions` | Stream ML predictions |

---

# 🧠 Tích hợp với Spark Streaming

Backend nhận dữ liệu realtime từ:

- Redis cache
- Kafka stream
- MongoDB historical collections

Spark Streaming sẽ:

```text
Kafka
    ↓
Spark Streaming
    ↓
Redis + MongoDB
```

Backend sau đó sẽ stream dữ liệu tới frontend thông qua WebSocket.

---

# 🗄️ MongoDB Integration

MongoDB được sử dụng để lưu:

- Historical candles
- Aggregated OHLC data
- Predictions
- Market history

---

# ⚡ Redis Integration

Redis được sử dụng làm:

- Realtime cache
- Temporary streaming buffer
- Fast access layer
- Speed layer trong Lambda Architecture

---

# 🐳 Docker Deployment

## Build Docker Image

```bash
docker build -t crypto-backend:v1 .
```

---

## Run Container

```bash
docker run -p 8000:8000 crypto-backend:v1
```

---

# ☸️ Kubernetes Deployment

Backend được triển khai trên Kubernetes thông qua:

```text
k8s/orchestration/backend.yaml
```

---

## Deployment Architecture

```text
Kubernetes Deployment
        ↓
FastAPI Pods
        ↓
ClusterIP Service
        ↓
Ingress / Port Forward
```

---

# 🚀 Chạy local development

## Cài dependencies

```bash
pip install -r requirements.txt
```

---

## Chạy FastAPI

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

---

# 🔍 Kiểm tra API

## Swagger UI

Sau khi chạy server:

```text
http://localhost:8000/docs
```

---

## ReDoc

```text
http://localhost:8000/redoc
```

---

# 📈 Monitoring

Backend hỗ trợ:

- API monitoring
- WebSocket monitoring
- Kafka connectivity
- Redis connectivity
- MongoDB health checking

---

# 🧪 Testing

## Chạy test API

```bash
python test_api.py
```

---

# 🔒 Cấu hình Environment Variables

Ví dụ:

```env
KAFKA_BROKER=kafka:9092
MONGO_URI=mongodb://mongodb:27017
REDIS_HOST=redis
REDIS_PORT=6379
```

---

# 📚 Vai trò trong Lambda Architecture

Backend đóng vai trò:

- API Gateway
- Realtime Data Distributor
- Prediction Provider
- Frontend Communication Layer

---

# ⚠️ Lưu ý

- Backend phụ thuộc Redis và MongoDB
- WebSocket yêu cầu frontend duy trì kết nối liên tục
- Prediction data được đồng bộ định kỳ từ Spark Batch pipeline

---

# 🔮 Hướng phát triển

- Authentication & Authorization
- Rate limiting
- API Gateway scaling
- Kafka consumer optimization
- Distributed WebSocket management
- gRPC communication
- Metrics exporter cho Prometheus