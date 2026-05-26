# ⚙️ Microservices Overview

Thư mục `services/` chứa toàn bộ các microservices cốt lõi của hệ thống Crypto Analytics Platform.

Mỗi service được thiết kế độc lập theo mô hình Microservices Architecture và được container hóa bằng Docker để triển khai trên Kubernetes.

---

# 🏗️ Kiến trúc tổng quan

```text
services/
├── airflow/           # Workflow orchestration
├── backend/           # FastAPI REST & WebSocket API
├── data-ingestion/    # Binance -> Kafka producer
├── frontend/          # Next.js realtime dashboard
├── spark-batch/       # Batch processing & ML pipeline
└── spark-streaming/   # Realtime streaming pipeline
```

---

# 🔄 Data Flow giữa các Services

```text
Binance Exchange
        ↓
data-ingestion
        ↓
Apache Kafka
        ↓
spark-streaming
        ↓
MongoDB + Redis
        ↓
backend
        ↓
frontend
```

---

# 📦 Các Microservices

---

# 1. 📡 data-ingestion

Thư mục:

```text
services/data-ingestion/
```

## Chức năng

Service chịu trách nhiệm:

- Kết nối Binance WebSocket API
- Thu thập dữ liệu market realtime
- Publish dữ liệu vào Kafka topics

## Công nghệ

- Python
- WebSocket
- Kafka Producer

## Data Flow

```text
Binance WebSocket
        ↓
Kafka Topics
```

## Thành phần chính

| File | Vai trò |
|---|---|
| producer.py | Streaming dữ liệu Binance |
| Dockerfile | Docker image definition |

---

# 2. ⚡ spark-streaming

Thư mục:

```text
services/spark-streaming/
```

## Chức năng

Xử lý realtime streaming pipeline bằng Spark Structured Streaming.

Service thực hiện:

- Consume dữ liệu từ Kafka
- ETL realtime
- Tính toán market statistics
- Ghi dữ liệu realtime vào Redis
- Lưu historical data vào MongoDB

---

## Công nghệ

- Apache Spark Structured Streaming
- PySpark
- Kafka Consumer

---

## Data Flow

```text
Kafka
    ↓
Spark Structured Streaming
    ↓
Redis + MongoDB
```

---

## Thành phần chính

| File | Vai trò |
|---|---|
| streaming_job.py | Realtime ETL pipeline |
| submit.sh | Spark submit script |
| requirements.txt | Python dependencies |

---

# 3. 🧠 spark-batch

Thư mục:

```text
services/spark-batch/
```

## Chức năng

Batch processing và Machine Learning pipeline.

Service đảm nhiệm:

- OHLC aggregation
- Feature engineering
- ML training
- Price prediction
- Historical analytics

---

## Công nghệ

- Apache Spark
- Spark MLlib
- PySpark

---

## Các Job chính

| File | Chức năng |
|---|---|
| ohlc_5m_aggregator.py | Aggregate dữ liệu 5 phút |
| ohlc_1h_aggregator.py | Aggregate dữ liệu 1 giờ |
| ohlc_4h_aggregator.py | Aggregate dữ liệu 4 giờ |
| ohlc_1d_aggregator.py | Aggregate dữ liệu 1 ngày |
| train_price_prediction.py | Train ML model |
| predict_price.py | Dự đoán giá |

---

## ML Pipeline

```text
MongoDB Historical Data
        ↓
Feature Engineering
        ↓
Model Training
        ↓
Prediction
        ↓
MongoDB / Redis
```

---

# 4. 🔁 airflow

Thư mục:

```text
services/airflow/
```

## Chức năng

Điều phối toàn bộ workflow của hệ thống.

Airflow thực hiện:

- Scheduling Spark jobs
- Trigger ML pipelines
- Batch orchestration
- Automation workflow

---

## Công nghệ

- Apache Airflow
- KubernetesPodOperator

---

## Các DAG chính

| DAG | Chức năng |
|---|---|
| ml_prediction_dag.py | ML prediction scheduling |
| ohlc_spark_aggregator.py | OHLC aggregation scheduling |
| maintenance_dag.py | Maintenance workflow |

---

## Workflow

```text
Airflow Scheduler
        ↓
Spark Batch Jobs
        ↓
ML Pipeline
```

---

# 5. 🌐 backend

Thư mục:

```text
services/backend/
```

## Chức năng

Backend API gateway của hệ thống.

Service cung cấp:

- REST API
- WebSocket realtime streaming
- Prediction APIs
- Redis/Mongo synchronization

---

## Công nghệ

- FastAPI
- WebSockets
- Redis
- MongoDB

---

## API Features

- Market data API
- Prediction API
- Realtime WebSocket
- Orderbook streaming
- Trade streaming

---

## Thành phần chính

| File | Vai trò |
|---|---|
| main.py | FastAPI entrypoint |
| kafka_manager.py | Kafka integration |
| ticker_updater.py | Market data updater |
| sync_predictions.py | Prediction synchronization |
| schemas.py | API schemas |

---

# 6. 📊 frontend

Thư mục:

```text
services/frontend/
```

## Chức năng

Dashboard realtime hiển thị dữ liệu cryptocurrency.

---

## Công nghệ

- Next.js
- React
- TypeScript
- WebSocket

---

## Tính năng Dashboard

- Candlestick charts
- Orderbook visualization
- Trade feeds
- Top gainers / losers
- ML predictions
- Pipeline monitoring

---

## Components chính

| Component | Chức năng |
|---|---|
| CandleChart.tsx | Candlestick chart |
| OrderBook.tsx | Orderbook realtime |
| TradeList.tsx | Trade streaming |
| TopGainers.tsx | Top market movers |
| MLPredictions.tsx | ML predictions |
| PipelineStatus.tsx | Pipeline health |

---

# 🐳 Docker Architecture

Mỗi service đều được container hóa riêng biệt bằng Docker.

## Dockerfiles

| Service | Dockerfile |
|---|---|
| data-ingestion | Dockerfile |
| spark-streaming | Dockerfile |
| spark-batch | Dockerfile |
| airflow | Dockerfile |
| backend | Dockerfile |
| frontend | Dockerfile |

---

# ☸️ Kubernetes Deployment

Toàn bộ services được triển khai trên Kubernetes thông qua:

```text
k8s/
```

Các deployment bao gồm:

- Deployments
- StatefulSets
- ConfigMaps
- Secrets
- Persistent Volumes
- Services
- Ingress

---

# 📈 Monitoring & Logging

Hệ thống hỗ trợ:

- Prometheus metrics
- Grafana dashboards
- Spark logs
- Kafka monitoring
- Kubernetes Dashboard

---

# 🚀 Khởi chạy hệ thống

Xem hướng dẫn đầy đủ tại:

```text
reproduce.md
```

---

# 📚 Ghi chú

- Các services hoạt động độc lập theo mô hình distributed system
- Toàn bộ pipeline hỗ trợ realtime processing
- Hệ thống được thiết kế theo Lambda Architecture
- Hỗ trợ mở rộng theo chiều ngang (horizontal scaling)