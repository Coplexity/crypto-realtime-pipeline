# Crypto Analytics System Deployment Guide (Lambda Architecture)

## Tổng quan hệ thống

Tài liệu này mô tả chi tiết cách triển khai hệ thống phân tích dữ liệu tiền điện tử theo kiến trúc Lambda Architecture trên môi trường Kubernetes sử dụng Minikube.

Hệ thống bao gồm các thành phần:

- Apache Kafka
- Apache Spark Streaming
- Apache Spark MLlib
- Apache Airflow
- MongoDB
- Redis
- FastAPI
- Next.js
- Kubernetes (Minikube)

Hệ thống hỗ trợ:

- Streaming dữ liệu crypto realtime
- Batch processing
- Machine Learning prediction
- Dashboard realtime
- WebSocket realtime update
- Distributed data pipeline

---

# 1. Chuẩn bị môi trường

## 1.1. Cài đặt các công cụ cần thiết

Cần cài đặt các công cụ sau:

- Docker
- kubectl
- Minikube
- Git

Kiểm tra phiên bản:

```bash
docker --version
kubectl version --client
minikube version
```

Ví dụ output:

```bash
Docker version 26.x.x
Client Version: v1.30.x
minikube version: v1.35.x
```

---

## 1.2. Khởi động Minikube

Khởi tạo Kubernetes cluster với đủ tài nguyên cho Spark, Kafka và Airflow.

```bash
minikube start --cpus=6 --memory=12288
```

Giải thích:

| Tham số | Ý nghĩa |
|---|---|
| `--cpus=6` | Cấp 6 CPU |
| `--memory=12288` | Cấp 12GB RAM |

Kiểm tra cluster:

```bash
kubectl get nodes
```

---

# 2. Khởi tạo Infrastructure

Toàn bộ hệ thống chạy trong namespace:

```text
crypto-system
```

---

## 2.1. Tạo Namespace

```bash
kubectl create namespace crypto-system
```

Kiểm tra:

```bash
kubectl get namespaces
```

---

## 2.2. Triển khai Kafka, Redis, MongoDB

Áp dụng toàn bộ manifest infrastructure:

```bash
kubectl apply -f k8s/infra/ -n crypto-system
```

Thư mục `k8s/infra/` thường chứa:

```text
k8s/infra/
├── kafka.yaml
├── zookeeper.yaml
├── redis.yaml
├── mongodb.yaml
└── postgres.yaml
```

---

## 2.3. Kiểm tra pod

```bash
kubectl get pods -n crypto-system
```

Kết quả mong muốn:

```text
kafka-0                 Running
zookeeper-0             Running
mongodb-0               Running
redis-xxxxx             Running
postgres-0              Running
```

---

# 3. Data Ingestion Pipeline

## 3.1. Data Ingestion Service

Service này có nhiệm vụ:

- Gọi Binance API
- Lấy dữ liệu market realtime
- Publish vào Kafka Topics

Deploy:

```bash
kubectl apply -f k8s/apps/data-ingestion.yaml -n crypto-system
```

Kiểm tra pod:

```bash
kubectl get pods -n crypto-system
```

Xem logs:

```bash
kubectl logs <pod-name> -n crypto-system
```

Ví dụ:

```bash
kubectl logs data-ingestion-xxxxx -n crypto-system
```

Nếu hoạt động đúng sẽ thấy:

```text
Connected to Binance WebSocket
Publishing BTCUSDT ticker to Kafka
```

---

# 4. Spark Streaming Realtime Layer

## 4.1. Mục tiêu

Spark Streaming sẽ:

- Consume dữ liệu từ Kafka
- Xử lý realtime
- Tính toán:
  - OHLC
  - Moving Average
  - Volume
  - Top Gainers
  - Top Losers
- Ghi kết quả vào:
  - Redis
  - MongoDB

---

## 4.2. Deploy Spark Streaming

```bash
kubectl apply -f k8s/apps/crypto-spark-streaming.yaml -n crypto-system
```

Kiểm tra:

```bash
kubectl get pods -n crypto-system
```

Xem logs Spark:

```bash
kubectl logs <spark-streaming-pod> -n crypto-system
```

Nếu thành công sẽ thấy:

```text
Connected to Kafka
Processing micro batch
Writing to Redis
Writing to MongoDB
```

---

# 5. Batch Layer & Machine Learning

## 5.1. Build Spark Batch Image

Spark MLlib dùng cho:

- Huấn luyện mô hình
- Price prediction
- Trend prediction

Sử dụng Docker environment của Minikube:

```bash
eval $(minikube docker-env)
```

Build image:

```bash
docker build -t crypto-spark-batch:v1 ./services/spark-batch
```

Lưu ý:

Tên image PHẢI đúng:

```text
crypto-spark-batch:v1
```

vì Airflow DAG sử dụng chính tên này.

---

# 6. Triển khai Airflow

## 6.1. Deploy Airflow

```bash
kubectl apply -f k8s/apps/airflow.yaml -n crypto-system
```

Airflow gồm:

- Scheduler
- Webserver

---

## 6.2. Khởi tạo Database Airflow

Nếu Airflow chưa migrate DB:

```bash
kubectl exec -it deploy/airflow-webserver -n crypto-system -- airflow db migrate
```

Tạo tài khoản admin:

```bash
kubectl exec -it deploy/airflow-webserver -n crypto-system -- airflow users create \
-u admin \
-p admin \
-f Admin \
-l User \
-r Admin \
-e admin@example.com
```

---

# 7. Build Backend & Frontend

## 7.1. Build Backend FastAPI

```bash
eval $(minikube docker-env)

docker build -t crypto-backend:v1 ./services/backend
```

---

## 7.2. Build Frontend Next.js

```bash
docker build -t crypto-frontend:v1 ./services/frontend
```

---

# 8. Deploy Backend & Frontend

## 8.1. Deploy Backend

```bash
kubectl apply -f k8s/apps/fastapi-backend.yaml -n crypto-system
```

---

## 8.2. Deploy Frontend

```bash
kubectl apply -f k8s/apps/nextjs-frontend.yaml -n crypto-system
```

---

# 9. Port Forwarding

Mở terminal riêng cho từng service.

---

## 9.1. Frontend Dashboard

```bash
kubectl port-forward svc/nextjs-frontend 3000:3000 -n crypto-system
```

Dashboard:

```text
http://localhost:3000
```

---

## 9.2. Backend API

```bash
kubectl port-forward svc/fastapi-backend 8000:8000 -n crypto-system
```

API:

```text
http://localhost:8000
```

---

## 9.3. Airflow UI

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n crypto-system
```

Airflow UI:

```text
http://localhost:8080
```

Tài khoản:

```text
admin / admin
```

---

## 9.4. MongoDB

```bash
kubectl port-forward svc/mongodb 27017:27017 -n crypto-system
```

Dùng MongoDB Compass để kiểm tra dữ liệu.

---

# 10. Kích hoạt ML Pipeline

Do Kubernetes có tính chất stateless nên mỗi lần restart cluster cần chạy lại pipeline ML.

---

## 10.1. Mở Airflow Dashboard

```text
http://localhost:8080
```

---

## 10.2. Unpause DAGs

Bật:

- `crypto_ml_training`
- `crypto_ml_prediction`

---

## 10.3. Trigger Training DAG

Chạy:

```text
crypto_ml_training
```

Mục tiêu:

- Load dữ liệu từ MongoDB
- Train model bằng Spark MLlib
- Sinh file:
  
```text
model.json
```

- Lưu vào PVC

---

## 10.4. Prediction DAG

DAG:

```text
crypto_ml_prediction
```

sẽ:

- Chạy mỗi 5 phút
- Sinh dự đoán giá
- Ghi kết quả vào MongoDB/Redis

---

# 11. Kiểm tra hệ thống

Truy cập:

```text
http://localhost:3000
```

Hệ thống hoạt động đúng khi:

- WebSocket Connected
- Candlestick realtime
- Orderbook realtime
- Gainers/Losers realtime
- ML Prediction hiển thị dữ liệu

---

# 12. Kiến trúc Lambda Architecture

## 12.1. Speed Layer

```text
Binance API
    ↓
Kafka
    ↓
Spark Streaming
    ↓
Redis + MongoDB
    ↓
FastAPI
    ↓
Next.js Dashboard
```

---

## 12.2. Batch Layer

```text
MongoDB Historical Data
    ↓
Airflow
    ↓
Spark MLlib
    ↓
Model Training
    ↓
Prediction
    ↓
MongoDB / Redis
```

---

# 13. Thành phần công nghệ

| Thành phần | Công nghệ |
|---|---|
| Containerization | Docker |
| Orchestration | Kubernetes |
| Streaming | Kafka |
| Stream Processing | Spark Streaming |
| Batch Processing | Spark MLlib |
| Workflow Scheduler | Airflow |
| Database | MongoDB |
| Cache | Redis |
| Backend | FastAPI |
| Frontend | Next.js |

---

# 14. Debug & Troubleshooting

## Xem pods

```bash
kubectl get pods -n crypto-system
```

---

## Xem logs

```bash
kubectl logs <pod-name> -n crypto-system
```

---

## Restart deployment

```bash
kubectl rollout restart deployment <deployment-name> -n crypto-system
```

Ví dụ:

```bash
kubectl rollout restart deployment airflow-webserver -n crypto-system
```

---

# 15. Kết luận

Hệ thống Crypto Analytics theo kiến trúc Lambda cho phép:

- Phân tích dữ liệu realtime
- Machine Learning prediction
- Scalable streaming system
- Dashboard realtime
- Distributed data processing

Đây là mô hình phù hợp cho:

- Big Data Systems
- Financial Analytics
- Realtime Processing
- Machine Learning Pipelines
- Distributed Systems
- Data Engineering