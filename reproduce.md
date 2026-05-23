# Hướng dẫn triển khai hệ thống Crypto Analytics (Lambda Architecture)

Tài liệu này hướng dẫn cách triển khai toàn bộ hệ thống phân tích dữ liệu tiền điện tử theo kiến trúc Lambda Architecture trên môi trường Kubernetes sử dụng Minikube.

Hệ thống bao gồm:

- Apache Kafka
- Apache Spark Streaming
- Apache Spark MLlib
- Apache Airflow
- MongoDB
- Redis
- FastAPI
- Next.js
- Prometheus + Grafana
- Kubernetes (Minikube)

Kiến trúc hệ thống hỗ trợ:

- Streaming dữ liệu realtime từ Binance
- Realtime analytics
- Batch Machine Learning pipeline
- Dashboard realtime
- WebSocket live updates
- Monitoring & Observability
- Distributed microservices deployment

---

# 1. Chuẩn bị môi trường

## Yêu cầu hệ thống

Cần cài đặt sẵn:

- Docker (Docker Desktop hoặc Docker Engine)
- kubectl
- Minikube
- Git

Kiểm tra phiên bản:

```bash
docker --version
kubectl version --client
minikube version
```

---

## Khởi động Minikube

Do hệ thống Big Data sử dụng Spark, Kafka và Airflow nên cần cấp tài nguyên đủ lớn cho Kubernetes Cluster.

Khởi động Minikube:

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

Nếu trạng thái là:

```text
Ready
```

nghĩa là cluster đã hoạt động thành công.

---

# 2. Build Docker Images cho Microservices

Toàn bộ mã nguồn được tách biệt theo mô hình Microservices trong thư mục:

```text
services/
```

Do hệ thống chạy trên Minikube local nên cần build image trực tiếp vào Docker daemon của Minikube để Kubernetes có thể pull image nội bộ mà không cần đẩy lên Docker Hub.

---

## 2.1. Trỏ Docker CLI vào Minikube

```bash
eval $(minikube docker-env)
```

---

## 2.2. Build Data Ingestion Service

Service này có nhiệm vụ:

- Kết nối Binance API
- Lấy dữ liệu realtime
- Publish dữ liệu vào Kafka

Build image:

```bash
docker build -t crypto-data-ingestion:v1 ./services/data-ingestion
```

---

## 2.3. Build Spark Streaming Service

Spark Streaming dùng để:

- Consume dữ liệu từ Kafka
- Xử lý realtime
- Tính toán analytics
- Đẩy dữ liệu vào Redis/MongoDB

Build image:

```bash
docker build -t crypto-spark-streaming:v1 ./services/spark-streaming
```

---

## 2.4. Build Spark Batch & MLlib

Spark Batch phụ trách:

- Batch Processing
- Machine Learning
- Price Prediction

Build image:

```bash
docker build -t crypto-spark-batch:v1 ./services/spark-batch
```

> Lưu ý:
>
> Tên image PHẢI đúng:
>
> ```text
> crypto-spark-batch:v1
> ```
>
> vì Airflow KubernetesPodOperator sẽ gọi chính image này.

---

## 2.5. Build Airflow Custom Image

Nếu project có custom DAGs/plugins:

```bash
docker build -t crypto-airflow:v1 ./services/airflow
```

---

## 2.6. Build FastAPI Backend

Backend phụ trách:

- REST API
- WebSocket realtime
- Kết nối MongoDB/Redis
- Phục vụ dữ liệu cho Frontend

Build image:

```bash
docker build -t crypto-backend:v1 ./services/backend
```

---

## 2.7. Build Next.js Frontend

Frontend dashboard realtime:

```bash
docker build -t crypto-frontend:v1 ./services/frontend
```

---

# 3. Khởi tạo Infrastructure Kubernetes

Toàn bộ manifest Kubernetes nằm trong thư mục:

```text
k8s/
```

Deployment sẽ được thực hiện theo thứ tự:

1. Namespace
2. Configs & Secrets
3. Storage
4. Database
5. Message Queue
6. Applications
7. Orchestration

---

# 3.1. Khởi tạo Namespace & Config

Tạo namespace:

```bash
kubectl apply -f k8s/namespaces.yaml
```

Apply ConfigMaps & Secrets:

```bash
kubectl apply -f k8s/config/
```

Kiểm tra:

```bash
kubectl get ns
```

Namespace mong muốn:

```text
crypto-system
```

---

# 3.2. Triển khai Storage & Database

## Khởi tạo Persistent Volume cho Machine Learning Model

PVC được dùng để lưu:

- model.json
- checkpoint
- trained artifacts

Deploy:

```bash
kubectl apply -f k8s/orchestration/model-pvc.yaml
```

---

## Khởi chạy MongoDB, Redis, PostgreSQL

```bash
kubectl apply -f k8s/storage/
```

Các service sẽ bao gồm:

| Thành phần | Vai trò |
|---|---|
| MongoDB | Historical market data |
| Redis | Speed Layer cache |
| PostgreSQL | Metadata DB cho Airflow |

---

## Kiểm tra trạng thái

```bash
kubectl get pods -n crypto-system
```

Kết quả mong muốn:

```text
mongodb-0       Running
redis-xxxxx     Running
postgres-0      Running
```

---

# 3.3. Triển khai Kafka & Zookeeper

Kafka là trung tâm streaming pipeline.

Triển khai Zookeeper:

```bash
kubectl apply -f k8s/message-queue/zookeeper.yaml
```

Triển khai Kafka:

```bash
kubectl apply -f k8s/message-queue/kafka.yaml
```

Kiểm tra:

```bash
kubectl get pods -n crypto-system
```

Kết quả:

```text
zookeeper-0     Running
kafka-0         Running
```

---

# 4. Triển khai Hệ sinh thái Ứng dụng

---

# 4.1. Khởi chạy Data Ingestion

Data Ingestion sẽ:

- Kết nối Binance WebSocket
- Stream dữ liệu realtime
- Publish vào Kafka Topics

Deploy:

```bash
kubectl apply -f k8s/apps/data-ingestion.yaml
```

Xem logs:

```bash
kubectl logs <pod-name> -n crypto-system
```

Nếu thành công:

```text
Connected to Binance WebSocket
Publishing ticker data to Kafka
```

---

# 4.2. Khởi chạy Spark Streaming Job

Spark Streaming sẽ:

- Consume Kafka
- Tính toán realtime indicators
- Ghi dữ liệu vào MongoDB/Redis

Deploy:

```bash
kubectl apply -f k8s/compute/spark-jobs/streaming-job.yaml
```

Kiểm tra logs:

```bash
kubectl logs <spark-pod> -n crypto-system
```

---

# 4.3. Deploy Backend & Frontend

## Deploy FastAPI Backend

```bash
kubectl apply -f k8s/orchestration/backend.yaml
```

---

## Deploy Next.js Frontend

```bash
kubectl apply -f k8s/orchestration/frontend.yaml
```

---

# 4.4. Khởi tạo Airflow

Airflow cần khởi tạo Database trước khi chạy Scheduler/Webserver.

---

## Chạy Airflow Init Job

```bash
kubectl apply -f k8s/orchestration/airflow-init.yaml
```

Theo dõi pod:

```bash
kubectl get pods -n crypto-system -w
```

Đợi pod:

```text
airflow-init
```

chuyển sang trạng thái:

```text
Completed
```

---

## Deploy Airflow chính thức

```bash
kubectl apply -f k8s/orchestration/airflow.yaml
```

Airflow bao gồm:

- Webserver
- Scheduler

---

# 5. Port Forwarding

Mở nhiều tab terminal và chạy các lệnh sau.

---

## Frontend Dashboard

```bash
kubectl port-forward svc/nextjs-frontend 3000:3000 -n crypto-system
```

Dashboard:

```text
http://localhost:3000
```

---

## Backend API

```bash
kubectl port-forward svc/fastapi-backend 8000:8000 -n crypto-system
```

API:

```text
http://localhost:8000
```

---

## Airflow UI

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n crypto-system
```

Airflow UI:

```text
http://localhost:8080
```

Tài khoản mặc định:

```text
admin / admin
```

---

# 6. Kích hoạt Machine Learning Pipeline

> Đây là bước QUAN TRỌNG nhất của hệ thống.

Do Kubernetes local có thể mất dữ liệu sau khi restart nên cần retrain model trước khi prediction pipeline hoạt động.

---

## Bước 1: Mở Airflow Dashboard

```text
http://localhost:8080
```

---

## Bước 2: Unpause DAGs

Bật hai DAG:

- `crypto_ml_training`
- `crypto_ml_prediction`

---

## Bước 3: Trigger DAG Training

Chạy DAG:

```text
crypto_ml_training
```

Mục tiêu:

- Load dữ liệu historical từ MongoDB
- Huấn luyện mô hình bằng Spark MLlib
- Sinh file:

```text
model.json
```

- Lưu model vào PVC

---

## Bước 4: Chờ DAG Success

Khi DAG hiển thị:

```text
Success
```

nghĩa là model đã được train thành công.

---

## Bước 5: Prediction Pipeline

Sau khi training xong:

```text
crypto_ml_prediction
```

sẽ:

- Tự động chạy mỗi 5 phút
- Sinh dữ liệu prediction
- Ghi prediction vào MongoDB/Redis
- Hiển thị realtime trên dashboard

---

# 7. Monitoring & Observability

Hệ thống tích hợp:

- Prometheus
- Grafana

để theo dõi:

- CPU
- Memory
- Kafka throughput
- Spark jobs
- Airflow DAGs
- API latency

---

## Mở Grafana

```bash
kubectl port-forward svc/grafana 3000:80 -n monitoring
```

Dashboard monitoring:

```text
http://localhost:3000
```

---

# 8. Kiểm tra End-to-End

Truy cập:

```text
http://localhost:3000
```

Hệ thống hoạt động thành công khi:

- WebSocket báo Connected
- Candlestick realtime cập nhật
- Order Book realtime hoạt động
- Gainers/Losers thay đổi liên tục
- ML Predictions hiển thị dữ liệu
- Spark Streaming không báo lỗi
- Airflow DAGs chạy thành công

---

# 9. Kiến trúc Lambda Architecture

## Speed Layer

```text
Binance API
    ↓
Kafka
    ↓
Spark Streaming
    ↓
Redis + MongoDB
    ↓
FastAPI WebSocket
    ↓
Next.js Dashboard
```

---

## Batch Layer

```text
MongoDB Historical Data
    ↓
Airflow
    ↓
Spark MLlib
    ↓
Model Training
    ↓
Prediction Pipeline
    ↓
MongoDB / Redis
```

---

# 10. Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Containerization | Docker |
| Orchestration | Kubernetes |
| Message Queue | Kafka |
| Stream Processing | Spark Streaming |
| Batch Processing | Spark MLlib |
| Workflow Orchestration | Airflow |
| Database | MongoDB |
| Cache | Redis |
| Backend API | FastAPI |
| Frontend | Next.js |
| Monitoring | Prometheus + Grafana |

---

# 11. Debug & Troubleshooting

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

# 12. Kết luận

Hệ thống Crypto Analytics theo kiến trúc Lambda Architecture cho phép:

- Streaming analytics realtime
- Distributed processing
- Machine Learning prediction
- Realtime dashboard
- Microservices deployment
- Big Data orchestration

Đây là mô hình phù hợp cho:

- Financial Analytics Systems
- Big Data Platforms
- Streaming Pipelines
- Machine Learning Infrastructure
- Realtime Monitoring Systems
- Distributed Data Engineering