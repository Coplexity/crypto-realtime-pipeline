# ☸️ Kubernetes Infrastructure

Thư mục `k8s/` chứa toàn bộ cấu hình Kubernetes dùng để triển khai hệ thống Crypto Analytics Platform trên môi trường containerized cluster.

Hệ thống được triển khai theo mô hình:

- Microservices Architecture
- Distributed Data Processing
- Lambda Architecture
- Cloud-native Infrastructure

Toàn bộ services được orchestration thông qua Kubernetes trên Minikube hoặc Kubernetes Cluster.

---

# 🏗️ Kubernetes Architecture

```text
                        Kubernetes Cluster
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  Frontend Layer                                             │
│  └── Next.js Dashboard                                      │
│                                                             │
│  Backend Layer                                              │
│  └── FastAPI APIs + WebSocket                               │
│                                                             │
│  Streaming Layer                                            │
│  ├── Kafka                                                  │
│  ├── Spark Streaming                                        │
│  └── Data Ingestion                                         │
│                                                             │
│  Batch & ML Layer                                           │
│  ├── Spark Batch                                            │
│  └── Airflow                                                │
│                                                             │
│  Storage Layer                                              │
│  ├── MongoDB                                                │
│  ├── Redis                                                  │
│  └── PostgreSQL                                             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

# 📂 Cấu trúc thư mục

```text
k8s/
├── apps/
├── compute/
├── config/
├── message-queue/
├── orchestration/
├── routing/
├── storage/
└── namespaces.yaml
```

---

# 📦 Tổng quan các thành phần

| Thư mục | Vai trò |
|---|---|
| apps/ | Application deployments |
| compute/ | Spark jobs & compute resources |
| config/ | ConfigMaps & Secrets |
| message-queue/ | Kafka & Zookeeper |
| orchestration/ | Airflow, Backend, Frontend |
| routing/ | Ingress & routing |
| storage/ | MongoDB, Redis, PostgreSQL |
| namespaces.yaml | Kubernetes namespace |

---

# 1. 📡 apps/

Thư mục:

```text
k8s/apps/
```

---

## Chức năng

Chứa các deployment của application services.

---

## Thành phần

| File | Vai trò |
|---|---|
| data-ingestion.yaml | Deploy Binance ingestion service |

---

## Data Flow

```text
Binance
    ↓
Data Ingestion
    ↓
Kafka
```

---

# 2. ⚡ compute/

Thư mục:

```text
k8s/compute/
```

---

## Chức năng

Chứa cấu hình cho:

- Spark jobs
- Spark Operator
- Distributed compute workloads

---

## Cấu trúc

```text
compute/
├── spark-jobs/
└── spark-operator-values.yaml
```

---

# 📊 spark-jobs/

Chứa các Spark batch và streaming jobs.

---

## Thành phần

| File | Vai trò |
|---|---|
| native-streaming.yaml | Native Spark streaming |
| streaming-job.yaml | Spark realtime pipeline |
| ohlc-5m-aggregator.yaml | OHLC aggregation 5m |
| ohlc-1h-aggregator.yaml | OHLC aggregation 1h |
| ohlc-4h-aggregator.yaml | OHLC aggregation 4h |
| ohlc-1d-aggregator.yaml | OHLC aggregation 1d |
| train-price-prediction.yaml | ML model training |
| predict-price.yaml | Price prediction |

---

## Spark Architecture

```text
Spark Driver Pod
        ↓
Spark Executor Pods
        ↓
Distributed Processing
```

---

# 3. ⚙️ config/

Thư mục:

```text
k8s/config/
```

---

## Chức năng

Quản lý:

- ConfigMaps
- Secrets
- Environment variables
- Cluster configuration

---

## Thành phần

| File | Vai trò |
|---|---|
| configmaps.yaml | System configuration |
| secrets.yaml | Sensitive credentials |

---

## Ví dụ cấu hình

```text
Kafka brokers
MongoDB URI
Redis configuration
API keys
Environment variables
```

---

# 4. 📨 message-queue/

Thư mục:

```text
k8s/message-queue/
```

---

## Chức năng

Triển khai hệ thống message queue.

---

## Thành phần

| File | Vai trò |
|---|---|
| kafka.yaml | Kafka cluster |
| zookeeper.yaml | Zookeeper service |

---

## Message Queue Architecture

```text
Data Ingestion
        ↓
Kafka Topics
        ↓
Spark Streaming
```

---

# 📡 Kafka Topics

Hệ thống sử dụng Kafka cho:

- Trades stream
- Tickers
- Orderbook
- Predictions
- Market events

---

# 5. 🔁 orchestration/

Thư mục:

```text
k8s/orchestration/
```

---

## Chức năng

Triển khai orchestration services:

- Airflow
- Backend
- Frontend
- PVC storage

---

## Thành phần

| File | Vai trò |
|---|---|
| airflow-init.yaml | Initialize Airflow database |
| airflow.yaml | Deploy Airflow |
| backend.yaml | Deploy FastAPI backend |
| frontend.yaml | Deploy Next.js frontend |
| model-pvc.yaml | Persistent model storage |

---

## Airflow Workflow

```text
Airflow Scheduler
        ↓
Spark Jobs
        ↓
ML Pipeline
```

---

# 6. 🌐 routing/

Thư mục:

```text
k8s/routing/
```

---

## Chức năng

Quản lý:

- Ingress
- External access
- HTTP routing
- Reverse proxy

---

## Thành phần

| File | Vai trò |
|---|---|
| ingress.yaml | Ingress configuration |

---

## Routing Flow

```text
External Client
        ↓
Ingress
        ↓
Frontend / Backend Services
```

---

# 7. 🗄️ storage/

Thư mục:

```text
k8s/storage/
```

---

## Chức năng

Triển khai hệ thống storage và databases.

---

## Thành phần

| File | Vai trò |
|---|---|
| mongodb.yaml | MongoDB StatefulSet |
| redis.yaml | Redis deployment |
| postgres.yaml | PostgreSQL database |

---

# 🧩 Storage Roles

| Database | Vai trò |
|---|---|
| MongoDB | Historical market data |
| Redis | Realtime cache |
| PostgreSQL | Airflow metadata |

---

# 🏗️ Namespace

File:

```text
namespaces.yaml
```

---

## Chức năng

Tạo namespace:

```text
crypto-system
```

để cô lập toàn bộ hệ thống.

---

# 🚀 Deployment Workflow

## 1. Tạo Namespace

```bash
kubectl apply -f k8s/namespaces.yaml
```

---

## 2. Apply Configurations

```bash
kubectl apply -f k8s/config/
```

---

## 3. Deploy Storage

```bash
kubectl apply -f k8s/storage/
```

---

## 4. Deploy Kafka

```bash
kubectl apply -f k8s/message-queue/
```

---

## 5. Deploy Applications

```bash
kubectl apply -f k8s/apps/
kubectl apply -f k8s/orchestration/
```

---

## 6. Deploy Spark Jobs

```bash
kubectl apply -f k8s/compute/spark-jobs/
```

---

# ☸️ Kubernetes Resources

Hệ thống sử dụng:

- Deployments
- StatefulSets
- Services
- ConfigMaps
- Secrets
- PersistentVolumeClaims
- Ingress
- SparkApplications

---

# 📊 Distributed Architecture

Hệ thống hỗ trợ:

- Horizontal scaling
- Fault tolerance
- Distributed execution
- Container orchestration
- Rolling updates

---

# 🐳 Docker Integration

Tất cả services đều được container hóa:

| Service | Docker Image |
|---|---|
| frontend | crypto-frontend:v1 |
| backend | crypto-backend:v1 |
| airflow | crypto-airflow:v1 |
| spark-batch | crypto-spark-batch:v1 |
| spark-streaming | crypto-spark-streaming:v1 |
| data-ingestion | crypto-data-ingestion:v1 |

---

# 📈 Monitoring

Hệ thống tích hợp:

- Kubernetes Dashboard
- Prometheus
- Grafana
- Spark monitoring
- Pod monitoring

---

# 🔍 Useful Commands

## Kiểm tra Pods

```bash
kubectl get pods -n crypto-system
```

---

## Kiểm tra Services

```bash
kubectl get svc -n crypto-system
```

---

## Kiểm tra Logs

```bash
kubectl logs <pod-name> -n crypto-system
```

---

## Port Forward

```bash
kubectl port-forward svc/nextjs-frontend 3000:3000 -n crypto-system
```

---

# 🌐 Port Mapping

| Service | Port |
|---|---|
| Frontend | 3000 |
| Backend | 8000 |
| Airflow | 8080 |
| MongoDB | 27017 |
| Kafka | 9092 |
| Redis | 6379 |

---

# 📚 Vai trò trong Lambda Architecture

Kubernetes đóng vai trò:

- Infrastructure Layer
- Orchestration Platform
- Distributed Runtime
- Container Management System

---

# ⚠️ Lưu ý

- Minikube cần tối thiểu 6 CPUs và 12GB RAM
- Spark jobs yêu cầu tài nguyên lớn
- Kafka cần hoạt động ổn định để realtime streaming
- Persistent storage cần mount đúng cho ML models

---

# 🔮 Hướng phát triển

- Helm charts
- Autoscaling
- Multi-node cluster
- Service mesh
- CI/CD integration
- GitOps deployment
- ArgoCD integration
- Cluster monitoring nâng cao
- Multi-environment deployment
- Cloud-native optimization