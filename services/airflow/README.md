# 🔁 Airflow Orchestration Service

Airflow service là thành phần orchestration trung tâm của hệ thống Crypto Analytics Platform.

Service sử dụng **Apache Airflow** để:

- Scheduling batch jobs
- Điều phối Spark jobs
- Trigger machine learning pipelines
- Quản lý workflow tự động
- Theo dõi trạng thái pipeline
- Tự động hóa hệ thống phân tích dữ liệu

Airflow đóng vai trò là bộ điều phối của toàn bộ kiến trúc Lambda Architecture.

---

# 🏗️ Kiến trúc Orchestration

```text
Airflow Scheduler
        ↓
DAG Execution
        ↓
Spark Batch Jobs
        ↓
Machine Learning Pipeline
        ↓
MongoDB / Redis
        ↓
Backend APIs
        ↓
Frontend Dashboard
```

---

# ⚙️ Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Workflow Engine | Apache Airflow |
| Language | Python |
| Scheduler | Airflow Scheduler |
| Executors | KubernetesPodOperator |
| Batch Processing | Apache Spark |
| Storage | PostgreSQL |
| Deployment | Docker + Kubernetes |

---

# 📂 Cấu trúc thư mục

```text
airflow/
├── Dockerfile
└── dags/
    ├── maintenance_dag.py
    ├── ml_prediction_dag.py
    └── ohlc_spark_aggregator.py
```

---

# 📦 Mô tả các file chính

| File | Vai trò |
|---|---|
| Dockerfile | Docker image cho Airflow |
| ml_prediction_dag.py | ML prediction workflow |
| ohlc_spark_aggregator.py | OHLC aggregation scheduling |
| maintenance_dag.py | Maintenance & cleanup workflow |

---

# 🚀 Chức năng chính

## 📅 Workflow Scheduling

Airflow thực hiện:

- Scheduling Spark jobs
- Trigger batch pipelines
- Chạy ML training định kỳ
- Prediction automation

---

## 🧠 Machine Learning Orchestration

Airflow điều phối:

```text
Historical Data
        ↓
Training Pipeline
        ↓
Model Generation
        ↓
Prediction Pipeline
        ↓
Prediction Results
```

---

## 📊 OHLC Aggregation Scheduling

Airflow tự động chạy:

- 5m aggregation
- 1h aggregation
- 4h aggregation
- 1d aggregation

---

## 🔄 Automation Workflow

Hệ thống hỗ trợ:

- Automated retraining
- Scheduled predictions
- Data aggregation
- Pipeline automation

---

# 🧩 DAGs Overview

---

# 1. 🤖 ML Prediction DAG

File:

```text
ml_prediction_dag.py
```

## Chức năng

DAG này thực hiện:

- Trigger model training
- Chạy prediction jobs
- Đồng bộ prediction data
- Ghi prediction vào MongoDB

---

## Workflow

```text
Training Job
        ↓
Model Generation
        ↓
Prediction Job
        ↓
MongoDB / Redis
```

---

# 2. 📈 OHLC Aggregator DAG

File:

```text
ohlc_spark_aggregator.py
```

## Chức năng

Tự động chạy Spark aggregation jobs:

- OHLC 5 phút
- OHLC 1 giờ
- OHLC 4 giờ
- OHLC 1 ngày

---

## Workflow

```text
Historical Trades
        ↓
Spark Aggregation
        ↓
OHLC Candles
        ↓
MongoDB
```

---

# 3. 🛠️ Maintenance DAG

File:

```text
maintenance_dag.py
```

## Chức năng

Maintenance workflow:

- Cleanup dữ liệu cũ
- Monitoring pipeline
- System health checking
- Maintenance automation

---

# 🔄 Airflow Workflow Architecture

```text
Airflow Scheduler
        ↓
DAG Parsing
        ↓
Task Scheduling
        ↓
Spark Kubernetes Jobs
        ↓
Distributed Execution
```

---

# ☸️ Kubernetes Integration

Airflow sử dụng:

```text
KubernetesPodOperator
```

để:

- Launch Spark jobs
- Chạy batch pipelines
- Scale processing jobs
- Tự động orchestration

---

# 📦 Spark Job Orchestration

Airflow điều phối các Spark jobs:

| Job | Vai trò |
|---|---|
| train-price-prediction | ML training |
| predict-price | Price prediction |
| ohlc-aggregator | OHLC aggregation |

---

# 📡 Data Flow

## Training Pipeline

```text
MongoDB Historical Data
        ↓
Spark Training Job
        ↓
Model Generation
        ↓
Persistent Storage
```

---

## Prediction Pipeline

```text
Trained Model
        ↓
Prediction Job
        ↓
MongoDB / Redis
        ↓
Frontend Dashboard
```

---

# 🧠 Machine Learning Lifecycle

Airflow quản lý:

- Training schedules
- Model retraining
- Prediction execution
- Workflow dependencies

---

# 📊 Scheduling System

Hệ thống hỗ trợ:

- Cron scheduling
- Periodic execution
- Dependency management
- Retry policies

---

# 🗄️ Metadata Database

Airflow sử dụng PostgreSQL để lưu:

- DAG metadata
- Task states
- Execution history
- Logs
- Scheduling information

---

# 🐳 Docker Deployment

## Build Docker Image

```bash
docker build -t crypto-airflow:v1 .
```

---

## Run Container

```bash
docker run crypto-airflow:v1
```

---

# ☸️ Kubernetes Deployment

Airflow được triển khai thông qua:

```text
k8s/orchestration/
```

---

## Deployment Files

| File | Vai trò |
|---|---|
| airflow-init.yaml | Khởi tạo database |
| airflow.yaml | Deploy Airflow services |

---

# 🚀 Khởi tạo Airflow Database

## Database Migration

```bash
airflow db migrate
```

---

## Tạo tài khoản admin

```bash
airflow users create \
  -u admin \
  -p admin \
  -f Admin \
  -l User \
  -r Admin \
  -e admin@example.com
```

---

# 🌐 Truy cập Airflow UI

Sau khi deploy:

```text
http://localhost:8080
```

---

## Default Credentials

```text
Username: admin
Password: admin
```

---

# 📈 Airflow Web UI

Airflow UI hỗ trợ:

- DAG monitoring
- Task tracking
- Logs visualization
- Workflow debugging
- Retry management

---

# 🚀 Chạy local development

## Cài dependencies

```bash
pip install apache-airflow
```

---

## Khởi tạo Airflow

```bash
airflow db init
```

---

## Chạy scheduler

```bash
airflow scheduler
```

---

## Chạy webserver

```bash
airflow webserver
```

---

# 📊 Monitoring & Logging

Airflow hỗ trợ:

- DAG execution logs
- Task monitoring
- Scheduler monitoring
- Retry tracking
- Workflow metrics

---

# 🧪 Fault Tolerance

Hệ thống hỗ trợ:

- Task retries
- Failure recovery
- Dependency management
- Distributed execution recovery

---

# 🔒 Environment Variables

Ví dụ:

```env
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:password@postgres:5432/airflow
```

---

# 📚 Vai trò trong Lambda Architecture

Airflow đóng vai trò:

- Orchestration Layer
- Workflow Scheduler
- Batch Processing Coordinator
- Machine Learning Automation Engine

---

# 📡 Integration với các Services khác

## Input Services

| Service | Vai trò |
|---|---|
| MongoDB | Historical data |
| Spark Batch | ML processing |

---

## Output Services

| Service | Vai trò |
|---|---|
| backend | Prediction APIs |
| frontend | Prediction dashboard |
| Redis | Cached prediction data |

---

# 🔍 Monitoring

Hệ thống hỗ trợ:

- DAG monitoring
- Scheduler monitoring
- Spark job monitoring
- Kubernetes job tracking
- Task execution metrics

---

# ⚠️ Lưu ý

- Airflow phụ thuộc PostgreSQL metadata database
- Spark jobs cần Kubernetes hoạt động ổn định
- DAGs cần được bật (Unpause) để chạy tự động
- ML model cần retrain định kỳ để đảm bảo accuracy

---

# 🔮 Hướng phát triển

- CeleryExecutor integration
- Distributed Airflow cluster
- MLflow integration
- Dynamic DAG generation
- Advanced monitoring
- SLA monitoring
- Auto-recovery workflows
- DAG versioning
- Event-driven orchestration
- Workflow optimization