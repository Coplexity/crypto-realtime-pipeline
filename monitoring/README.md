# 📈 Monitoring & Observability

Thư mục `monitoring/` chứa toàn bộ cấu hình giám sát (Monitoring) và quan sát hệ thống (Observability) cho Crypto Analytics Platform.

Hệ thống sử dụng:

- Prometheus
- Grafana
- Kubernetes Metrics
- Spark Metrics
- Kafka Monitoring

để theo dõi:

- Hiệu năng hệ thống
- Trạng thái services
- Realtime throughput
- Resource utilization
- Streaming pipeline health

---

# 🏗️ Monitoring Architecture

```text
Kubernetes Cluster
        ↓
Prometheus Metrics Collection
        ↓
Grafana Dashboards
        ↓
Realtime Monitoring Visualization
```

---

# ⚙️ Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Metrics Collection | Prometheus |
| Visualization | Grafana |
| Monitoring Targets | Kubernetes + Spark + Kafka |
| Dashboard | Grafana Dashboard JSON |
| Deployment | Docker + Kubernetes |

---

# 📂 Cấu trúc thư mục

```text
monitoring/
├── grafana
│   ├── dashboards
│   │   └── crypto.json
│   │
│   └── provisioning
│       ├── dashboards
│       │   └── crypto.yaml
│       │
│       └── datasources
│           └── prometheus.yaml
│
└── prometheus.yml
```

---

# 📦 Tổng quan các thành phần

| Thành phần | Vai trò |
|---|---|
| prometheus.yml | Cấu hình Prometheus |
| grafana/dashboards/crypto.json | Dashboard visualization |
| grafana/provisioning/dashboards/crypto.yaml | Auto-load dashboards |
| grafana/provisioning/datasources/prometheus.yaml | Prometheus datasource |

---

# 📊 Prometheus

File:

```text
prometheus.yml
```

---

## Chức năng

Prometheus chịu trách nhiệm:

- Thu thập metrics
- Scrape services
- Monitoring cluster
- Time-series storage
- Metrics aggregation

---

# 🔄 Metrics Collection Flow

```text
Applications
    ↓
Prometheus Exporters
    ↓
Prometheus Server
    ↓
Grafana
```

---

# 📡 Monitoring Targets

Prometheus theo dõi:

| Service | Metrics |
|---|---|
| Kafka | Throughput & brokers |
| Spark | Executors & jobs |
| Airflow | DAG execution |
| FastAPI | API latency |
| MongoDB | Database performance |
| Redis | Cache metrics |
| Kubernetes | Cluster resources |

---

# ⚡ Spark Monitoring

Hệ thống theo dõi:

- Executor usage
- Job duration
- Task failures
- Streaming throughput
- Batch execution time

---

# 📨 Kafka Monitoring

Metrics Kafka bao gồm:

- Topic throughput
- Producer rate
- Consumer lag
- Broker health
- Partition statistics

---

# ☸️ Kubernetes Monitoring

Prometheus theo dõi:

- CPU usage
- Memory usage
- Pod health
- Node status
- Deployment status
- Resource limits

---

# 📈 Grafana

Grafana được sử dụng để:

- Visualization metrics
- Dashboard monitoring
- Realtime analytics
- Cluster monitoring
- Streaming monitoring

---

# 🎨 Dashboard Architecture

```text
Prometheus
        ↓
Grafana Datasource
        ↓
Dashboard Panels
        ↓
Realtime Visualization
```

---

# 📊 Dashboard Features

Dashboard hỗ trợ:

- Realtime metrics
- System overview
- Spark monitoring
- Kafka throughput
- Cluster health
- Resource monitoring

---

# 📁 Dashboard Configuration

## Dashboard JSON

File:

```text
grafana/dashboards/crypto.json
```

---

## Chức năng

Chứa:

- Dashboard layout
- Visualization panels
- Query configuration
- Charts & graphs

---

# 🔌 Datasource Configuration

File:

```text
grafana/provisioning/datasources/prometheus.yaml
```

---

## Chức năng

Tự động kết nối Grafana với Prometheus datasource.

---

# 📊 Dashboard Provisioning

File:

```text
grafana/provisioning/dashboards/crypto.yaml
```

---

## Chức năng

Tự động:

- Load dashboards
- Import dashboard JSON
- Provision Grafana dashboards

---

# 🚀 Triển khai Monitoring

## Chạy Prometheus

```bash
docker run -p 9090:9090 prom/prometheus
```

---

## Chạy Grafana

```bash
docker run -p 3001:3000 grafana/grafana
```

---

# ☸️ Kubernetes Monitoring Deployment

Monitoring có thể triển khai trên Kubernetes thông qua:

```text
Prometheus Deployment
Grafana Deployment
```

---

# 🌐 Truy cập Monitoring UI

## Grafana

```text
http://localhost:3001
```

---

## Prometheus

```text
http://localhost:9090
```

---

# 🔑 Grafana Credentials

Default credentials:

```text
Username: admin
Password: admin
```

---

# 📈 Realtime Metrics

Dashboard hỗ trợ realtime monitoring:

- Streaming latency
- Kafka throughput
- Spark job execution
- API performance
- Resource consumption

---

# ⚡ Performance Monitoring

Hệ thống theo dõi:

| Metric | Mô tả |
|---|---|
| CPU Usage | Resource utilization |
| Memory Usage | RAM consumption |
| Network Traffic | Data throughput |
| API Latency | Backend response time |
| Kafka Throughput | Streaming rate |
| Spark Job Time | Processing duration |

---

# 🔍 System Health Monitoring

Monitoring hỗ trợ:

- Pod status
- Deployment health
- Container crashes
- Restart counts
- Cluster stability

---

# 📡 Observability Pipeline

```text
Applications
    ↓
Metrics Exporters
    ↓
Prometheus
    ↓
Grafana
    ↓
Visualization Dashboard
```

---

# 📊 Lambda Architecture Monitoring

Monitoring bao phủ toàn bộ Lambda Architecture:

---

## Speed Layer

Theo dõi:

- Spark Streaming
- Kafka realtime throughput
- Redis performance

---

## Batch Layer

Theo dõi:

- Spark Batch jobs
- ML training jobs
- Prediction pipelines

---

## Serving Layer

Theo dõi:

- FastAPI APIs
- WebSocket performance
- Frontend health

---

# 🧠 Spark Metrics

Spark monitoring bao gồm:

- Executors
- Drivers
- Task execution
- Batch duration
- Streaming rate

---

# 📦 Resource Monitoring

Kubernetes monitoring hỗ trợ:

- Resource requests
- Resource limits
- Node pressure
- Pod scheduling

---

# 🔒 Security Monitoring

Có thể mở rộng để theo dõi:

- Authentication logs
- API access
- Cluster events
- Security alerts

---

# 🐳 Docker Integration

Monitoring hỗ trợ:

- Container metrics
- Docker resource usage
- Container lifecycle monitoring

---

# 🚀 Chạy local development

## Start Prometheus

```bash
prometheus --config.file=prometheus.yml
```

---

## Start Grafana

```bash
grafana-server
```

---

# 📊 Visualization Panels

Dashboard có thể hiển thị:

- Time-series graphs
- Heatmaps
- Gauges
- Tables
- Cluster overview
- Streaming metrics

---

# 🧪 Alerting

Có thể mở rộng:

- Slack alerts
- Email alerts
- Failure notifications
- Resource threshold alerts

---

# 📚 Vai trò trong Lambda Architecture

Monitoring đóng vai trò:

- Observability Layer
- Performance Analytics
- System Health Monitoring
- Infrastructure Analytics

---

# ⚠️ Lưu ý

- Grafana phụ thuộc Prometheus datasource
- Spark metrics cần được expose đúng cách
- Kafka monitoring yêu cầu exporters
- Resource monitoring cần Kubernetes metrics-server

---

# 🔮 Hướng phát triển

- Loki log aggregation
- ELK Stack integration
- Distributed tracing
- Jaeger integration
- AlertManager integration
- Advanced anomaly detection
- ML-based monitoring
- Multi-cluster monitoring
- Realtime alerting
- SLO/SLA dashboards