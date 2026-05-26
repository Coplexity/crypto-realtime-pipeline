# 🧠 Spark Batch & Machine Learning Service

Spark Batch service là thành phần xử lý batch processing và machine learning của hệ thống Crypto Analytics Platform.

Service sử dụng **Apache Spark + Spark MLlib** để:

- Xử lý historical data
- Aggregation dữ liệu OHLC
- Feature engineering
- Train machine learning models
- Price prediction
- Batch analytics

Đây là thành phần chính của **Batch Layer** trong kiến trúc Lambda Architecture.

---

# 🏗️ Kiến trúc Batch Processing

```text
MongoDB Historical Data
        ↓
Spark Batch Processing
        ↓
Feature Engineering
        ↓
Machine Learning Pipeline
        ↓
Prediction Results
        ↓
MongoDB / Redis
        ↓
Frontend Dashboard
```

---

# ⚙️ Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Processing Engine | Apache Spark |
| Machine Learning | Spark MLlib |
| Language | Python |
| Framework | PySpark |
| Storage | MongoDB |
| Cache | Redis |
| Orchestration | Apache Airflow |
| Deployment | Docker + Kubernetes |

---

# 📂 Cấu trúc thư mục

```text
spark-batch/
├── Dockerfile
├── Dockerfile.worker
├── ohlc_1d_aggregator.py
├── ohlc_1h_aggregator.py
├── ohlc_4h_aggregator.py
├── ohlc_5m_aggregator.py
├── predict_price.py
├── requirements.txt
├── test.py
├── train_price_prediction.py
└── utils.py
```

---

# 📦 Mô tả các file chính

| File | Vai trò |
|---|---|
| ohlc_5m_aggregator.py | Aggregate dữ liệu 5 phút |
| ohlc_1h_aggregator.py | Aggregate dữ liệu 1 giờ |
| ohlc_4h_aggregator.py | Aggregate dữ liệu 4 giờ |
| ohlc_1d_aggregator.py | Aggregate dữ liệu 1 ngày |
| train_price_prediction.py | Train machine learning model |
| predict_price.py | Dự đoán giá cryptocurrency |
| utils.py | Utility functions |
| test.py | Testing pipeline |

---

# 🚀 Chức năng chính

## 📊 OHLC Aggregation

Service thực hiện aggregation dữ liệu theo nhiều timeframe:

- 5 phút
- 1 giờ
- 4 giờ
- 1 ngày

---

## 📈 Historical Analytics

Spark Batch xử lý:

- Historical trades
- Market trends
- Volume analysis
- Candlestick generation
- Statistical analysis

---

## 🧠 Machine Learning Pipeline

Pipeline machine learning bao gồm:

- Feature extraction
- Data preprocessing
- Model training
- Prediction generation
- Model evaluation

---

## 🤖 Price Prediction

Hệ thống hỗ trợ:

- Cryptocurrency price forecasting
- Trend prediction
- Market direction analysis
- Historical pattern learning

---

# 🔄 Batch Processing Flow

```text
MongoDB Historical Data
        ↓
Spark DataFrame
        ↓
Feature Engineering
        ↓
ML Training / Prediction
        ↓
Prediction Output
        ↓
MongoDB / Redis
```

---

# 📡 Data Sources

Spark Batch đọc dữ liệu từ:

| Source | Vai trò |
|---|---|
| MongoDB | Historical market data |
| Redis | Cached realtime data |
| Kafka | Streaming integration (optional) |

---

# 🧩 OHLC Aggregation Pipeline

Pipeline aggregation:

```text
Historical Trades
        ↓
Grouping by Time Window
        ↓
Open / High / Low / Close Calculation
        ↓
OHLC Candles
        ↓
MongoDB Storage
```

---

# 📊 Aggregation Timeframes

| Script | Timeframe |
|---|---|
| ohlc_5m_aggregator.py | 5 phút |
| ohlc_1h_aggregator.py | 1 giờ |
| ohlc_4h_aggregator.py | 4 giờ |
| ohlc_1d_aggregator.py | 1 ngày |

---

# 🧠 Machine Learning Workflow

## Training Pipeline

```text
Historical Data
        ↓
Feature Engineering
        ↓
Dataset Preparation
        ↓
Model Training
        ↓
Model Export
```

---

## Prediction Pipeline

```text
Realtime / Historical Data
        ↓
Feature Transformation
        ↓
Prediction Model
        ↓
Price Forecast
        ↓
MongoDB / Redis
```

---

# 📈 Spark MLlib

Hệ thống sử dụng Spark MLlib để:

- Distributed ML processing
- Feature transformation
- Regression / classification
- Model training
- Scalable analytics

---

# 📦 Model Storage

Model ML được lưu thông qua:

```text
Persistent Volume Claim (PVC)
```

hoặc:

```text
MongoDB
```

---

# 🔁 Airflow Integration

Spark Batch jobs được orchestration bởi Airflow.

---

## Workflow

```text
Airflow Scheduler
        ↓
Spark Batch Jobs
        ↓
Training / Prediction
        ↓
MongoDB / Redis
```

---

# ⚡ Distributed Processing

Spark Batch hỗ trợ:

- Distributed execution
- Parallel processing
- Horizontal scaling
- Fault tolerance

---

# 🐳 Docker Deployment

## Build Docker Image

```bash
docker build -t crypto-spark-batch:v1 .
```

---

## Run Container

```bash
docker run crypto-spark-batch:v1
```

---

# ☸️ Kubernetes Deployment

Spark Batch được triển khai thông qua:

```text
k8s/compute/spark-jobs/
```

---

## Spark Jobs

| File | Chức năng |
|---|---|
| train-price-prediction.yaml | ML training job |
| predict-price.yaml | Prediction job |
| ohlc-*.yaml | Aggregation jobs |

---

# 🚀 Chạy local development

## Cài dependencies

```bash
pip install -r requirements.txt
```

---

## Chạy training

```bash
python train_price_prediction.py
```

---

## Chạy prediction

```bash
python predict_price.py
```

---

# 📊 Feature Engineering

Feature engineering bao gồm:

- Price trends
- Volume indicators
- Market volatility
- Historical momentum
- Statistical indicators

---

# 🧪 Model Evaluation

Hệ thống hỗ trợ:

- Prediction accuracy
- Loss evaluation
- Historical validation
- Trend analysis

---

# 📈 Batch Analytics

Spark Batch hỗ trợ:

- Historical computation
- Large-scale aggregation
- Offline analytics
- Distributed ETL

---

# 🔒 Environment Variables

Ví dụ:

```env
MONGO_URI=mongodb://mongodb:27017
REDIS_HOST=redis
REDIS_PORT=6379
MODEL_PATH=/models/model.json
```

---

# 📚 Vai trò trong Lambda Architecture

Spark Batch đóng vai trò:

- Batch Layer
- Historical Analytics Engine
- Machine Learning Engine
- Long-term data processing system

---

# 📡 Integration với các Services khác

## Input Services

| Service | Vai trò |
|---|---|
| spark-streaming | Historical market data |
| MongoDB | Batch data storage |

---

## Output Services

| Service | Vai trò |
|---|---|
| backend | Prediction APIs |
| frontend | Prediction dashboard |
| airflow | Workflow orchestration |

---

# 🔍 Monitoring

Hệ thống hỗ trợ monitoring:

- Spark job metrics
- Batch processing time
- Training status
- Prediction performance
- Executor monitoring

---

# ⚠️ Lưu ý

- Spark Batch phụ thuộc MongoDB historical data
- Airflow cần hoạt động ổn định để orchestration jobs
- ML model cần được retrain định kỳ để đảm bảo accuracy

---

# 🔮 Hướng phát triển

- Deep Learning integration
- LSTM / Transformer forecasting
- Hyperparameter tuning
- Automated retraining
- Distributed model serving
- Advanced feature engineering
- Multi-asset prediction
- Online learning
- Model versioning
- MLflow integration