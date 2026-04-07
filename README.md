# Hệ Thống Xử Lý Dữ Liệu Tiền Điện Tử Real-time

**Nền tảng xử lý dữ liệu lớn (Big Data) cho thị trường tiền điện tử**

## Tổng Quan

Hệ thống này xây dựng một quy trình xử lý dữ liệu real-time hoàn chỉnh:
- **Quá trình 1:** Thu thập dữ liệu real-time từ sàn Binance
- **Quá trình 2:** Xử lý, tích hợp, lưu trữ sử dụng Big Data Stack (Kafka, Spark)
- **Quá trình 3:** Phân tích, tối ưu hóa, và trực quan hóa dữ liệu
- **Quá trình 4:** Cung cấp API và dashboard để giám sát

## Kiến Trúc Tổng Thể

```
┌─────────────────────────────────────────────────────────────┐
│             HỆ THỐNG XỬ LÝ DỮ LIỆU TIỀN ĐIỆN TỬ             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  PHASE 1: HẠ TẦNG CÔNG NGHỆ                                │
│  ├─ Docker & Kubernetes (Minikube)                         │
│  ├─ Apache Kafka (Message Queue)                           │
│  ├─ HDFS (Dung lượng lưu trữ phân tán)                    │
│  └─ Monitoring Stack (Prometheus, Grafana)                 │
│                                                             │
│  PHASE 2: THU THẬP DỮ LIỆU (🔄 HIỆN TẠI)                 │
│  ├─ Binance WebSocket API (Ghi dịch real-time)            │
│  ├─ Kafka Producer (Gửi đáng tin cậy)                     │
│  ├─ Kafka Topic (binance_trades)                           │
│  └─ Verification Consumer (Kiểm tra chất lượng)            │
│                                                             │
│  PHASE 3: XỬ LÝ DỮ LIỆU                                    │
│  ├─ Spark Structured Streaming                            │
│  ├─ OHLCV Aggregation (1m, 5m, 1h, 4h, 1d)               │
│  ├─ Window Functions & Watermarking                        │
│  └─ MongoDB/HDFS Storage                                   │
│                                                             │
│  PHASE 4: LƯU TRỮ & PHÂN PHỐI                             │
│  ├─ MongoDB (Dữ liệu lịch sử)                             │
│  ├─ Redis (Cache real-time)                                │
│  ├─ HDFS (Sao lưu batch)                                   │
│  └─ Quản lý phiên bản dữ liệu                              │
│                                                             │
│  PHASE 5: TRỰC QUAN HÓA & API                             │
│  ├─ FastAPI Backend (REST + WebSocket)                     │
│  ├─ Next.js Frontend (Dashboard tương tác)                 │
│  ├─ Biểu đồ real-time & phân tích                          │
│  └─ Tích hợp dự báo ML                                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Những Tính Năng Chính

### 🔄 Thu Thập Dữ Liệu (Phase 2 - Hiện tại)
- ✅ Kết nối trực tiếp WebSocket với Binance
- ✅ Truyền tải dữ liệu real-time (30+ ghi dịch/giây)
- ✅ Xử lý tự động khi mất kết nối (auto-reconnect)
- ✅ Gửi đến Kafka với bảo đảm độ tin cậy cao (acks=all)
- ✅ Xác minh chất lượng dữ liệu real-time

### ⚙️ Xử Lý Dữ Liệu (Phase 3 - Sắp tới)
- Streaming aggregation với Spark Structured Streaming
- Tạo nến giá (OHLCV) từ dữ liệu trade thô
- Xử lý dữ liệu muộn (Watermarking 10 giây)
- Tính toán chỉ số kỹ thuật

### 💾 Lưu Trữ Dữ Liệu (Phase 4 - Sắp tới)
- MongoDB cho dữ liệu lịch sử
- Redis cho cache real-time
- HDFS cho sao lưu batch
- Quản lý version dữ liệu

### 📊 Trực Quan Hóa (Phase 5 - Sắp tới)
- Dashboard Next.js với biểu đồ nến Nhật
- Theo dõi khối lượng giao dịch
- Bảng xếp hạng top gainers/losers
- Dự báo giá sử dụng ML

## Yêu Cầu Hệ Thống

### Phần Mềm
- **Docker Desktop** 20.10 hoặc cao hơn
- **Python** 3.9 hoặc cao hơn
- **Node.js** 18+ (cho frontend)
- **Git** 2.30+

### Phần Cứng
- **CPU:** 4 cores tối thiểu (8 cores khuyến nghị)
- **RAM:** 8GB tối thiểu (16GB khuyến nghị)
- **Disk:** 50GB tối thiểu
- **Network:** Kết nối Internet ổn định

### Hệ Điều Hành
- Windows 10/11 (với Docker Desktop)
- macOS 10.15+
- Linux (Ubuntu 20.04+)

## Công Nghệ Sử Dụng

| Thành Phần | Công Nghệ | Phiên Bản |
|-----------|-----------|----------|
| **Message Queue** | Apache Kafka | 3.x |
| **Stream Processing** | Apache Spark | 3.x |
| **Database** | MongoDB | 5.0+ |
| **Cache** | Redis | 7.0+ |
| **Scheduling** | Apache Airflow | 2.x |
| **API** | FastAPI | 0.95+ |
| **Frontend** | Next.js | 14.x |
| **Containerization** | Docker & Compose | 20.10+ |
| **Infrastructure** | Kubernetes (Minikube) | 1.24+ |

## Cấu Trúc Thư Mục

```
d:\crypto-realtime-pipeline\
│
├── 📄 DOCUMENTATION/
│   ├── README.md                       ← Tổng quan (file này)
│   ├── ARCHITECTURE.md                 ← Kiến trúc chi tiết
│   ├── PHASE_2_SPECIFICATION.md        ← Spec kỹ thuật Phase 2
│   ├── INSTALLATION.md                 ← Hướng dẫn cài đặt
│   ├── CONFIGURATION.md                ← Danh sách cấu hình
│   ├── TROUBLESHOOTING.md              ← Khắc phục sự cố
│   ├── PERFORMANCE.md                  ← Benchmark
│   ├── RUN.md                          ← Quick start
│   └── CHANGELOG.md                    ← Lịch sử versions
│
├── 📜 CONFIGURATION/
│   ├── docker-compose.yml              ← Docker services
│   ├── requirements.txt                ← Python dependencies
│   ├── .env.example                    ← Environment template
│   └── .gitignore
│
├── 🐍 CODE - PHASE 2/
│   ├── binance_to_kafka.py            ← Binance → Kafka Producer
│   ├── kafka_consumer_verification.py ← Kafka Consumer
│   └── setup_kafka_topics.sh           ← Kafka setup script
│
├── 🐍 CODE - OTHER/
│   ├── main.py                         ← FastAPI backend
│   ├── spark_processor.py              ← Spark processor
│   └── test.py                         ← Tests
│
├── 🎨 FRONTEND/
│   └── (Next.js application)
│
├── 📋 METADATA/
│   ├── mygroup.md                      ← Task assignments
│   ├── PROJECT_STATUS.md               ← Current status
│   └── .git/                           ← Git repository
│
└── .gitignore
```

## Bắt Đầu Nhanh

### 1️⃣ Cài Đặt Môi Trường

```bash
# Clone repository
git clone <repository-url>
cd crypto-realtime-pipeline

# Tạo virtual environment
python -m venv .venv

# Kích hoạt (Windows)
.venv\Scripts\activate
# Kích hoạt (Linux/Mac)
source .venv/bin/activate

# Cài đặt dependencies
pip install -r requirements.txt
```

### 2️⃣ Cấu Hình Hệ Thống

```bash
# Copy template
cp .env.example .env

# Xác minh các biến (mặc định đã tối ưu)
cat .env
```

### 3️⃣ Khởi Động Infrastructure

```bash
# Khởi động services
docker compose up -d zookeeper kafka

# Xác minh containers
docker compose ps

# Tạo Kafka topic
./setup_kafka_topics.sh
```

### 4️⃣ Chạy Phase 2

```bash
# Terminal 1: Producer
python binance_to_kafka.py

# Terminal 2: Verification
python kafka_consumer_verification.py

# Optional - Terminal 3: Backend
python -m uvicorn main:app --reload
```

## Kiểm Tra Cài Đặt

```bash
# Xác minh Docker
docker compose ps

# Xác minh Kafka topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Xác minh messages
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic binance_trades --from-beginning --max-messages 5
```

## Tài Liệu Chi Tiết

| Tài Liệu | Nội Dung |
|---------|---------|
| **ARCHITECTURE.md** | Kiến trúc chi tiết, data flow, component diagram |
| **PHASE_2_SPECIFICATION.md** | Spec kỹ thuật chuyên sâu (IT-11, 12, 13, 14) |
| **INSTALLATION.md** | Hướng dẫn cài đặt chi tiết từng bước |
| **CONFIGURATION.md** | Danh sách tất cả biến cấu hình môi trường |
| **TROUBLESHOOTING.md** | Giải pháp các vấn đề thường gặp |
| **PERFORMANCE.md** | Benchmark, tối ưu hóa, metrics |
| **RUN.md** | Hướng dẫn chạy nhanh, quick start |
| **CHANGELOG.md** | Lịch sử thay đổi, version notes |

## Giám Sát & Logs

### File Logs
- **Producer:** `binance_producer.log` - Log từ Binance WebSocket Producer
- **Consumer:** `kafka_consumer_verification.log` - Log từ Verification Consumer

### Docker Logs
```bash
docker logs kafka       # Xem logs Kafka
docker logs zookeeper   # Xem logs Zookeeper
```

### Xem Metrics
```bash
# Xem Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Xem topic details
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic binance_trades
```

## Hỗ Trợ & Giao Tiếp

### Tài Liệu
- Xem **TROUBLESHOOTING.md** trước khi liên hệ
- Xem **CONFIGURATION.md** cho các tuỳ chọn
- Xem **PHASE_2_SPECIFICATION.md** cho chi tiết kỹ thuật

### Đội Dự Án
- Xem `mygroup.md` để biết danh sách thành viên
- Xem `PROJECT_STATUS.md` để biết trạng thái hiện tại

## Lộ Trình Phát Triển

```
✅ Phase 1: Infrastructure (Hoàn thành)
🔄 Phase 2: Data Collection (Hiện tại - IT-11, IT-12, IT-13, IT-14)
⏳ Phase 3: Spark Processing (Sắp tới)
⏳ Phase 4: Storage & Distribution (Sắp tới)
⏳ Phase 5: Visualization & API (Sắp tới)

Nâng cấp trong tương lai:
- Hỗ trợ nhiều ký hiệu (Multi-symbol support)
- Dự báo giá sử dụng ML
- Tín hiệu giao dịch tự động
- Phân tích nâng cao
```

## Liên Kết Tài Liệu

- [➜ Cài Đặt Chi Tiết](INSTALLATION.md)
- [➜ Kiến Trúc Hệ Thống](ARCHITECTURE.md)
- [➜ Phase 2 Specification](PHASE_2_SPECIFICATION.md)
- [➜ Cấu Hình Môi Trường](CONFIGURATION.md)
- [➜ Khắc Phục Sự Cố](TROUBLESHOOTING.md)
- [➜ Hiệu Năng & Tuning](PERFORMANCE.md)
- [➜ Quick Start](RUN.md)
- [➜ Lịch Sử Thay Đổi](CHANGELOG.md)

---

**Phiên Bản:** 2.0  
**Trạng Thái:** Phase 2 - In Progress  
**Cập Nhật:** April 7, 2026  
**Lizzie License** - Xem LICENSE file để biết chi tiết
