# 📊 Frontend Dashboard Service

Frontend của hệ thống Crypto Analytics Platform được xây dựng bằng **Next.js + React + TypeScript** nhằm cung cấp dashboard realtime cho việc theo dõi thị trường cryptocurrency.

Dashboard hiển thị:

- Realtime market data
- Candlestick charts
- Orderbook
- Trade streams
- Market statistics
- ML predictions
- Pipeline status

Frontend giao tiếp với Backend thông qua:

- REST APIs
- WebSocket realtime streaming

---

# 🏗️ Kiến trúc Frontend

```text
FastAPI Backend
        ↓
REST APIs + WebSocket
        ↓
Next.js Frontend
        ↓
Realtime Dashboard
```

---

# ⚙️ Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Framework | Next.js |
| UI Library | React |
| Language | TypeScript |
| Styling | CSS |
| Communication | WebSocket + REST API |
| Charts | Recharts / Chart Libraries |
| Runtime | Node.js |

---

# 📂 Cấu trúc thư mục

```text
frontend/
├── Dockerfile
├── next.config.js
├── package.json
├── public/
├── src/
│   ├── app/
│   │   ├── globals.css
│   │   ├── layout.tsx
│   │   └── page.tsx
│   │
│   ├── components/
│   │   ├── CandleChart.tsx
│   │   ├── MLPredictions.tsx
│   │   ├── MarketStats.tsx
│   │   ├── OrderBook.tsx
│   │   ├── PipelineStatus.tsx
│   │   ├── TickerBar.tsx
│   │   ├── TopGainers.tsx
│   │   └── TradeList.tsx
│   │
│   ├── hooks/
│   │   ├── useOrderBookWS.ts
│   │   ├── useTradesWS.ts
│   │   └── useWebSocket.ts
│   │
│   └── lib/
│       └── api.ts
│
└── tsconfig.json
```

---

# 🚀 Chức năng chính

## 📈 Candlestick Chart

Hiển thị biểu đồ nến realtime của cryptocurrency.

### Hỗ trợ:

- OHLC visualization
- Multi timeframe
- Historical candles
- Realtime updates

---

## 📖 Order Book

Hiển thị:

- Bid / Ask realtime
- Market depth
- Live order updates

Dữ liệu được stream trực tiếp qua WebSocket.

---

## 💹 Trade Stream

Hiển thị:

- Recent trades
- Trade price
- Trade volume
- Buy/Sell direction

---

## 📊 Market Statistics

Dashboard hỗ trợ:

- Market cap
- Price changes
- Volume statistics
- Top gainers
- Top losers

---

## 🤖 Machine Learning Predictions

Hiển thị:

- Predicted prices
- Prediction trends
- ML confidence
- AI forecasting results

Prediction data được lấy từ Spark ML pipeline.

---

## ⚙️ Pipeline Monitoring

Hiển thị trạng thái:

- Kafka
- Spark Streaming
- Airflow
- Backend API
- WebSocket connectivity

---

# 🧩 Các Components chính

| Component | Chức năng |
|---|---|
| CandleChart.tsx | Candlestick chart |
| OrderBook.tsx | Orderbook realtime |
| TradeList.tsx | Trade stream |
| TopGainers.tsx | Top market movers |
| MarketStats.tsx | Market statistics |
| MLPredictions.tsx | ML predictions |
| PipelineStatus.tsx | Pipeline health |
| TickerBar.tsx | Live ticker updates |

---

# 🔌 WebSocket Integration

Frontend sử dụng WebSocket để nhận dữ liệu realtime với độ trễ thấp.

---

## Luồng hoạt động

```text
FastAPI WebSocket
        ↓
Custom React Hooks
        ↓
React Components
        ↓
Realtime UI Updates
```

---

## Custom Hooks

| Hook | Vai trò |
|---|---|
| useWebSocket.ts | WebSocket base connection |
| useTradesWS.ts | Trade stream |
| useOrderBookWS.ts | Orderbook stream |

---

# 🌐 REST API Integration

Frontend giao tiếp với Backend thông qua:

```text
src/lib/api.ts
```

---

## Các loại dữ liệu được fetch

- Historical candles
- Market statistics
- Predictions
- Tickers
- Pipeline status

---

# 📡 Realtime Data Flow

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
WebSocket
    ↓
Next.js Dashboard
```

---

# 🎨 UI Design

Dashboard được thiết kế theo phong cách:

- Financial trading platform
- Realtime analytics
- Dark mode friendly
- Responsive layout
- Live updating widgets

---

# 📱 Responsive Design

Frontend hỗ trợ:

- Desktop
- Tablet
- Mobile responsive layout

---

# 🐳 Docker Deployment

## Build Docker Image

```bash
docker build -t crypto-frontend:v1 .
```

---

## Run Container

```bash
docker run -p 3000:3000 crypto-frontend:v1
```

---

# ☸️ Kubernetes Deployment

Frontend được triển khai trên Kubernetes thông qua:

```text
k8s/orchestration/frontend.yaml
```

---

## Deployment Architecture

```text
Kubernetes Deployment
        ↓
Next.js Pods
        ↓
ClusterIP Service
        ↓
Ingress / Port Forward
```

---

# 🚀 Chạy local development

## Cài dependencies

```bash
npm install
```

---

## Chạy development server

```bash
npm run dev
```

---

## Truy cập ứng dụng

```text
http://localhost:3000
```

---

# 📈 Monitoring

Frontend hỗ trợ:

- WebSocket connection monitoring
- API latency checking
- Pipeline health display
- Backend connectivity checking

---

# 🧪 Testing

## Build production

```bash
npm run build
```

---

## Start production server

```bash
npm run start
```

---

# 🔒 Environment Variables

Ví dụ:

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000
```

---

# 📚 Vai trò trong Lambda Architecture

Frontend đóng vai trò:

- Visualization Layer
- Realtime Analytics Dashboard
- User Interaction Layer
- Monitoring Interface

---

# 🖼️ Dashboard Features

## Trading Dashboard

- Candlestick charts
- Realtime ticker
- Market depth
- Trades stream

---

## Analytics Dashboard

- Market statistics
- Top movers
- Historical analysis
- Prediction analytics

---

## ML Dashboard

- Price forecasting
- Prediction trends
- AI-generated insights

---

# ⚠️ Lưu ý

- Frontend phụ thuộc Backend WebSocket APIs
- Dashboard yêu cầu kết nối realtime liên tục
- Nếu WebSocket mất kết nối, dữ liệu realtime sẽ không cập nhật

---

# 🔮 Hướng phát triển

- Advanced charting tools
- Technical indicators
- Multi-symbol support
- Authentication system
- Portfolio management
- Trading simulation
- Theme customization
- Performance optimization
- WebSocket reconnection strategy
- PWA support