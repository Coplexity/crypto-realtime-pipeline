// ===== Trade Data Types =====

export interface RawTrade {
  symbol: string;
  trade_id: number;
  price: number;
  quantity: number;
  buyer_order_id: number;
  seller_order_id: number;
  trade_time: number;
  is_buyer_maker: boolean;
  is_best_match: boolean;
  ingestion_timestamp: number;
}

export interface WebSocketMessage {
  type: 'trade';
  data: RawTrade;
  timestamp: number;
}

export interface TradeDisplay {
  id: number;
  price: number;
  quantity: number;
  total: number;
  time: string;
  side: 'buy' | 'sell';
  timestamp: number;
}

// ===== OHLCV Candlestick =====

export interface CandleData {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

// ===== Technical Indicators =====

export interface RSIData {
  value: number;
  timestamp: number;
}

export interface VWAPData {
  value: number;
  timestamp: number;
}

export interface PredictionResult {
  direction: 'up' | 'down' | 'neutral';
  confidence: number;
  predicted_price: number;
  timestamp: number;
  model: string;
}

// ===== System Status =====

export interface HealthStatus {
  status: string;
  kafka_configured: boolean;
  kafka_topic: string;
}

export interface KafkaMetrics {
  topic: string;
  partitions: number;
  messages_per_second: number;
  consumer_lag: number;
  total_messages: number;
}

export interface SparkJob {
  id: string;
  name: string;
  status: 'running' | 'completed' | 'failed';
  start_time: string;
  duration: string;
  records_processed: number;
}

export interface SystemMetrics {
  uptime: string;
  total_trades_processed: number;
  messages_per_second: number;
  avg_latency_ms: number;
  error_rate: number;
  active_connections: number;
}

// ===== WebSocket State =====

export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected' | 'reconnecting';

export interface WebSocketState {
  status: ConnectionStatus;
  lastMessage: WebSocketMessage | null;
  error: string | null;
  reconnectAttempt: number;
}

// ===== Dashboard Stats =====

export interface DashboardStats {
  currentPrice: number;
  priceChange24h: number;
  priceChangePercent24h: number;
  high24h: number;
  low24h: number;
  volume24h: number;
  tradeCount24h: number;
  avgPrice1m: number;
}

// ===== Chart Timeframe =====

export type Timeframe = '1m' | '5m' | '15m' | '1h' | '4h' | '1d';

export interface TimeframeOption {
  label: string;
  value: Timeframe;
  seconds: number;
}
