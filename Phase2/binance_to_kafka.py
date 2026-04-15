"""
IT-12 & IT-13: Binance WebSocket → Kafka Producer (Multi-Pair Edition)

Kết nối trực tiếp tới Binance WebSocket API để thu thập dữ liệu trade real-time
cho TẤT CẢ các cặp giao dịch với độ tin cậy cao (acks=all), xử lý lỗi tự động,
và logging chi tiết. Hỗ trợ dynamic pair discovery và multistream WebSocket.

Tính năng:
- Multistream WebSocket real-time từ Binance API
- Dynamic pair discovery từ REST API (lọc theo volume)
- Đẩy dữ liệu vào Kafka topic với nén Snappy
- Xử lý tự động khi mất kết nối (exponential backoff)
- Tracking thống kê: message count, error rate per pair
- Scheduled pair list refresh
- Structured logging: file + console
"""

import json
import logging
import time
import os
from datetime import datetime
from typing import Optional, Dict, Any, List, Set
import websocket
import requests
from urllib.parse import urlencode
from threading import Thread

from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv


class BinanceKafkaProducer:
    """Class tối ưu cho Binance WebSocket → Multi-Pair Kafka Producer."""
    
    # Constants
    BINANCE_REST_API: str = "https://api.binance.com/api/v3"
    BINANCE_WS_API: str = "wss://stream.binance.com:9443"
    MAX_RECONNECT_ATTEMPTS: int = 10
    RECONNECT_BASE_DELAY: float = 1.0
    RECONNECT_MAX_DELAY: float = 300.0
    WS_MULTISTREAM_LIMIT: int = 200  # Max streams per WebSocket connection
    
    def __init__(self):
        """Khởi tạo producer với cấu hình từ environment."""
        load_dotenv()
        
        self.kafka_servers: str = os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'
        )
        self.topic: str = os.getenv('KAFKA_TOPIC_NAME', 'binance_trades')
        
        # Pair configuration
        self.pair_filter: str = os.getenv('BINANCE_PAIR_FILTER', 'hot')
        self.pair_min_volume: float = float(os.getenv('BINANCE_PAIR_MIN_VOLUME', '100000'))
        self.pair_whitelist: Set[str] = set(
            p.lower() for p in os.getenv('BINANCE_PAIR_WHITELIST', '').split(',')
            if p.strip()
        )
        self.pair_update_interval: int = int(os.getenv('BINANCE_PAIR_UPDATE_INTERVAL', '3600'))
        
        self.producer: Optional[KafkaProducer] = None
        self.active_pairs: Set[str] = set()
        self.message_count: int = 0
        self.error_count: int = 0
        self.pair_stats: Dict[str, Dict[str, Any]] = {}
        
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
    
    def _setup_logging(self) -> None:
        """Cấu hình structured logging: file + console."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - [%(levelname)s] - %(message)s',
            handlers=[
                logging.FileHandler('binance_producer.log'),
                logging.StreamHandler()
            ]
        )
    
    def _fetch_traded_pairs(self) -> Set[str]:
        """
        Lấy danh sách các cặp giao dịch từ Binance API.
        Filter theo volume hoặc whitelist.
        
        Returns:
            Set of trading pair symbols (lowercase, e.g., 'btcusdt')
        """
        try:
            self.logger.info("Fetching trading pairs từ Binance API...")
            
            if self.pair_filter == 'whitelist' and self.pair_whitelist:
                self.logger.info(f"Using whitelist: {self.pair_whitelist}")
                return self.pair_whitelist
            
            # Get all trading pairs
            url = f"{self.BINANCE_REST_API}/ticker/24hr"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            tickers = response.json()
            pairs = set()
            
            # Filter USDT pairs by volume
            for ticker in tickers:
                symbol = ticker.get('symbol', '').lower()
                
                # Only USDT pairs for now
                if not symbol.endswith('usdt'):
                    continue
                
                # Get notional volume (price * quantity)
                try:
                    volume = float(ticker.get('quoteVolume', 0))
                    if volume >= self.pair_min_volume:
                        pairs.add(symbol)
                except (ValueError, TypeError):
                    continue
            
            self.logger.info(f"Found {len(pairs)} trading pairs with volume > {self.pair_min_volume}")
            return pairs
        
        except Exception as e:
            self.logger.error(f"Error fetching pairs: {e}")
            # Fallback to default pairs
            return {'btcusdt', 'ethusdt', 'bnbusdt', 'adausdt', 'xrpusdt'}
    
    def _build_multistream_url(self, pairs: Set[str]) -> str:
        """
        Build Binance WebSocket multistream URL.
        Limit to WS_MULTISTREAM_LIMIT to avoid connection issues.
        
        Args:
            pairs: Set of trading pair symbols
            
        Returns:
            WebSocket endpoint URL
        """
        # Limit to max streams per connection
        pairs_list = sorted(list(pairs))[:self.WS_MULTISTREAM_LIMIT]
        
        # Create stream names (symbol@trade format)
        streams = [f"{symbol}@trade" for symbol in pairs_list]
        
        # Build URL with encoded streams
        stream_param = '/'.join(streams)
        url = f"{self.BINANCE_WS_API}/stream?streams={stream_param}"
        
        self.logger.info(f"WebSocket URL prepared for {len(streams)} pairs")
        return url
    
    def _init_producer(self) -> KafkaProducer:
        """
        Khởi tạo Kafka Producer với cấu hình tối ưu.
        """
        self.logger.info(f"Khởi tạo Kafka Producer: {self.kafka_servers}")
        
        try:
            producer = KafkaProducer(
                bootstrap_servers=[self.kafka_servers],
                value_serializer=self._serialize_message,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='snappy',
                request_timeout_ms=10000,
                connections_max_idle_ms=540000,
            )
            self.logger.info("✓ Kafka Producer khởi tạo thành công")
            return producer
        except Exception as e:
            self.logger.error(f"✗ Lỗi khởi tạo Kafka Producer: {e}")
            raise
    
    @staticmethod
    def _serialize_message(msg: Dict[str, Any]) -> bytes:
        """Serialize message thành JSON bytes."""
        return json.dumps(msg).encode('utf-8')
    
    def _extract_trade_data(self, stream_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Trích xuất trade data từ Binance WebSocket message (wrapped format).
        
        Args:
            stream_data: Wrapper data từ multistream endpoint
            
        Returns:
            Processed trade data dict, or None if invalid
        """
        try:
            # Multistream sends wrapped data
            if 'data' in stream_data:
                raw_data = stream_data['data']
            else:
                raw_data = stream_data
            
            # Validate trade message
            if raw_data.get('e') != 'trade':
                return None
            
            return {
                "symbol": raw_data.get("s", "").lower(),
                "trade_id": raw_data.get("t"),
                "price": float(raw_data.get("p", 0)),
                "quantity": float(raw_data.get("q", 0)),
                "buyer_order_id": raw_data.get("b"),
                "seller_order_id": raw_data.get("a"),
                "trade_time": raw_data.get("T"),
                "is_buyer_maker": raw_data.get("m", False),
                "is_best_match": raw_data.get("M", False),
                "ingestion_timestamp": int(datetime.now().timestamp() * 1000),
            }
        except Exception as e:
            self.logger.error(f"Error extracting trade data: {e}")
            return None
    
    def _on_send_success(self, metadata) -> None:
        """Callback gửi thành công."""
        self.message_count += 1
        symbol = "unknown"
        
        # Track stats per pair
        if self.message_count % 100 == 0:
            self.logger.info(
                f"✓ Gửi {self.message_count} messages "
                f"(Partition: {metadata.partition}, Offset: {metadata.offset})"
            )
    
    def _on_send_error(self, exc: Exception) -> None:
        """Callback gửi thất bại."""
        self.error_count += 1
        self.logger.error(f"✗ Lỗi gửi Kafka #{self.error_count}: {exc}")
    
    def _on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        """Handle message từ WebSocket (multistream)."""
        try:
            data = json.loads(message)
            trade_data = self._extract_trade_data(data)
            
            if trade_data is not None:
                # Track per-pair stats
                symbol = trade_data['symbol']
                if symbol not in self.pair_stats:
                    self.pair_stats[symbol] = {'count': 0, 'last_price': 0}
                
                self.pair_stats[symbol]['count'] += 1
                self.pair_stats[symbol]['last_price'] = trade_data['price']
                
                # Send to Kafka
                future = self.producer.send(self.topic, key=symbol.encode('utf-8'), value=trade_data)
                future.add_callback(self._on_send_success)
                future.add_errback(self._on_send_error)
        
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            self.logger.error(f"✗ Lỗi parse message: {e}")
    
    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        """Handle WebSocket error."""
        self.logger.error(f"✗ WebSocket error: {error}")
    
    def _on_close(
        self,
        ws: websocket.WebSocketApp,
        code: int,
        msg: str
    ) -> None:
        """Handle WebSocket close."""
        self.logger.warning(f"✗ WebSocket closed (Code: {code}, Msg: {msg})")
        
        # Display pair statistics
        if self.pair_stats:
            self.logger.info("=" * 70)
            self.logger.info("PAIR STATISTICS")
            self.logger.info("=" * 70)
            total = 0
            for symbol, stats in sorted(self.pair_stats.items()):
                self.logger.info(
                    f"  {symbol:12s} | Messages: {stats['count']:8d} | "
                    f"Last Price: ${stats['last_price']:12.2f}"
                )
                total += stats['count']
            self.logger.info(f"  Total Messages: {total}")
            self.logger.info("=" * 70)
    
    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        """Handle WebSocket open."""
        self.logger.info(
            f"✓ WebSocket kết nối thành công\n"
            f"  Pairs: {len(self.active_pairs)} symbols\n"
            f"  Kafka Topic: {self.topic}\n"
            f"  Kafka Servers: {self.kafka_servers}"
        )
        
        # Display pair list
        if self.active_pairs:
            pairs_list = sorted(list(self.active_pairs))
            self.logger.info(f"  Monitoring: {', '.join(pairs_list[:10])}" + 
                           (f"... and {len(pairs_list)-10} more" if len(pairs_list) > 10 else ""))
    
    def _refresh_pairs_thread(self) -> None:
        """Background thread để refresh danh sách pairs."""
        while True:
            try:
                time.sleep(self.pair_update_interval)
                new_pairs = self._fetch_traded_pairs()
                
                if new_pairs != self.active_pairs:
                    self.logger.info(f"Pair list updated: {len(new_pairs)} pairs")
                    self.active_pairs = new_pairs
                    
            except Exception as e:
                self.logger.error(f"Error refreshing pairs: {e}")
    
    def run(self) -> None:
        """Khởi chạy producer với reconnect logic."""
        self.logger.info("=" * 70)
        self.logger.info("PHASE 2: Multi-Pair Data Collection (IT-12 & IT-13)")
        self.logger.info("=" * 70)
        
        try:
            self.producer = self._init_producer()
        except Exception:
            self.logger.error("Không thể khởi tạo producer. Dừng.")
            return
        
        # Fetch initial pair list
        self.active_pairs = self._fetch_traded_pairs()
        if not self.active_pairs:
            self.logger.error("No trading pairs found. Dừng.")
            return
        
        # Start background refresh thread (optional)
        if self.pair_update_interval > 0:
            refresh_thread = Thread(target=self._refresh_pairs_thread, daemon=True)
            refresh_thread.start()
        
        reconnect_attempts = 0
        
        while True:
            try:
                reconnect_attempts += 1
                self.logger.info(f"Kết nối #{reconnect_attempts}...")
                
                # Build multistream URL
                ws_url = self._build_multistream_url(self.active_pairs)
                
                ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                
                ws.run_forever(ping_interval=30, ping_timeout=10)
                
                if reconnect_attempts >= self.MAX_RECONNECT_ATTEMPTS:
                    self.logger.error(
                        f"✗ Đã thử {self.MAX_RECONNECT_ATTEMPTS} lần. Dừng."
                    )
                    break
                
                wait_time = min(
                    self.RECONNECT_BASE_DELAY * (2 ** reconnect_attempts),
                    self.RECONNECT_MAX_DELAY,
                )
                self.logger.info(f"Chờ {wait_time}s trước khi reconnect...")
                time.sleep(wait_time)
                
            except KeyboardInterrupt:
                self.logger.info("Nhận tín hiệu dừng (Ctrl+C)")
                break
            except Exception as e:
                self.logger.error(f"✗ Lỗi: {e}")
                reconnect_attempts += 1
                if reconnect_attempts >= self.MAX_RECONNECT_ATTEMPTS:
                    break
                wait_time = min(
                    self.RECONNECT_BASE_DELAY * (2 ** reconnect_attempts),
                    self.RECONNECT_MAX_DELAY,
                )
                time.sleep(wait_time)
        
        self._cleanup()
    
    def _cleanup(self) -> None:
        """Clean up resources."""
        if self.producer:
            self.logger.info("Đóng Kafka Producer...")
            self.producer.flush(timeout=10)
            self.producer.close()
        
        self.logger.info("=" * 70)
        self.logger.info(
            f"Kết thúc. Total Messages: {self.message_count}, Lỗi: {self.error_count}"
        )
        self.logger.info(f"Pairs tracked: {len(self.active_pairs)}")
        self.logger.info("=" * 70)


if __name__ == "__main__":
    producer = BinanceKafkaProducer()
    producer.run()