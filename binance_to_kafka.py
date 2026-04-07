"""
IT-12 & IT-13: Binance WebSocket → Kafka Producer

Kết nối trực tiếp tới Binance WebSocket API để thu thập dữ liệu trade real-time
cho cặp BTC/USDT với độ tin cậy cao (acks=all), xử lý lỗi tự động, và logging chi tiết.

Tính năng:
- StreamWebSocket real-time từ Binance API
- Đẩy dữ liệu vào Kafka topic với nén Snappy
- Xử lý tự động khi mất kết nối (exponential backoff)
- Tracking thống kê: message count, error rate
- Structured logging: file + console
"""

import json
import logging
import time
import os
from datetime import datetime
from typing import Optional, Dict, Any, Callable
import websocket

from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv


class BinanceKafkaProducer:
    """Class tối ưu cho Binance WebSocket → Kafka Producer."""
    
    # Constants
    BINANCE_SYMBOL: str = "btcusdt"
    BINANCE_STREAM_URL: str = f"wss://stream.binance.com:9443/ws/{BINANCE_SYMBOL}@trade"
    MAX_RECONNECT_ATTEMPTS: int = 10
    RECONNECT_BASE_DELAY: float = 1.0
    RECONNECT_MAX_DELAY: float = 300.0
    
    def __init__(self):
        """Khởi tạo producer với cấu hình từ environment."""
        load_dotenv()
        
        self.kafka_servers: str = os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'
        )
        self.topic: str = os.getenv('KAFKA_TOPIC_NAME', 'binance_trades')
        
        self.producer: Optional[KafkaProducer] = None
        self.message_count: int = 0
        self.error_count: int = 0
        
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
    
    def _init_producer(self) -> KafkaProducer:
        """
        Khởi tạo Kafka Producer với cấu hình tối ưu cho độ tin cậy cao.
        
        Returns:
            KafkaProducer: Producer instance
            
        Raises:
            KafkaError: Nếu không kết nối được
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
    
    def _extract_trade_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Trích xuất trade data từ Binance WebSocket message.
        
        Args:
            raw_data: Raw JSON từ Binance stream
            
        Returns:
            Processed trade data dict
        """
        return {
            "symbol": raw_data.get("s", "").upper(),
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
    
    def _on_send_success(self, metadata) -> None:
        """Callback gửi thành công."""
        self.message_count += 1
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
        """Handle message từ WebSocket."""
        try:
            raw_data = json.loads(message)
            trade_data = self._extract_trade_data(raw_data)
            
            future = self.producer.send(self.topic, value=trade_data)
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
    
    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        """Handle WebSocket open."""
        self.logger.info(
            f"✓ WebSocket kết nối thành công\n"
            f"  Stream: {self.BINANCE_STREAM_URL}\n"
            f"  Stream: {self.BINANCE_SYMBOL.upper()}\n"
            f"  Kafka Topic: {self.topic}"
        )
    
    def run(self) -> None:
        """Khởi chạy producer với reconnect logic."""
        self.logger.info("=" * 70)
        self.logger.info("PHASE 2: Thu thập dữ liệu (IT-12 & IT-13)")
        self.logger.info("=" * 70)
        
        try:
            self.producer = self._init_producer()
        except Exception:
            self.logger.error("Không thể khởi tạo producer. Dừng.")
            return
        
        reconnect_attempts = 0
        
        while True:
            try:
                reconnect_attempts += 1
                self.logger.info(f"Kết nối #{reconnect_attempts}...")
                
                ws = websocket.WebSocketApp(
                    self.BINANCE_STREAM_URL,
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
            f"Kết thúc. Messages: {self.message_count}, Lỗi: {self.error_count}"
        )
        self.logger.info("=" * 70)


if __name__ == "__main__":
    producer = BinanceKafkaProducer()
    producer.run()