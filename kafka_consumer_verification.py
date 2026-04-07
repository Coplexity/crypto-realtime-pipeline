"""
IT-14: Kafka Consumer Verification

Consume messages từ Kafka topic (binance_trades) để kiểm tra chất lượng dữ liệu
real-time. Xác minh format, type, ranges, và tracking thống kê chi tiết.

Tính năng:
- Real-time consumption từ Kafka
- Validation: fields, types, value ranges
- Statistics tracking: throughput, latency, error rate
- Structured logging: file + console
- Consumer group offset management
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any, Optional
from collections import defaultdict

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv


class VerificationConsumer:
    """Consumer tối ưu để kiểm tra chất lượng dữ liệu Kafka."""
    
    # Constants
    REQUIRED_FIELDS = ['symbol', 'trade_id', 'price', 'quantity', 'trade_time']
    PRICE_MIN = 1000.0
    PRICE_MAX = 1000000.0
    QUANTITY_MIN = 0.001
    QUANTITY_MAX = 1000.0
    STATS_INTERVAL = 10
    
    def __init__(self):
        """Khởi tạo consumer với cấu hình từ environment."""
        load_dotenv()
        
        self.kafka_servers = os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'
        )
        self.topic = os.getenv('KAFKA_TOPIC_NAME', 'binance_trades')
        self.consumer_group = 'verification-consumer-group'
        
        self.stats: Dict[str, Any] = {
            'messages_received': 0,
            'messages_processed': 0,
            'messages_errors': 0,
            'start_time': None,
            'symbol_count': defaultdict(int),
            'min_price': float('inf'),
            'max_price': 0.0,
            'total_volume': 0.0,
            'last_message_time': None,
        }
        
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
    
    def _setup_logging(self) -> None:
        """Cấu hình structured logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - [%(levelname)s] - %(message)s',
            handlers=[
                logging.FileHandler('kafka_consumer_verification.log'),
                logging.StreamHandler()
            ]
        )
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration to readable string."""
        if seconds < 0:
            return "0s"
        elif seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds / 60:.1f}m"
        else:
            return f"{seconds / 3600:.1f}h"
    
    def _display_statistics(self) -> None:
        """Hiển thị thống kê real-time."""
        self.logger.info("=" * 80)
        self.logger.info("THỐNG KÊ REAL-TIME")
        self.logger.info("=" * 80)
        
        if self.stats['start_time']:
            elapsed = time.time() - self.stats['start_time']
            self.logger.info(f"⏱️  Thời gian chạy: {self._format_duration(elapsed)}")
        
        self.logger.info(f"📨 Messages nhận: {self.stats['messages_received']}")
        self.logger.info(f"✓ Messages xử lý: {self.stats['messages_processed']}")
        self.logger.info(f"✗ Messages lỗi: {self.stats['messages_errors']}")
        
        if self.stats['messages_received'] > 0:
            success_rate = (
                self.stats['messages_processed'] 
                / self.stats['messages_received'] * 100
            )
            self.logger.info(f"✓ Tỷ lệ thành công: {success_rate:.1f}%")
        
        if self.stats['symbol_count']:
            symbols = dict(self.stats['symbol_count'])
            self.logger.info(f"Ký hiệu: {symbols}")
        
        if self.stats['min_price'] != float('inf'):
            self.logger.info(
                f"Giá: Min={self.stats['min_price']:.2f}, "
                f"Max={self.stats['max_price']:.2f}"
            )
        
        self.logger.info(f"Khối lượng: {self.stats['total_volume']:.8f}")
        
        if self.stats['last_message_time']:
            lag = time.time() - (self.stats['last_message_time'] / 1000)
            self.logger.info(f"Độ trễ: {lag:.2f}s")
        
        self.logger.info("=" * 80)
    
    def _verify_message(self, msg: Dict[str, Any]) -> bool:
        """
        Kiểm tra xem message có hợp lệ không.
        
        Args:
            msg: Message từ Kafka
            
        Returns:
            True nếu hợp lệ, False nếu lỗi
        """
        # Check required fields
        for field in self.REQUIRED_FIELDS:
            if field not in msg:
                self.logger.error(f"✗ Thiếu trường: {field}")
                return False
        
        # Check data types
        try:
            price = float(msg['price'])
            quantity = float(msg['quantity'])
            trade_id = int(msg['trade_id'])
        except (ValueError, TypeError) as e:
            self.logger.error(f"✗ Lỗi kiểu dữ liệu: {e}")
            return False
        
        # Check value ranges
        if not (self.PRICE_MIN <= price <= self.PRICE_MAX):
            self.logger.error(f"✗ Giá ngoài range: {price}")
            return False
        
        if not (self.QUANTITY_MIN <= quantity <= self.QUANTITY_MAX):
            self.logger.error(f"✗ Khối lượng ngoài range: {quantity}")
            return False
        
        return True
    
    def _process_message(self, msg: Dict[str, Any]) -> bool:
        """
        Xử lý và cập nhật statistics.
        
        Args:
            msg: Message từ Kafka
            
        Returns:
            True nếu thành công
        """
        try:
            # Verify
            if not self._verify_message(msg):
                self.stats['messages_errors'] += 1
                return False
            
            # Update stats
            self.stats['messages_processed'] += 1
            self.stats['symbol_count'][msg['symbol']] += 1
            self.stats['min_price'] = min(
                self.stats['min_price'], 
                float(msg['price'])
            )
            self.stats['max_price'] = max(
                self.stats['max_price'], 
                float(msg['price'])
            )
            self.stats['total_volume'] += float(msg['quantity'])
            self.stats['last_message_time'] = msg.get(
                'trade_time', 
                int(datetime.now().timestamp() * 1000)
            )
            
            # Display sample messages
            if (self.stats['messages_processed'] <= 5 
                or self.stats['messages_processed'] % 100 == 0):
                self.logger.info(
                    f"[{self.stats['messages_processed']:06d}] "
                    f"Symbol={msg['symbol']:10s} | "
                    f"Price={msg['price']:12.2f} | "
                    f"Qty={msg['quantity']:15.8f}"
                )
            
            return True
        
        except Exception as e:
            self.logger.error(f"✗ Lỗi xử lý: {e}")
            self.stats['messages_errors'] += 1
            return False
    
    def run(self) -> None:
        """Khởi chạy consumer."""
        self.logger.info("=" * 80)
        self.logger.info("PHASE 2: Kafka Consumer Verification (IT-14)")
        self.logger.info("=" * 80)
        
        # Khởi tạo consumer
        try:
            self.logger.info(f"Khởi tạo Consumer:")
            self.logger.info(f"  Servers: {self.kafka_servers}")
            self.logger.info(f"  Topic: {self.topic}")
            self.logger.info(f"  Group: {self.consumer_group}")
            
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=[self.kafka_servers],
                group_id=self.consumer_group,
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                session_timeout_ms=30000,
                consumer_timeout_ms=5000,
            )
            self.logger.info("✓ Consumer khởi tạo thành công")
        
        except Exception as e:
            self.logger.error(f"✗ Lỗi khởi tạo: {e}")
            return
        
        # Consuming loop
        self.stats['start_time'] = time.time()
        last_stats_display = time.time()
        
        self.logger.info("")
        self.logger.info("Đang lắng nghe Kafka Topic...")
        self.logger.info("(Nhấn Ctrl+C để dừng)")
        self.logger.info("")
        
        try:
            for kafka_msg in consumer:
                self.stats['messages_received'] += 1
                self._process_message(kafka_msg.value)
                
                # Display stats periodically
                now = time.time()
                if now - last_stats_display > self.STATS_INTERVAL:
                    self._display_statistics()
                    last_stats_display = now
        
        except KeyboardInterrupt:
            self.logger.info("\nNhận tín hiệu dừng (Ctrl+C)")
        except Exception as e:
            self.logger.error(f"✗ Lỗi: {e}")
        finally:
            self._cleanup(consumer)
    
    def _cleanup(self, consumer: KafkaConsumer) -> None:
        """Clean up resources."""
        self.logger.info("\nĐóng Consumer...")
        consumer.close()
        
        self.logger.info("")
        self._display_statistics()
        
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("✓ Verification Consumer dừng")
        self.logger.info("=" * 80)


if __name__ == "__main__":
    consumer = VerificationConsumer()
    consumer.run()
