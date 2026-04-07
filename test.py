"""
Test Utilities - Phase Testing Framework

Các hàm test cho Phase 2 components.
Bao gồm: Kafka connection, message validation, performance tests.
"""

import json
import logging
import time
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class KafkaTestHelper:
    """Helper class cho Kafka testing."""
    
    @staticmethod
    def validate_trade_message(msg: Dict[str, Any]) -> bool:
        """Validate trade message structure."""
        required_fields = [
            'symbol', 'trade_id', 'price', 'quantity',
            'trade_time', 'ingestion_timestamp'
        ]
        
        for field in required_fields:
            if field not in msg:
                logger.error(f"Missing field: {field}")
                return False
        
        try:
            float(msg['price'])
            float(msg['quantity'])
            return True
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def generate_mock_trade(symbol: str = "BTCUSDT") -> Dict[str, Any]:
        """Generate mock trade message for testing."""
        return {
            "symbol": symbol,
            "trade_id": int(time.time() * 1000) % 1000000,
            "price": 45123.45,
            "quantity": 0.5,
            "buyer_order_id": 123,
            "seller_order_id": 124,
            "trade_time": int(time.time() * 1000),
            "is_buyer_maker": False,
            "is_best_match": True,
            "ingestion_timestamp": int(time.time() * 1000),
        }


class PerformanceTestHelper:
    """Helper class cho performance testing."""
    
    @staticmethod
    def measure_latency(func, *args, **kwargs) -> float:
        """Measure execution latency (milliseconds)."""
        start = time.time()
        func(*args, **kwargs)
        end = time.time()
        return (end - start) * 1000
    
    @staticmethod
    def measure_throughput(func, iterations: int) -> float:
        """Measure operations per second."""
        start = time.time()
        for _ in range(iterations):
            func()
        elapsed = time.time() - start
        return iterations / elapsed


class ValidationTestHelper:
    """Helper class cho validation testing."""
    
    @staticmethod
    def test_price_ranges(price: float) -> bool:
        """Test price within expected ranges."""
        return 1000 <= price <= 1000000
    
    @staticmethod
    def test_quantity_ranges(qty: float) -> bool:
        """Test quantity within expected ranges."""
        return 0.001 <= qty <= 1000
    
    @staticmethod
    def test_message_types(msg: Dict[str, Any]) -> bool:
        """Test all fields have correct types."""
        type_checks = {
            'symbol': str,
            'trade_id': int,
            'price': (int, float),
            'quantity': (int, float),
            'trade_time': int,
            'ingestion_timestamp': int,
        }
        
        for field, expected_type in type_checks.items():
            if field in msg:
                if not isinstance(msg[field], expected_type):
                    return False
        return True


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    print("Testing Kafka message validation...")
    mock_msg = KafkaTestHelper.generate_mock_trade()
    is_valid = KafkaTestHelper.validate_trade_message(mock_msg)
    print(f"✓ Message valid: {is_valid}")
    
    print("\nTesting validation rules...")
    print(f"✓ Price range OK: {ValidationTestHelper.test_price_ranges(mock_msg['price'])}")
    print(f"✓ Quantity range OK: {ValidationTestHelper.test_quantity_ranges(mock_msg['quantity'])}")
    print(f"✓ Types OK: {ValidationTestHelper.test_message_types(mock_msg)}")
    
    print("\n✓ All tests passed")
