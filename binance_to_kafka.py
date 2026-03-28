import json
import websocket
from kafka import KafkaProducer

# 1. Cấu hình Kafka Producer kết nối vào cổng 29092
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "binance_trades"

def on_message(ws, message):
    raw_data = json.loads(message)
    
    # Lấy các thông số: Symbol, Price, Quantity, Timestamp
    trade_data = {
        "symbol": raw_data.get("s"),
        "price": float(raw_data.get("p")),
        "quantity": float(raw_data.get("q")),
        "timestamp": raw_data.get("T")
    }
    
    # Bắn dữ liệu vào Kafka
    producer.send(TOPIC_NAME, value=trade_data)
    print(f"[REAL-TIME] Đã gửi vào Kafka: {trade_data}")

def on_error(ws, error):
    print(f"Lỗi: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Đã ngắt kết nối.")

def on_open(ws):
    print("🚀 Đã kết nối Binance! Đang hứng dữ liệu BTC/USDT...")

if __name__ == "__main__":
    # Luồng trực tiếp của cặp Bitcoin / Tether
    socket = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    
    ws = websocket.WebSocketApp(socket,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()