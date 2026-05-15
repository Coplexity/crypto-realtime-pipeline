import asyncio
import json
import contextlib
from collections import defaultdict
from typing import Dict, Set, Optional
from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaConsumer
from fastapi import WebSocket, WebSocketDisconnect

class SharedKafkaManager:
    def __init__(self, kafka_bootstrap: str):
        self.kafka_bootstrap = kafka_bootstrap
        self.subscribers: Dict[str, Set[WebSocket]] = defaultdict(set)
        self.consumers: Dict[str, KafkaConsumer] = {}
        self.streaming_tasks: Dict[str, asyncio.Task] = {}
        self.executor = ThreadPoolExecutor(max_workers=3)
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._running: Dict[str, bool] = {}
    
    # [FIX] Thêm interval vào key để phân loại luồng dữ liệu
    async def subscribe(self, topic: str, symbol: str, websocket: WebSocket, interval: str = None):
        key = f"{topic}:{symbol}:{interval}" if interval else f"{topic}:{symbol}"
        async with self._locks[key]:
            self.subscribers[key].add(websocket)
            if key not in self.consumers:
                await self._start_consumer(topic, symbol, key, interval)
    
    async def unsubscribe(self, topic: str, symbol: str, websocket: WebSocket, interval: str = None):
        key = f"{topic}:{symbol}:{interval}" if interval else f"{topic}:{symbol}"
        async with self._locks[key]:
            self.subscribers[key].discard(websocket)
            if not self.subscribers[key] and key in self.consumers:
                await self._stop_consumer(key)
    
    async def _start_consumer(self, topic: str, symbol: str, key: str, interval: str = None):
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.kafka_bootstrap,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.consumers[key] = consumer
        self._running[key] = True
        task = asyncio.create_task(self._stream_messages(topic, symbol, key, interval))
        self.streaming_tasks[key] = task
    
    async def _stream_messages(self, topic: str, symbol: str, key: str, interval: str = None):
        consumer = self.consumers[key]
        running = True
        
        def poll_messages():
            messages = []
            try:
                msg_pack = consumer.poll(timeout_ms=1000)
                for topic_partition, msgs in msg_pack.items():
                    for msg in msgs:
                        messages.append(msg.value)
            except Exception as e:
                pass
            return messages
        
        try:
            while running and self._running.get(key, False):
                try:
                    messages = await asyncio.get_event_loop().run_in_executor(
                        self.executor, poll_messages
                    )
                    for msg in messages:
                        if not msg: continue
                        
                        # [FIX] Lấy đúng trường "symbol" thay vì "s"
                        msg_symbol = msg.get("symbol", msg.get("s", ""))
                        if msg_symbol != symbol:
                            continue
                            
                        # [FIX] Lọc chính xác theo khung giờ (interval)
                        if interval:
                            msg_interval = msg.get("interval", msg.get("i", ""))
                            if msg_interval and msg_interval != interval:
                                continue
                        
                        formatted_msg = self._format_message(topic, symbol, msg)
                        if not formatted_msg: continue
                        
                        disconnected = set()
                        async with self._locks[key]:
                            subscribers_copy = list(self.subscribers[key])
                        
                        for ws in subscribers_copy:
                            try:
                                await ws.send_json(formatted_msg)
                            except (RuntimeError, WebSocketDisconnect):
                                disconnected.add(ws)
                        
                        if disconnected:
                            async with self._locks[key]:
                                for ws in disconnected:
                                    self.subscribers[key].discard(ws)
                                if not self.subscribers[key]:
                                    running = False
                                    break
                    await asyncio.sleep(0.1)
                except asyncio.CancelledError:
                    break
                except Exception:
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
    
    def _format_message(self, topic: str, symbol: str, msg: dict) -> Optional[dict]:
        if "kline" in topic.lower() or "candle" in topic.lower():
            # [FIX] Đọc đúng các key chuẩn từ producer.py
            open_time = msg.get("open_time", msg.get("t"))
            is_closed = msg.get("is_closed", msg.get("x", False))
            
            candle_data = {
                "symbol": symbol,
                "interval": msg.get("interval", msg.get("i", "1m")),
                "openTime": open_time,
                "closeTime": msg.get("close_time", msg.get("T")),
                "open": float(msg.get("open", msg.get("o", 0))),
                "high": float(msg.get("high", msg.get("h", 0))),
                "low": float(msg.get("low", msg.get("l", 0))),
                "close": float(msg.get("close", msg.get("c", 0))),
                "volume": float(msg.get("volume", msg.get("v", 0))),
                "quoteVolume": float(msg.get("quote_volume", msg.get("q", 0))),
                "trades": int(msg.get("trades_count", msg.get("n", 0))),
                "x": is_closed,
            }
            return {
                "type": "update" if not is_closed else "realtime",
                "candle": candle_data
            }
        
        elif "orderbook" in topic.lower():
            bids_raw = msg.get("bids", [])
            asks_raw = msg.get("asks", [])
            if not bids_raw and not asks_raw: return None
            # ... (Phần Orderbook giữ nguyên như cũ của bạn)
            return {"type": "update", "symbol": symbol, "bids": bids_raw, "asks": asks_raw, "timestamp": msg.get("received_at")}
            
        elif "trades" in topic.lower():
            trade = {
                "symbol": symbol,
                "price": float(msg.get("price", msg.get("p", 0))),
                "quantity": float(msg.get("quantity", msg.get("q", 0))),
                "time": msg.get("trade_time", msg.get("T", 0)),
                "isBuyerMaker": msg.get("is_buyer_maker", msg.get("m", False)),
                "tradeId": msg.get("trade_id", msg.get("t"))
            }
            return {"type": "realtime", "trade": trade}
        return None

    async def _stop_consumer(self, key: str):
        self._running[key] = False
        if key in self.streaming_tasks:
            task = self.streaming_tasks[key]
            if not task.done(): task.cancel()
            del self.streaming_tasks[key]
        if key in self.consumers:
            self.consumers[key].close()
            del self.consumers[key]

    async def shutdown(self):
        keys = list(self.consumers.keys())
        for key in keys: await self._stop_consumer(key)
        self.executor.shutdown(wait=True)