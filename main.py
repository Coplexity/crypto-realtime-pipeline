"""
FastAPI Backend - Phase 5 Placeholder

REST API + WebSocket endpoint để truyền dữ liệu real-time tới frontend.
Kết nối tới Kafka consumer group để lấy dữ liệu trade mới nhất.

Features:
- REST API cho historical data
- WebSocket endpoints cho real-time updates
- Trade data streaming tới multiple clients
- Error handling và logging
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any

from aiokafka import AIOKafkaConsumer
from dotenv import dotenv_values, load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

app = FastAPI(
    title="Crypto Realtime Pipeline API",
    description="REST API + WebSocket for real-time crypto data",
    version="2.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_config() -> Dict[str, Optional[str]]:
    """Load configuration from .env file."""
    dotenv_config = dotenv_values(BASE_DIR / ".env")
    
    kafka_topic = (
        os.getenv("KAFKA_TOPIC_NAME") 
        or dotenv_config.get("KAFKA_TOPIC_NAME")
        or "binance_trades"
    )
    
    kafka_servers = (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        or dotenv_config.get("KAFKA_BOOTSTRAP_SERVERS")
        or "localhost:29092"
    )
    
    return {
        "kafka_topic": kafka_topic,
        "kafka_servers": kafka_servers,
    }


@app.get("/")
async def read_root():
    """Health check endpoint."""
    return {
        "status": "OK",
        "service": "Crypto Realtime Pipeline API",
        "version": "2.0",
    }


@app.get("/health")
async def health_check():
    """Detailed health check."""
    config = _get_config()
    return {
        "status": "healthy",
        "kafka_configured": all(config.values()),
        "kafka_topic": config["kafka_topic"],
    }


@app.websocket("/ws/crypto")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint để stream dữ liệu trade real-time.
    
    Clients kết nối tới endpoint này để nhận dữ liệu trade
    từ Kafka topic theo real-time.
    """
    await websocket.accept()
    
    config = _get_config()
    kafka_topic = config["kafka_topic"]
    kafka_servers = config["kafka_servers"]
    
    logger.info(f"WebSocket client connected. Topic: {kafka_topic}")
    
    if not kafka_topic or not kafka_servers:
        await websocket.send_json({
            "error": "Missing Kafka config",
            "required": ["KAFKA_TOPIC_NAME", "KAFKA_BOOTSTRAP_SERVERS"]
        })
        await websocket.close(code=1011)
        return
    
    try:
        # Create Kafka consumer
        consumer = AIOKafkaConsumer(
            kafka_topic,
            bootstrap_servers=[kafka_servers],
            group_id="websocket-client-group",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        )
        
        await consumer.start()
        logger.info(f"Kafka consumer started for {kafka_topic}")
        
        try:
            async for message in consumer:
                # Send trade data to client
                trade_data = {
                    "type": "trade",
                    "data": message.value,
                    "timestamp": message.timestamp,
                }
                
                await websocket.send_json(trade_data)
        
        except WebSocketDisconnect:
            logger.info("WebSocket client disconnected")
        
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            await websocket.send_json({"error": str(e)})
        
        finally:
            await consumer.stop()
            logger.info("Kafka consumer stopped")
    
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        await websocket.send_json({"error": "Failed to connect to Kafka"})
        await websocket.close(code=1011)


@app.get("/api/trades/latest")
async def get_latest_trades(limit: int = 10):
    """
    Get latest trades (placeholder for Phase 4+).
    
    Phase 4 sẽ lấy dữ liệu từ MongoDB database.
    """
    return {
        "status": "placeholder",
        "message": "Phase 4: Implement database layer",
        "limit": limit,
    }


@app.get("/api/trades/{symbol}")
async def get_trades_by_symbol(symbol: str, limit: int = 100):
    """
    Get trades for specific symbol (placeholder for Phase 4+).
    """
    return {
        "status": "placeholder",
        "symbol": symbol,
        "limit": limit,
        "message": "Phase 4: Implement database layer",
    }


@app.get("/api/ohlcv/{symbol}")
async def get_ohlcv(symbol: str, timeframe: str = "1m"):
    """
    Get OHLCV data (placeholder for Phase 3+).
    """
    return {
        "status": "placeholder",
        "symbol": symbol,
        "timeframe": timeframe,
        "message": "Phase 3: Implement Spark processor",
    }


if __name__ == "__main__":
    import uvicorn
    
    config = _get_config()
    logger.info(f"Starting API server...")
    logger.info(f"Kafka Topic: {config['kafka_topic']}")
    logger.info(f"Kafka Servers: {config['kafka_servers']}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )
        kafka_topic,
        bootstrap_servers=kafka_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    try:
        await consumer.start()
        async for msg in consumer:
            await websocket.send_json(msg.value)
    except Exception as e:
        print(f"Loi ket noi: {e}")
        await websocket.close(code=1011)
    finally:
        await consumer.stop()