import json
import os
from pathlib import Path
from typing import Optional

from aiokafka import AIOKafkaConsumer
from dotenv import dotenv_values, load_dotenv
from fastapi import FastAPI, WebSocket

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
DOTENV_CONFIG = dotenv_values(BASE_DIR / ".env")

app = FastAPI()

def _first_nonempty(*values: object) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _get_kafka_config() -> tuple[Optional[str], Optional[str]]:
    dotenv_config = dotenv_values(BASE_DIR / ".env")
    kafka_topic = _first_nonempty(
        os.getenv("KAFKA_TOPIC_NAME"),
        dotenv_config.get("KAFKA_TOPIC_NAME"),
    )
    kafka_servers = _first_nonempty(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        dotenv_config.get("KAFKA_BOOTSTRAP_SERVERS"),
    )
    return kafka_topic, kafka_servers


@app.websocket("/ws/crypto")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    kafka_topic, kafka_servers = _get_kafka_config()

    if not kafka_topic or not kafka_servers:
        await websocket.send_json(
            {
                "error": "Missing Kafka config. Set KAFKA_TOPIC_NAME and KAFKA_BOOTSTRAP_SERVERS in .env",
            }
        )
        await websocket.close(code=1011)
        return

    consumer = AIOKafkaConsumer(
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