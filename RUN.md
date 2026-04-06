# Huong Dan Chay Du An

Tai lieu nay mo ta thu tu chay toan bo he thong: Kafka -> Backend -> Producer -> Frontend.

## 1) Chuan bi

- Cai Docker Desktop
- Cai Python 3.9+
- Cai Node.js 18+

## 2) Clone va cai dat Python

Mo terminal tai thu muc LLXLDL, sau do chay:

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

## 3) Tao file moi truong

Neu chua co file .env thi tao tu mau:

    cp .env.example .env

Noi dung mac dinh:

    KAFKA_BOOTSTRAP_SERVERS=localhost:29092
    KAFKA_TOPIC_NAME=binance_trades
    FASTAPI_PORT=8000

## 4) Chay Docker services (Kafka/Zookeeper)

Tai thu muc LLXLDL:

    docker compose up -d zookeeper kafka

Kiem tra container:

    docker compose ps

## 5) Chay Backend FastAPI

Mo terminal 1:

    cd /Users/macbook/Workspace/LLXLDL
    source .venv/bin/activate
    python -m uvicorn main:app --reload

Backend se chay tai:
- http://127.0.0.1:8000
- Swagger: http://127.0.0.1:8000/docs

## 6) Chay Producer Binance -> Kafka

Mo terminal 2:

    cd /Users/macbook/Workspace/LLXLDL
    source .venv/bin/activate
    python binance_to_kafka.py

Neu on dinh, ban se thay log da gui trade vao Kafka.

## 7) Chay Frontend Next.js

Mo terminal 3:

    cd /Users/macbook/Workspace/LLXLDL/frontend
    npm install
    npm run dev

Mo trinh duyet:
- http://localhost:3001 (hoac port Next.js dang hien thi trong terminal)

## 8) Thu tu kiem tra nhanh khi bi loi

1. Kiem tra backend dang chay tren 8000.
2. Kiem tra producer dang chay va co log trade.
3. Kiem tra file .env trong LLXLDL co day du bien.
4. Kiem tra dang dung dung Python env:

    which python
    python -c "import sys; print(sys.executable)"

Duong dan dung phai la: /Users/macbook/Workspace/LLXLDL/.venv/bin/python

## 9) Dung he thong

- Dung cac process dang chay bang Ctrl + C trong tung terminal.
- Neu muon tat Kafka/Zookeeper:

    docker compose down