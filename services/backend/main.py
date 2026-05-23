import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as redis
from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

from config import settings
from schemas import (
    Candle, LatestKline, OHLCResponse,
    OrderBookResponse, OrderBookEntry, TradesResponse, Trade,
    Prediction, PredictionResponse, PredictionsListResponse,
    PredictionHistory, PredictionHistoryResponse,
    RankingResponse, CoinRanking
)
from kafka_manager import SharedKafkaManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect to MongoDB
    mongo_client = AsyncIOMotorClient(settings.mongo_uri)
    app.state.mongo_client = mongo_client
    app.state.mongo_db = mongo_client[settings.mongo_db]
    
    # Test MongoDB connection
    try:
        await mongo_client.admin.command('ping')
        print(f"✅ Connected to MongoDB: {settings.mongo_db}")
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB: {e}")

    redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
        decode_responses=True,
    )
    app.state.redis = redis_client

    kafka_manager = SharedKafkaManager(kafka_bootstrap=settings.kafka_bootstrap)
    app.state.kafka_manager = kafka_manager

    yield


    await kafka_manager.shutdown()
    await redis_client.aclose()
    mongo_client.close()


app = FastAPI(title=settings.app_name, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def get_mongo(request: Request):
    return request.app.state.mongo_db


async def get_redis(request: Request):
    return request.app.state.redis


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/health")
async def health():
    return {"status": "ok", "service": settings.app_name}


@app.get("/symbols")
async def list_symbols():
    return {"symbols": settings.symbols}

# get from mongodb
@app.get("/ohlc", response_model=OHLCResponse)
async def get_ohlc(
    symbol: str = Query("BTCUSDT", description="Trading pair, e.g. BTCUSDT"),
    interval: Optional[str] = Query(None, description="Interval (matches stored docs)"),
    collection: Optional[str] = Query(None, description="MongoDB collection name (e.g. kline_5m, kline_1h)"),
    limit: int = Query(200, ge=1, le=2000),
    start: Optional[int] = Query(None, description="Start openTime (ms)"),
    end: Optional[int] = Query(None, description="End openTime (ms)"),
    mongo=Depends(get_mongo),
):
    # Determine collection to use
    if collection:
        col_name = collection
    else:
        col_name = settings.mongo_collection_ohlc
        
    # VÀ THAY BẰNG ĐOẠN NÀY:
    if collection:
        col_name = collection
    elif interval:
        col_name = f"ohlc_{interval}" # Tự động nối chuỗi thành ohlc_1m, ohlc_5m...
    else:
        col_name = settings.mongo_collection_ohlc
    
    if not interval:
        if collection:
            # Map collection names to intervals
            collection_to_interval = {
                "5m_kline": "5m",
                "1h_kline": "1h",
                "4h_kline": "4h",
                "1d_kline": "1d",
            }
            interval = collection_to_interval.get(collection, "5m")
        else:
            interval = "5m"
    
    col = mongo[col_name]
    query = {"symbol": symbol, "interval": interval}
    if start is not None and end is not None:
        query["openTime"] = {"$gte": start, "$lte": end}
    elif start is not None:
        query["openTime"] = {"$gte": start}
    elif end is not None:
        query["openTime"] = {"$lte": end}

    cursor = (
        col.find(query)
        .sort("openTime", -1)
        .limit(limit)
    )
    docs = await cursor.to_list(length=limit)
    candles = [Candle(**doc).as_chart_point() for doc in reversed(docs)]

    return OHLCResponse(symbol=symbol, interval=interval, count=len(candles), candles=candles)


@app.get("/latest", response_model=LatestKline)
async def latest_kline(
    symbol: str = Query("BTCUSDT"),
    redis_client=Depends(get_redis),
):
    key = f"crypto:{symbol}:1m:latest"
    raw = await redis_client.get(key)
    if not raw:
        raise HTTPException(status_code=404, detail="No data for symbol")
    data = json.loads(raw)
    return LatestKline(**data)


@app.get("/ohlc/realtime", response_model=OHLCResponse)
async def get_ohlc_realtime(
    symbol: str = Query("BTCUSDT", description="Trading pair, e.g. BTCUSDT"),
    limit: int = Query(200, ge=1, le=2000, description="Number of candles to return"),
    start: Optional[int] = Query(None, description="Start openTime (ms) - load candles before this time"),
    end: Optional[int] = Query(None, description="End openTime (ms)"),
    redis_client=Depends(get_redis),
):
    """
    Get OHLC data from Redis for realtime mode (1m interval).
    Used for lazy-loading historical data when user scrolls/pans backward.
    """
    index_key = f"crypto:{symbol}:1m:index"
    
    try:
        # Get timestamps from sorted set
        if start is not None:
            # Load candles before start time (for backward scrolling)
            # Get all candles before start, then take the N most recent ones
            all_before = await redis_client.zrangebyscore(
                index_key,
                "-inf",
                start - 1,
                withscores=True
            )
            # Sort by score (timestamp) descending to get most recent first
            all_before.sort(key=lambda x: x[1], reverse=True)
            # Take the N most recent candles before start
            timestamps = all_before[:limit]
            # Reverse to get oldest first (for proper chronological order)
            timestamps.reverse()
        elif end is not None:
            # Load candles up to end time
            timestamps = await redis_client.zrangebyscore(
                index_key,
                "-inf",
                end,
                withscores=True,
                start=0,
                num=limit
            )
            timestamps = sorted(timestamps, key=lambda x: x[1])  # Sort ascending
        else:
            # Load latest N candles
            timestamps = await redis_client.zrange(
                index_key,
                -limit,
                -1,
                withscores=True
            )
        
        candles = []
        for ts_str, score in timestamps:
            key = f"crypto:{symbol}:1m:{ts_str}"
            raw = await redis_client.get(key)
            if raw:
                data = json.loads(raw)
                # Only include closed candles
                if data.get("x", False):
                    candles.append({
                        "openTime": data["openTime"],
                        "y": [data["open"], data["high"], data["low"], data["close"]],
                        "volume": data["volume"],
                    })
        
        # Sort by openTime ascending
        candles.sort(key=lambda x: x["openTime"])
        
        return OHLCResponse(
            symbol=symbol,
            interval="1m",
            count=len(candles),
            candles=candles
        )
    except Exception as e:
        print(f"Error loading realtime OHLC from Redis: {e}")
        raise HTTPException(status_code=500, detail=f"Error loading data: {str(e)}")


@app.get("/orderbook", response_model=OrderBookResponse)
async def get_orderbook(
    symbol: str = Query("BTCUSDT", description="Trading pair, e.g. BTCUSDT"),
    limit: int = Query(20, ge=1, le=100, description="Number of levels per side"),
    redis_client=Depends(get_redis),
):
    """Get Order Book snapshot from Redis"""
    key = f"orderbook:{symbol}:latest"
    raw = await redis_client.get(key)
    if not raw:
        raise HTTPException(status_code=404, detail=f"No order book data for {symbol}")
    
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Invalid JSON data in Redis for {symbol}: {str(e)}"
        )
    
    # Process bids and asks, calculate totals, and limit results
    bids_raw = data.get("bids", [])
    asks_raw = data.get("asks", [])
    
    # Validate data format
    if not isinstance(bids_raw, list) or not isinstance(asks_raw, list):
        raise HTTPException(
            status_code=500,
            detail=f"Invalid data format in Redis for {symbol}: bids and asks must be lists"
        )
    
    # Calculate cumulative totals for bids (descending) and asks (ascending)
    bids = []
    bids_total = 0.0
    try:
        for i, level in enumerate(bids_raw[:limit]):
            if not isinstance(level, list) or len(level) < 2:
                continue
            price = float(level[0])
            qty = float(level[1])
            bids_total += qty * price
            bids.append(OrderBookEntry(
                price=price,
                quantity=qty,
                total=bids_total
            ))
    except (ValueError, TypeError, IndexError) as e:
        print(f"Error processing bids for {symbol}: {e}")
        print(f"Bids raw data: {bids_raw[:5] if bids_raw else 'empty'}")
    
    asks = []
    asks_total = 0.0
    try:
        for i, level in enumerate(asks_raw[:limit]):
            if not isinstance(level, list) or len(level) < 2:
                continue
            price = float(level[0])
            qty = float(level[1])
            asks_total += qty * price
            asks.append(OrderBookEntry(
                price=price,
                quantity=qty,
                total=asks_total
            ))
    except (ValueError, TypeError, IndexError) as e:
        print(f"Error processing asks for {symbol}: {e}")
        print(f"Asks raw data: {asks_raw[:5] if asks_raw else 'empty'}")
    
    # Log warning if data is empty
    if len(bids) == 0 and len(asks) == 0:
        print(f"⚠️ Warning: Empty order book data for {symbol} from Redis")
        print(f"Raw data keys: {list(data.keys())}")
        print(f"Bids raw type: {type(bids_raw)}, length: {len(bids_raw) if isinstance(bids_raw, list) else 'N/A'}")
        print(f"Asks raw type: {type(asks_raw)}, length: {len(asks_raw) if isinstance(asks_raw, list) else 'N/A'}")
    
    return OrderBookResponse(
        symbol=symbol,
        bids=bids,
        asks=asks,
        timestamp=data.get("timestamp")
    )


@app.get("/trades", response_model=TradesResponse)
async def get_trades(
    symbol: str = Query("BTCUSDT", description="Trading pair, e.g. BTCUSDT"),
    limit: int = Query(50, ge=1, le=100, description="Number of trades to return"),
    redis_client=Depends(get_redis),
):
    """Get Market Trades from Redis"""
    key = f"trades:{symbol}:list"
    
    # Get trades from sorted set (sorted by time, descending)
    trades_raw = await redis_client.zrange(key, -limit, -1, withscores=False)
    
    if not trades_raw:
        raise HTTPException(status_code=404, detail=f"No trades data for {symbol}")
    
    # Parse and reverse (oldest first)
    trades = []
    for trade_json in reversed(trades_raw):
        trade_data = json.loads(trade_json)
        trades.append(Trade(
            symbol=trade_data.get("symbol", symbol),
            price=trade_data.get("price", 0),
            quantity=trade_data.get("quantity", 0),
            time=trade_data.get("trade_time", 0),
            isBuyerMaker=trade_data.get("isBuyerMaker", False),
            tradeId=trade_data.get("tradeId")
        ))
    
    return TradesResponse(
        symbol=symbol,
        count=len(trades),
        trades=trades
    )


@app.websocket("/ws/kline")
async def ws_kline(
    websocket: WebSocket,
    symbol: str,
    interval: str = Query("1m", description="Khung thời gian (VD: 1m, 5m, 1h)"),
    limit: int = Query(100, ge=1, le=500),
):
    await websocket.accept()
    redis_client = websocket.app.state.redis
    kafka_manager = websocket.app.state.kafka_manager
    
    try:
        # [FIX] Trỏ đúng vào interval mà User đang xem
        index_key = f"crypto:{symbol}:{interval}:index"
        timestamps = await redis_client.zrange(index_key, -limit, -1)
        
        candles_from_redis = []
        for ts in timestamps:
            key = f"crypto:{symbol}:{interval}:{ts}"
            raw = await redis_client.get(key)
            if raw:
                data = json.loads(raw)
                # [FIX] Đọc đúng key is_closed từ Redis
                if data.get("is_closed", data.get("x", False)):
                    candles_from_redis.append(data)
        
        candles_from_redis.sort(key=lambda x: x.get("openTime", 0))
        if candles_from_redis:
            await websocket.send_json({
                "type": "initial",
                "candles": candles_from_redis
            })
        
        # [FIX] Bắn nến mới nhất theo đúng khung giờ
        latest_key = f"crypto:{symbol}:{interval}:latest"
        latest_raw = await redis_client.get(latest_key)
        if latest_raw:
            latest_data = json.loads(latest_raw)
            formatted_latest = {
                "symbol": latest_data.get("symbol"),
                "interval": latest_data.get("interval"),
                "openTime": latest_data.get("open_time", latest_data.get("openTime")),
                "closeTime": latest_data.get("close_time", latest_data.get("closeTime")),
                "open": latest_data.get("open"),
                "high": latest_data.get("high"),
                "low": latest_data.get("low"),
                "close": latest_data.get("close"),
                "volume": latest_data.get("volume"),
            }
            await websocket.send_json({
                "type": "latest",
                "candle": formatted_latest
            })
    except Exception as e:
        print(f"Error loading from Redis: {e}")
    
    try:
        # [FIX] Đăng ký Kafka theo cả symbol và interval
        await kafka_manager.subscribe(settings.kafka_topic, symbol, websocket, interval)
        try:
            while True:
                try:
                    await asyncio.wait_for(websocket.receive(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except (WebSocketDisconnect, RuntimeError):
                    break
        except WebSocketDisconnect:
            pass
    finally:
        await kafka_manager.unsubscribe(settings.kafka_topic, symbol, websocket, interval)


@app.websocket("/ws/orderbook")
async def ws_orderbook(
    websocket: WebSocket,
    symbol: str,
):
    """WebSocket endpoint for real-time Order Book updates (Top of Book)"""
    await websocket.accept()
    redis_client = websocket.app.state.redis
    key = f"orderbook:{symbol}"
    
    async def get_orderbook_data():
        """Fetch Hash data from Redis and format for Frontend"""
        try:
            # Dùng hgetall thay vì get để lôi toàn bộ hộc tủ ra
            raw_data = await redis_client.hgetall(key)
            if not raw_data:
                return None

            # Parse byte to string (nếu Redis trả về bytes)
            def decode(val):
                return val.decode('utf-8') if isinstance(val, bytes) else val
            
            data = {decode(k): decode(v) for k, v in raw_data.items()}

            best_bid = float(data.get("best_bid", 0))
            best_ask = float(data.get("best_ask", 0))
            timestamp = int(data.get("updated_at", 0))

            # Spark không lưu Quantity nên mình mượn tạm số 1.0 để giao diện không báo lỗi NaN
            return {
                "symbol": symbol,
                "bids": [{"price": best_bid, "quantity": 1.0, "total": 1.0}] if best_bid else [],
                "asks": [{"price": best_ask, "quantity": 1.0, "total": 1.0}] if best_ask else [],
                "timestamp": timestamp
            }
        except Exception as e:
            print(f"Error formatting Hash data: {e}")
            return None

    # 1. Gửi dữ liệu lần đầu tiên
    try:
        initial_data = await get_orderbook_data()
        if initial_data:
            await websocket.send_json({"type": "initial", **initial_data})
    except Exception as e:
        print(f"Error sending initial: {e}")

    # 2. Vòng lặp lấy dữ liệu Realtime
    last_timestamp = None
    poll_interval = 0.2  # Quét 5 lần/giây
    
    try:
        while True:
            current_data = await get_orderbook_data()
            if current_data:
                curr_ts = current_data["timestamp"]
                # Chỉ gửi khi có cập nhật thời gian mới
                if curr_ts != last_timestamp:
                    await websocket.send_json({"type": "update", **current_data})
                    last_timestamp = curr_ts
            
            # Chờ một nhịp và kiểm tra kết nối từ client
            try:
                await asyncio.wait_for(websocket.receive(), timeout=poll_interval)
            except asyncio.TimeoutError:
                continue # Timeout là bình thường, tiếp tục vòng lặp
            except (WebSocketDisconnect, RuntimeError):
                break # Mất kết nối thì thoát
    except Exception as e:
        print(f"Orderbook WS Error: {e}")


@app.websocket("/ws/trades")
async def ws_trades(
    websocket: WebSocket,
    symbol: str,
    limit: int = Query(50, ge=1, le=100),
):
    """WebSocket endpoint for real-time Market Trades"""
    await websocket.accept()
    redis_client = websocket.app.state.redis
    kafka_manager = websocket.app.state.kafka_manager
    
    # 1. Send initial trades from Redis
    try:
        key = f"trades:{symbol}:list"
        trades_raw = await redis_client.zrange(key, -limit, -1, withscores=False)
        
        trades = []
        for trade_json in reversed(trades_raw):
            trade_data = json.loads(trade_json)
            trades.append({
                "symbol": trade_data.get("symbol", symbol),
                "price": trade_data.get("price", 0),
                "quantity": trade_data.get("quantity", 0),
                "time": trade_data.get("trade_time", 0),
                "isBuyerMaker": trade_data.get("isBuyerMaker", False),
                "tradeId": trade_data.get("tradeId")
            })
        
        if trades:
            try:
                await websocket.send_json({
                    "type": "initial",
                    "symbol": symbol,
                    "trades": trades
                })
            except (RuntimeError, WebSocketDisconnect) as send_err:
                print(f"Error sending initial trades data: {send_err}")
                raise  # Re-raise to close connection properly
    except Exception as e:
        print(f"Error loading initial trades from Redis: {e}")
        # Don't raise - continue with streaming even if initial data fails
    
    # 2. Subscribe to shared Kafka consumer
    try:
        await kafka_manager.subscribe("crypto.trades", symbol, websocket)
        
        # Keep connection alive and monitor for disconnects
        try:
            while True:
                # Try to receive with timeout to detect disconnects
                try:
                    await asyncio.wait_for(websocket.receive(), timeout=1.0)
                except asyncio.TimeoutError:
                    # Timeout is OK, connection still alive
                    continue
                except (WebSocketDisconnect, RuntimeError):
                    # Connection closed
                    break
        except WebSocketDisconnect:
            pass
        except Exception as e:
            print(f"Error in trades websocket: {e}")
    finally:
        # Unsubscribe when connection closes
        await kafka_manager.unsubscribe("crypto.trades", symbol, websocket)


@app.get("/ranking/top-gainers", response_model=RankingResponse)
async def get_top_gainers(
    limit: int = Query(1000, ge=1, le=10000, description="Number of top coins to return"),
    type: str = Query("gainers", description="Type: 'gainers' or 'losers'"),
    redis_client=Depends(get_redis),
):
    """Get Top Gainers/Losers ranking from Redis"""
    redis_key = "ranking:top_gainers"
    
    try:
        raw = await redis_client.get(redis_key)
        if not raw:
            raise HTTPException(
                status_code=404,
                detail="No ranking data available. Spark streaming job may not be running."
            )
        
        rankings_data = json.loads(raw)
        
        if not isinstance(rankings_data, list):
            raise HTTPException(
                status_code=500,
                detail="Invalid ranking data format in Redis"
            )
        
        # Convert to CoinRanking objects
        rankings = [CoinRanking(**item) for item in rankings_data]
        
        # Filter by type (gainers or losers)
        if type == "losers":
            # Sort by percent_change ascending (most negative first)
            rankings = sorted(rankings, key=lambda x: x.percent_change)[:limit]
        else:
            # Sort by percent_change descending (highest first) - default gainers
            rankings = sorted(rankings, key=lambda x: x.percent_change, reverse=True)[:limit]
        
        return RankingResponse(
            type=type,
            count=len(rankings),
            rankings=rankings,
            updated_at=datetime.now(timezone.utc).isoformat()
        )
        
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Invalid JSON data in Redis: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error loading ranking data: {str(e)}"
        )


@app.get("/prediction/{symbol}", response_model=PredictionResponse)
async def get_prediction(
    symbol: str,
    redis_client=Depends(get_redis),
    mongo_db=Depends(get_mongo),
):
    """Get latest price prediction for a symbol from Redis (fallback to MongoDB)"""
    redis_key = f"crypto:prediction:{symbol}"
    
    try:
        # Try Redis first
        raw = await redis_client.get(redis_key)
        if raw:
            try:
                pred_data = json.loads(raw)
                return PredictionResponse(
                    symbol=symbol,
                    prediction=Prediction(**pred_data)
                )
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Error parsing prediction JSON from Redis for {symbol}: {e}")
                # Fall through to MongoDB
        
        # Fallback to MongoDB
        predictions_col = mongo_db["predictions"]
        latest_pred = await predictions_col.find_one(
            {"symbol": symbol},
            sort=[("prediction_time", -1)]
        )
        
        if latest_pred:
            # Convert MongoDB document to Prediction model
            pred_data = {
                "symbol": latest_pred.get("symbol", symbol),
                "current_price": float(latest_pred.get("close", 0)),
                "predicted_price": float(latest_pred.get("predicted_price", 0)),
                "predicted_change": float(latest_pred.get("predicted_change_pct", 0)),
                "direction": latest_pred.get("direction", "UP"),
                "prediction_time": latest_pred.get("prediction_time", ""),
                "target_time": latest_pred.get("target_time", ""),
                "confidence_score": float(abs(latest_pred.get("predicted_change_pct", 0)))
            }
            return PredictionResponse(
                symbol=symbol,
                prediction=Prediction(**pred_data)
            )
        
        raise HTTPException(
            status_code=404,
            detail=f"No prediction found for {symbol}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error loading prediction for {symbol}: {str(e)}"
        )



@app.get("/predictions", response_model=PredictionsListResponse)
async def get_all_predictions(
    redis_client=Depends(get_redis),
    mongo_db=Depends(get_mongo),
):
    """Lấy predictions mới nhất cho tất cả symbols"""
    predictions = []
    for symbol in settings.symbols:
        # Thử Redis trước
        redis_key = f"crypto:prediction:{symbol}"
        raw = await redis_client.get(redis_key)
        if raw:
            try:
                pred_data = json.loads(raw)
                predictions.append(Prediction(**pred_data))
                continue
            except Exception:
                pass
        # Fallback MongoDB
        doc = await mongo_db["predictions"].find_one(
            {"symbol": symbol}, sort=[("predicted_at", -1)]
        )
        if doc:
            predictions.append(Prediction(
                symbol=doc.get("symbol", symbol),
                current_price=float(doc.get("close", 0)),
                predicted_price=float(doc.get("predicted_price", 0)),
                predicted_change=float(doc.get("predicted_change_pct", 0)),
                direction=doc.get("direction", "HOLD"),
                prediction_time=str(doc.get("predicted_at", "")),
                target_time=str(doc.get("target_time", "")),
                confidence_score=float(abs(doc.get("predicted_change_pct", 0))),
            ))
    return PredictionsListResponse(count=len(predictions), predictions=predictions)

@app.get("/prediction/{symbol}/history", response_model=PredictionHistoryResponse)
async def get_prediction_history(
    symbol: str,
    limit: int = Query(50, ge=1, le=100),
    mongo_db=Depends(get_mongo),
):
    """Lấy lịch sử dự báo giá của AI cho 1 đồng coin"""
    predictions_col = mongo_db["predictions"]
    
    cursor = predictions_col.find(
        {"symbol": symbol}
    ).sort("prediction_time", -1).limit(limit)
    
    docs = await cursor.to_list(length=limit)
    
    history = []
    for doc in docs:
        history.append(PredictionHistory(
            symbol=doc.get("symbol", symbol),
            prediction_time=doc.get("prediction_time", ""),
            predicted_price=float(doc.get("predicted_price", 0)),
            predicted_change=float(doc.get("predicted_change_pct", 0)),
            # Trong môi trường thực tế, bạn sẽ join với bảng kline để lấy actual_price
            actual_price=None, 
            actual_change=None,
            accuracy=None
        ))
        
    return PredictionHistoryResponse(
        symbol=symbol,
        count=len(history),
        history=history
    )