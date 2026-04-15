#!/usr/bin/env python3
"""
Dự đoán giá theo Thời gian thực (Real-time Inference)
Tải mô hình đã huấn luyện và dự đoán tỷ lệ biến động giá trong 5 phút tiếp theo
"""

import os
import sys
import json
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, avg, stddev,
    when, lit, current_timestamp, greatest, least
)
from pyspark.sql.window import Window
from pymongo import MongoClient
import redis

# ==========================================
# CẤU HÌNH HỆ THỐNG
# ==========================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "CRYPTO")
MONGO_INPUT_COLLECTION = os.getenv("MONGO_INPUT_COLLECTION", "5m_kline")
MONGO_PREDICTION_COLLECTION = os.getenv("MONGO_PREDICTION_COLLECTION", "predictions")

REDIS_HOST = os.getenv("REDIS_HOST", "redis-master.crypto-infra.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "123456")
ENABLE_REDIS = os.getenv("ENABLE_REDIS", "1") == "1"

MODEL_PATH = os.getenv("MODEL_PATH", "model/crypto_lr_model")
LOOKBACK_PERIODS = 40  # Cần ít nhất 20 nến để tính đường MA20 + nến dự phòng

def get_active_symbols(mongo_uri, db_name, collection_name):
    """Tự động quét MongoDB để tìm tất cả các đồng coin đang có dữ liệu"""
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        # Dùng lệnh distinct để lấy ra tất cả các symbol khác nhau
        active_symbols = db[collection_name].distinct("symbol")
        client.close()
        
        return active_symbols
    except Exception as e:
        print(f"⚠️ Lỗi quét danh sách coin: {e}. Quay về danh sách mặc định 5 đồng.")
        # Đã sửa lại chuẩn 5 đồng cho khớp tuyệt đối với phương án dự phòng của Kafka
        return ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT"]

def fetch_recent_data(mongo_uri, db_name, collection_name, symbols, periods=30):
    """Kéo dữ liệu OHLC gần nhất từ MongoDB để làm đầu vào cho AI dự đoán"""
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    
    # Lấy dữ liệu của các nến gần đây nhất
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=5 * periods)
    start_timestamp = int(start_time.timestamp() * 1000)
    
    query = {
        "symbol": {"$in": symbols},
        "interval": "5m",
        "openTime": {"$gte": start_timestamp}
    }
    
    cursor = collection.find(query).sort("openTime", 1)
    data = list(cursor)
    client.close()
    
    return data

def fetch_latest_k_per_symbol(mongo_uri, db_name, collection_name, symbols, k=40):
    """Cơ chế dự phòng: Kéo chính xác K cây nến mới nhất của mỗi đồng coin (bất chấp thời gian)"""
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    out = []
    for sym in symbols:
        cursor = (
            collection.find({"symbol": sym, "interval": "5m"})
            .sort("openTime", -1)
            .limit(k)
        )
        docs = list(cursor)
        if docs:
            docs.reverse()  # Lật lại thành chiều tăng dần theo thời gian
            out.extend(docs)
    client.close()
    return out

def normalize_mongo_docs(docs):
    """Chuẩn hóa dữ liệu từ MongoDB sang kiểu Python thuần để Spark không bị lú"""
    required_keys = [
        "symbol", "interval", "openTime", "open", "high", "low", "close", "volume"
    ]
    normalized = []
    for d in docs:
        if not isinstance(d, dict): continue
        nd = dict(d)
        nd.pop("_id", None)
        
        nd = {k: nd.get(k) for k in required_keys}
        
        if nd.get("symbol") is not None: nd["symbol"] = str(nd["symbol"])
        if nd.get("interval") is not None: nd["interval"] = str(nd["interval"])
            
        for tkey in ["openTime"]:
            if nd.get(tkey) is not None:
                try: nd[tkey] = int(nd[tkey])
                except Exception: pass
                    
        for fkey in ["open", "high", "low", "close", "volume"]:
            if nd.get(fkey) is not None:
                try: nd[fkey] = float(nd[fkey])
                except Exception: pass
        normalized.append(nd)
    return normalized

def create_features(df):
    """Tạo lại chính xác các Bộ chỉ báo kỹ thuật giống hệt lúc Huấn luyện (Training)"""
    window_spec = Window.partitionBy("symbol").orderBy("openTime")
    
    df = df.withColumn("price_range", col("high") - col("low"))
    df = df.withColumn("body_size", col("close") - col("open"))
    df = df.withColumn("upper_shadow", col("high") - greatest(col("open"), col("close")))
    df = df.withColumn("lower_shadow", least(col("open"), col("close")) - col("low"))
    
    df = df.withColumn("close_lag1", lag("close", 1).over(window_spec))
    df = df.withColumn("close_lag2", lag("close", 2).over(window_spec))
    df = df.withColumn("close_lag3", lag("close", 3).over(window_spec))
    
    df = df.withColumn("return_1", when(col("close_lag1").isNotNull(), (col("close") - col("close_lag1")) / col("close_lag1") * 100).otherwise(0))
    df = df.withColumn("return_2", when(col("close_lag2").isNotNull(), (col("close") - col("close_lag2")) / col("close_lag2") * 100).otherwise(0))
    
    df = df.withColumn("ma5", avg("close").over(window_spec.rowsBetween(-4, 0)))
    df = df.withColumn("ma10", avg("close").over(window_spec.rowsBetween(-9, 0)))
    df = df.withColumn("ma20", avg("close").over(window_spec.rowsBetween(-19, 0)))
    
    df = df.withColumn("volatility_5", stddev("close").over(window_spec.rowsBetween(-4, 0)))
    df = df.withColumn("volatility_10", stddev("close").over(window_spec.rowsBetween(-9, 0)))
    
    df = df.withColumn("volume_ma5", avg("volume").over(window_spec.rowsBetween(-4, 0)))
    df = df.withColumn("volume_ratio", when(col("volume_ma5") > 0, col("volume") / col("volume_ma5")).otherwise(1.0))
    
    df = df.withColumn("price_to_ma5", when(col("ma5") > 0, (col("close") - col("ma5")) / col("ma5") * 100).otherwise(0))
    df = df.withColumn("price_to_ma20", when(col("ma20") > 0, (col("close") - col("ma20")) / col("ma20") * 100).otherwise(0))
    
    df = df.withColumn("gain", when(col("return_1") > 0, col("return_1")).otherwise(0))
    df = df.withColumn("loss", when(col("return_1") < 0, -col("return_1")).otherwise(0))
    
    df = df.withColumn("avg_gain", avg("gain").over(window_spec.rowsBetween(-13, 0)))
    df = df.withColumn("avg_loss", avg("loss").over(window_spec.rowsBetween(-13, 0)))
    
    df = df.withColumn("rsi", 
                       when(col("avg_loss") > 0, 100 - (100 / (1 + col("avg_gain") / col("avg_loss"))))
                       .when(col("avg_gain") > 0, 100)
                       .otherwise(50))
    return df

def main():
    """Luồng chạy chính của quá trình Dự đoán giá"""
    sys.stdout.flush()
    
    start_time = datetime.now(timezone.utc)
    print("=" * 80)
    print("🔮 DỰ ĐOÁN GIÁ CRYPTO - XỬ LÝ THEO THỜI GIAN THỰC (INFERENCE)")
    print(f"⏰ Thời gian bắt đầu: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)
    sys.stdout.flush()
    
    # Sửa lỗi Spark Worker trên Windows
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    # TỰ ĐỘNG LẤY DANH SÁCH COIN TỪ KHO DỮ LIỆU
    SYMBOLS = get_active_symbols(MONGO_URI, MONGO_DB, MONGO_INPUT_COLLECTION)
    
    print(f"🔍 Phát hiện {len(SYMBOLS)} đồng coin đang có dữ liệu trong Database!")
    print(f"   Danh sách: {', '.join(SYMBOLS[:10])} ...")

    # Khởi tạo Spark
    print("\n[1/6] 🔧 Đang khởi tạo Spark Session...")
    sys.stdout.flush()
    spark = SparkSession.builder \
        .appName("CryptoPricePredictor-Inference") \
        .config("spark.mongodb.read.connection.uri", MONGO_URI) \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Đã khởi tạo Spark Session thành công")
    sys.stdout.flush()
    
    # Đọc cấu hình Mô hình từ file JSON (Siêu nhẹ, không tốn RAM)
    print(f"\n[2/6] 📂 Đang nạp mô hình từ {MODEL_PATH}...")
    sys.stdout.flush()
    params_path = os.path.join(MODEL_PATH, "model.json")
    if not os.path.exists(params_path):
        print(f"❌ Lỗi: Không tìm thấy file mô hình tại {params_path}")
        print("   Vui lòng chạy file train_price_prediction.py để AI học và sinh ra file này trước!")
        return
        
    load_start = datetime.now(timezone.utc)
    with open(params_path, 'r', encoding='utf-8') as f:
        model_params = json.load(f)
    load_duration = (datetime.now(timezone.utc) - load_start).total_seconds()
    
    print(f"✅ Đã nạp thành công tham số mô hình trong {load_duration:.1f} giây")
    print(f"   Loại mô hình: {model_params.get('model_type', 'Không rõ')}")
    print(f"   Số lượng đặc trưng: {len(model_params.get('features', []))} biến")
    sys.stdout.flush()
    
    # Kéo dữ liệu thời gian thực
    print(f"\n[3/6] 📥 Đang lấy dữ liệu {LOOKBACK_PERIODS} nến gần nhất...")
    print(f"   Số lượng Coin: {len(SYMBOLS)} đồng")
    sys.stdout.flush()
    fetch_start = datetime.now(timezone.utc)
    raw_data = fetch_recent_data(MONGO_URI, MONGO_DB, MONGO_INPUT_COLLECTION, SYMBOLS, LOOKBACK_PERIODS)
    
    if len(raw_data) < LOOKBACK_PERIODS:
        print(f"⚠️  Dữ liệu gần đây bị thiếu (chỉ có {len(raw_data)}). Đang kích hoạt cơ chế kéo dữ liệu dự phòng...")
        sys.stdout.flush()
        raw_data = fetch_latest_k_per_symbol(MONGO_URI, MONGO_DB, MONGO_INPUT_COLLECTION, SYMBOLS, k=max(LOOKBACK_PERIODS, 40))
        if len(raw_data) == 0:
            print("❌ Lỗi: Cơ sở dữ liệu hoàn toàn trống rỗng. Hãy chạy ohlc_5m_aggregator.py để tạo nến trước!")
            return
    
    fetch_duration = (datetime.now(timezone.utc) - fetch_start).total_seconds()
    print(f"✅ Đã lấy được {len(raw_data)} cây nến trong {fetch_duration:.1f} giây")
    sys.stdout.flush()
    
    print("   Đang chuẩn hóa dữ liệu đưa vào Spark...")
    sys.stdout.flush()
    normalize_start = datetime.now(timezone.utc)
    raw_data = normalize_mongo_docs(raw_data)
    df = spark.createDataFrame(raw_data)
    normalize_duration = (datetime.now(timezone.utc) - normalize_start).total_seconds()
    print(f"✅ Tạo DataFrame thành công: {df.count()} dòng ({normalize_duration:.1f} giây)")
    sys.stdout.flush()
    
    # Feature Engineering
    print("\n[4/6] 🔧 Đang tính toán Kỹ thuật trích xuất đặc trưng...")
    print("   Tính toán đường MA, RSI, Volatility...")
    sys.stdout.flush()
    feature_start = datetime.now(timezone.utc)
    df_features = create_features(df)
    feature_duration = (datetime.now(timezone.utc) - feature_start).total_seconds()
    print(f"✅ Hoàn tất tính toán trong {feature_duration:.1f} giây")
    sys.stdout.flush()
    
    feature_cols = [
        "return_1", "return_2", "price_to_ma5", "price_to_ma20",
        "volatility_5", "volatility_10", "volume_ratio", "rsi",
        "price_range", "body_size"
    ]
    
    # Chỉ lấy CÂY NẾN MỚI NHẤT của từng đồng coin để dự đoán tương lai
    window_latest = Window.partitionBy("symbol").orderBy(col("openTime").desc())
    from pyspark.sql.functions import row_number
    
    df_latest = df_features.withColumn("rn", row_number().over(window_latest)) \
        .filter(col("rn") == 1) \
        .drop("rn")
    
    df_predict = df_latest.select(["symbol", "openTime", "open", "high", "low", "close", "volume"] + feature_cols)
    
    # Lọc bỏ các dòng lỗi Null
    for fcol in feature_cols:
        df_predict = df_predict.filter(col(fcol).isNotNull())
    
    predict_count = df_predict.count()
    print(f"✅ Dữ liệu đã sẵn sàng để AI dự đoán cho {predict_count} đồng coin")
    sys.stdout.flush()
    
    # TIẾN HÀNH DỰ ĐOÁN (Công thức Toán học Thủ công siêu nhanh)
    print("\n[5/6] 🎯 Đang tiến hành dự đoán tương lai...")
    sys.stdout.flush()
    predict_start = datetime.now(timezone.utc)

    # Dựng phương trình Hồi quy tuyến tính từ file JSON: Y = Sum(((X - Mean)/Std) * Coef) + Intercept
    intercept = float(model_params.get("intercept", 0.0))
    pred_expr = lit(intercept)
    for f in feature_cols:
        mean_f = float(model_params.get("scaler_mean", {}).get(f, 0.0))
        std_f = float(model_params.get("scaler_std", {}).get(f, 1.0))
        coef_f = float(model_params.get("coefficients", {}).get(f, 0.0))
        standardized = when(lit(std_f) != 0.0, (col(f) - lit(mean_f)) / lit(std_f)).otherwise(lit(0.0))
        pred_expr = pred_expr + (standardized * lit(coef_f))

    predictions = df_predict.withColumn("prediction", pred_expr)
    
    # Quy đổi Tỷ lệ (%) thành Giá cụ thể và Xu hướng Tăng/Giảm
    from pyspark.sql.functions import from_unixtime
    predictions = predictions.withColumn(
        "predicted_change_pct", col("prediction")
    ).withColumn(
        "predicted_price", col("close") * (1 + col("prediction") / 100)
    ).withColumn(
        "direction", when(col("prediction") > 0, "UP").otherwise("DOWN")
    ).withColumn(
        "prediction_time", lit(datetime.now(timezone.utc).isoformat())
    ).withColumn(
        "target_time", from_unixtime((col("openTime") + 300000) / 1000)  # Thời gian chốt sổ của nến 5m tiếp theo
    )
    
    predict_duration = (datetime.now(timezone.utc) - predict_start).total_seconds()
    print(f"✅ Hoàn tất dự đoán trong {predict_duration:.1f} giây")
    sys.stdout.flush()
    
    output = predictions.select(
        "symbol", "openTime", "close", "predicted_price", 
        "predicted_change_pct", "direction", "prediction_time", 
        "target_time", "return_1", "rsi", "volatility_5", "volume_ratio"
    )
    
    # In Bảng Báo cáo ra Terminal
    print("\n" + "=" * 80)
    print("📈 KẾT QUẢ DỰ BÁO CỦA TRÍ TUỆ NHÂN TẠO")
    print("=" * 80)
    sys.stdout.flush()
    
    predictions_collected = output.orderBy(col("predicted_change_pct").desc()).collect()
    
    if predictions_collected:
        print(f"\n{'Đồng Coin':<12} {'Giá Hiện tại':<14} {'Giá Dự báo':<14} {'Mức Thay đổi':<12} {'Xu Hướng':<10} {'RSI':<6}")
        print("-" * 80)
        for row in predictions_collected:
            symbol = row["symbol"]
            current = float(row["close"])
            predicted = float(row["predicted_price"])
            change_pct = float(row["predicted_change_pct"])
            direction = row["direction"]
            try: rsi = float(row["rsi"]) if row["rsi"] is not None else 50.0
            except (KeyError, TypeError): rsi = 50.0
            
            # Gắn Icon cho ngầu
            direction_icon = "📈" if direction == "UP" else "📉"
            print(f"{symbol:<12} ${current:<13.4f} ${predicted:<13.4f} {change_pct:>+9.2f}% {direction_icon} {direction:<8} {rsi:>5.1f}")
        
        print("-" * 80)
        
        # Tóm tắt số liệu
        up_count = sum(1 for row in predictions_collected if row["direction"] == "UP")
        down_count = len(predictions_collected) - up_count
        avg_change = sum(float(row["predicted_change_pct"]) for row in predictions_collected) / len(predictions_collected)
        max_gain = max(float(row["predicted_change_pct"]) for row in predictions_collected)
        max_loss = min(float(row["predicted_change_pct"]) for row in predictions_collected)
        
        print(f"\n📊 Bản tin Thị trường nhanh:")
        print(f"   Dự báo TĂNG (UP): {up_count} mã ({100*up_count/len(predictions_collected):.1f}%)")
        print(f"   Dự báo GIẢM (DOWN): {down_count} mã ({100*down_count/len(predictions_collected):.1f}%)")
        print(f"   Mức biến động trung bình toàn thị trường: {avg_change:+.2f}%")
        print(f"   Mã có khả năng tăng mạnh nhất: {max_gain:+.2f}%")
        print(f"   Mã có khả năng giảm sâu nhất: {max_loss:+.2f}%")
    else:
        print("⚠️  Không tạo được kết quả dự đoán nào!")
    
    print("=" * 80)
    sys.stdout.flush()
    
    # 6. LƯU TRỮ KẾT QUẢ ĐỂ SỬ DỤNG
    print("\n[6/6] 💾 Đang lưu kết quả dự đoán để phân phối...")
    sys.stdout.flush()
    save_start = datetime.now(timezone.utc)
    predictions_list = output.toPandas().to_dict('records')
    
    # Lưu vào MongoDB (Dùng để báo cáo, đối soát model)
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_PREDICTION_COLLECTION]
    collection.create_index([("symbol", 1), ("prediction_time", -1)])
    
    if predictions_list:
        collection.insert_many(predictions_list)
        print(f"✅ Đã lưu {len(predictions_list)} dự báo vào kho MongoDB")
    sys.stdout.flush()
    
    # Lưu vào Redis (Dùng để bắn dữ liệu siêu tốc lên Website/App cho User xem)
    if ENABLE_REDIS:
        try:
            redis_client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, 
                password=REDIS_PASSWORD, decode_responses=True
            )
            for pred in predictions_list:
                key = f"crypto:prediction:{pred['symbol']}"
                # Giữ trong Redis 30 phút, quá giờ tự động xóa
                redis_client.setex(
                    key, 1800, 
                    json.dumps({
                        "symbol": pred["symbol"],
                        "current_price": float(pred["close"]),
                        "predicted_price": float(pred["predicted_price"]),
                        "predicted_change": float(pred["predicted_change_pct"]),
                        "direction": pred["direction"],
                        "prediction_time": pred["prediction_time"],
                        "target_time": pred["target_time"],
                        "confidence_score": float(abs(pred["predicted_change_pct"]))
                    })
                )
            redis_client.close()
            print(f"✅ Đã cập nhật {len(predictions_list)} dự báo lên Cache Redis (Sẵn sàng phục vụ Web/App)")
        except Exception as e:
            print(f"⚠️  Không thể kết nối Redis ({REDIS_HOST}:{REDIS_PORT}): {e}. Bỏ qua bước lưu Redis.")
    else:
        print("ℹ️  Tính năng lưu Redis đang bị tắt (ENABLE_REDIS=0)")
    sys.stdout.flush()
    
    total_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    print("\n" + "=" * 80)
    print("✅ ĐÃ HOÀN TẤT CHU TRÌNH DỰ BÁO!")
    print(f"⏰ Tổng thời gian chạy: {total_duration:.1f} giây")
    print("=" * 80)
    sys.stdout.flush()
    
    client.close()
    spark.stop()

if __name__ == "__main__":
    main()