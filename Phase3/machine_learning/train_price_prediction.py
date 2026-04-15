#!/usr/bin/env python3
"""
Pipeline Huấn luyện Machine Learning - Dự đoán giá bằng Hồi quy tuyến tính (Linear Regression)
Huấn luyện mô hình để dự đoán tỷ lệ biến động giá trong nến 5 phút tiếp theo
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, avg, stddev, max as spark_max, min as spark_min,
    when, lit, unix_timestamp, from_unixtime
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pymongo import MongoClient

# ==========================================
# CẤU HÌNH HỆ THỐNG
# ==========================================
# Ưu tiên lấy biến môi trường, nếu không có thì dùng giá trị mặc định (Localhost)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "CRYPTO")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "5m_kline")

MODEL_PATH = os.getenv("MODEL_PATH", "/tmp/crypto_lr_model")
TRAINING_DAYS = int(os.getenv("TRAINING_DAYS", 30)) # Số ngày lấy dữ liệu để huấn luyện

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


def fetch_training_data(mongo_uri, db_name, collection_name, symbols, days=30):
    """Kéo dữ liệu lịch sử nến (OHLC) từ MongoDB để làm tài liệu học cho AI"""
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    
    # Tính toán khoảng thời gian
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)
    start_timestamp = int(start_time.timestamp() * 1000)
    end_timestamp = int(end_time.timestamp() * 1000)
    
    print(f"📊 Đang kéo dữ liệu từ {start_time} đến {end_time}")
    print(f"   Danh sách Coin: {symbols}")
    
    # Truy vấn MongoDB
    query = {
        "symbol": {"$in": symbols},
        "interval": "5m",
        "openTime": {"$gte": start_timestamp, "$lte": end_timestamp}
    }
    
    # Ép sắp xếp theo thời gian để đảm bảo chuỗi Time Series chuẩn xác
    cursor = collection.find(query).sort("openTime", 1)
    data = list(cursor)
    client.close()
    
    print(f"✅ Đã lấy thành công {len(data)} cây nến")
    return data


def create_features(df):
    """
    Kỹ thuật trích xuất đặc trưng (Feature Engineering) cho mô hình ML
    Các đặc trưng bao gồm:
    - Sự thay đổi và tỷ suất sinh lời của giá (Returns)
    - Đường trung bình động (MA 5, 10, 20 nến)
    - Độ biến động (Độ lệch chuẩn - Volatility)
    - Đột biến khối lượng giao dịch (Volume ratio)
    - Chỉ báo động lượng tương tự RSI
    - Vị thế của giá so với đường trung bình
    """
    
    # Định nghĩa "Khung nhìn" cho từng đồng coin, sắp xếp theo thời gian
    window_spec = Window.partitionBy("symbol").orderBy("openTime")
    
    # Các đặc trưng cơ bản của nến
    df = df.withColumn("price_range", col("high") - col("low")) # Biên độ giá
    df = df.withColumn("body_size", col("close") - col("open")) # Thân nến
    
    # Dùng hàm greatest/least để tính bóng nến (râu nến) trên và dưới
    from pyspark.sql.functions import greatest, least
    df = df.withColumn("upper_shadow", col("high") - greatest(col("open"), col("close")))
    df = df.withColumn("lower_shadow", least(col("open"), col("close")) - col("low"))
    
    # Các đặc trưng nhìn về quá khứ (Lag features)
    df = df.withColumn("close_lag1", lag("close", 1).over(window_spec))
    df = df.withColumn("close_lag2", lag("close", 2).over(window_spec))
    df = df.withColumn("close_lag3", lag("close", 3).over(window_spec))
    
    # Tỷ suất lợi nhuận so với nến trước đó (%)
    df = df.withColumn("return_1", 
                       when(col("close_lag1").isNotNull(), 
                            (col("close") - col("close_lag1")) / col("close_lag1") * 100)
                       .otherwise(0))
    
    df = df.withColumn("return_2", 
                       when(col("close_lag2").isNotNull(), 
                            (col("close") - col("close_lag2")) / col("close_lag2") * 100)
                       .otherwise(0))
    
    # Đường trung bình động (Moving Averages)
    df = df.withColumn("ma5", avg("close").over(window_spec.rowsBetween(-4, 0)))
    df = df.withColumn("ma10", avg("close").over(window_spec.rowsBetween(-9, 0)))
    df = df.withColumn("ma20", avg("close").over(window_spec.rowsBetween(-19, 0)))
    
    # Độ biến động rủi ro (Volatility)
    df = df.withColumn("volatility_5", stddev("close").over(window_spec.rowsBetween(-4, 0)))
    df = df.withColumn("volatility_10", stddev("close").over(window_spec.rowsBetween(-9, 0)))
    
    # Các đặc trưng về Khối lượng (Volume)
    df = df.withColumn("volume_ma5", avg("volume").over(window_spec.rowsBetween(-4, 0)))
    df = df.withColumn("volume_ratio", 
                       when(col("volume_ma5") > 0, col("volume") / col("volume_ma5"))
                       .otherwise(1.0))
    
    # Vị thế của giá so với đường trung bình (%)
    df = df.withColumn("price_to_ma5", 
                       when(col("ma5") > 0, (col("close") - col("ma5")) / col("ma5") * 100)
                       .otherwise(0))
    df = df.withColumn("price_to_ma20", 
                       when(col("ma20") > 0, (col("close") - col("ma20")) / col("ma20") * 100)
                       .otherwise(0))
    
    # Chỉ báo động lượng đơn giản (Giống RSI)
    df = df.withColumn("gain", when(col("return_1") > 0, col("return_1")).otherwise(0))
    df = df.withColumn("loss", when(col("return_1") < 0, -col("return_1")).otherwise(0))
    
    df = df.withColumn("avg_gain", avg("gain").over(window_spec.rowsBetween(-13, 0)))
    df = df.withColumn("avg_loss", avg("loss").over(window_spec.rowsBetween(-13, 0)))
    
    df = df.withColumn("rsi", 
                       when(col("avg_loss") > 0, 
                            100 - (100 / (1 + col("avg_gain") / col("avg_loss"))))
                       .when(col("avg_gain") > 0, 100)  # Tăng tuyệt đối, không có giảm
                       .otherwise(50))
    
    # TẠO NHÃN (TARGET): Tỷ lệ biến động giá (%) của cây nến 5 phút TIẾP THEO
    df = df.withColumn("next_close", lag("close", -1).over(window_spec))
    df = df.withColumn("target", 
                       when(col("next_close").isNotNull(), 
                            (col("next_close") - col("close")) / col("close") * 100)
                       .otherwise(None))
    
    return df


def main():
    """Luồng chạy chính của quá trình huấn luyện mô hình"""
    import sys
    sys.stdout.flush()  # Ép flush log ra terminal ngay lập tức
    
    start_time = datetime.now(timezone.utc)
    print("=" * 80)
    print("🤖 DỰ ĐOÁN GIÁ CRYPTO - HUẤN LUYỆN MÔ HÌNH HỒI QUY TUYẾN TÍNH")
    print(f"⏰ Thời gian bắt đầu: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)
    sys.stdout.flush()

    # Sửa lỗi Windows: Ép Spark sử dụng đúng Python của môi trường ảo (venv)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    # TỰ ĐỘNG LẤY DANH SÁCH COIN TỪ KHO DỮ LIỆU
    SYMBOLS = get_active_symbols(MONGO_URI, MONGO_DB, MONGO_COLLECTION) 
    
    print(f"🔍 Phát hiện {len(SYMBOLS)} đồng coin đang có dữ liệu trong Database!")
    print(f"   Danh sách: {', '.join(SYMBOLS[:10])} ...")

    # 1. Khởi tạo Spark
    print("\n[1/7] 🔧 Đang khởi tạo Spark Session...")
    sys.stdout.flush()
    spark = SparkSession.builder \
        .appName("CryptoPricePredictor-Training") \
        .config("spark.mongodb.read.connection.uri", MONGO_URI) \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN") # Tắt bớt log rác của Spark
    print("✅ Đã khởi tạo Spark Session thành công")
    sys.stdout.flush()
    
    # 2. Kéo dữ liệu từ MongoDB
    print("\n[2/7] 📥 Đang kéo dữ liệu huấn luyện từ MongoDB...")
    print(f"   Số ngày lấy dữ liệu: {TRAINING_DAYS} ngày")
    print(f"   Số lượng Coin: {len(SYMBOLS)} đồng")
    sys.stdout.flush()
    fetch_start = datetime.now(timezone.utc)
    raw_data = fetch_training_data(MONGO_URI, MONGO_DB, MONGO_COLLECTION, SYMBOLS, TRAINING_DAYS)
    fetch_duration = (datetime.now(timezone.utc) - fetch_start).total_seconds()
    print(f"✅ Đã kéo {len(raw_data)} bản ghi trong {fetch_duration:.1f} giây")
    sys.stdout.flush()
    
    if len(raw_data) < 100:
        print("❌ Dữ liệu quá ít, không đủ để huấn luyện mô hình!")
        return

    # 3. Chuẩn hóa dữ liệu để Spark đọc hiểu được
    print("\n[3/7] 🔄 Đang chuẩn hóa và chuyển đổi kiểu dữ liệu...")
    sys.stdout.flush()
    normalize_start = datetime.now(timezone.utc)
    numeric_fields = [
        "openTime", "closeTime", "open", "high", "low", "close",
        "volume", "quoteVolume", "trades"
    ]

    total_docs = len(raw_data)
    for idx, doc in enumerate(raw_data):
        if (idx + 1) % 10000 == 0:
            print(f"   Đang xử lý: {idx + 1}/{total_docs} tài liệu ({100*(idx+1)/total_docs:.1f}%)", flush=True)
        doc.pop("_id", None) # Xóa ID của MongoDB

        # Chuyển các kiểu BSON/Numpy/Pandas lạ sang kiểu Python thuần
        for field in numeric_fields:
            val = doc.get(field)
            if val is None:
                continue
            try:
                # Xử lý các giá trị vô hướng của numpy/pandas
                if hasattr(val, "item"):
                    val = val.item()
                # Ép về float cho giá/volume, về int cho thời gian/số lượng GD
                if field in ("openTime", "closeTime", "trades"):
                    val = int(val)
                else:
                    val = float(val)
                doc[field] = val
            except Exception:
                # Nếu không chuyển được thì xóa luôn để Spark không bị lỗi Schema
                doc.pop(field, None)
    
    normalize_duration = (datetime.now(timezone.utc) - normalize_start).total_seconds()
    print(f"✅ Đã chuẩn hóa {total_docs} tài liệu trong {normalize_duration:.1f} giây")
    sys.stdout.flush()
    
    # Tạo Spark DataFrame
    print("   Đang đẩy dữ liệu vào Spark DataFrame...")
    sys.stdout.flush()
    df = spark.createDataFrame(raw_data)
    
    row_count = df.count()
    col_count = len(df.columns)
    symbol_count = df.select('symbol').distinct().count()
    print(f"✅ Tạo DataFrame thành công: {row_count:,} dòng, {col_count} cột")
    print(f"   Số lượng Coin phân biệt: {symbol_count}")
    sys.stdout.flush()
    
    # 4. Tính toán Chỉ báo kỹ thuật (Feature Engineering)
    print("\n[4/7] 🔧 Đang tính toán Kỹ thuật trích xuất đặc trưng...")
    print("   Bước này sẽ mất vài phút (Tính toán SMA, RSI, Độ biến động...)...")
    sys.stdout.flush()
    feature_start = datetime.now(timezone.utc)
    df_features = create_features(df)
    feature_duration = (datetime.now(timezone.utc) - feature_start).total_seconds()
    print(f"✅ Tính toán xong các đặc trưng trong {feature_duration:.1f} giây")
    sys.stdout.flush()
    
    # Lựa chọn các cột đặc trưng để nhét vào AI
    feature_cols = [
        "return_1", "return_2",
        "price_to_ma5", "price_to_ma20",
        "volatility_5", "volatility_10",
        "volume_ratio",
        "rsi",
        "price_range", "body_size"
    ]
    
    # Lọc bỏ các dòng bị Null (những dòng đầu tiên bị thiếu dữ liệu MA)
    df_clean = df_features.select(["symbol", "openTime", "close", "target"] + feature_cols) \
        .filter(col("target").isNotNull())
    
    for fcol in feature_cols:
        df_clean = df_clean.filter(col(fcol).isNotNull())
    
    clean_count = df_clean.count()
    print(f"✅ Dữ liệu sạch: {clean_count:,} dòng (Đã dọn dẹp các giá trị rỗng/Null)")
    sys.stdout.flush()
    
    # 5. Chia tập dữ liệu (80% để Học, 20% để Thi)
    print("\n[5/7] ✂️  Đang cắt lớp dữ liệu (80% Train, 20% Test)...")
    sys.stdout.flush()
    train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
    
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"✅ Hoàn tất cắt lớp:")
    print(f"   Tập huấn luyện (Train): {train_count:,} dòng ({100*train_count/clean_count:.1f}%)")
    print(f"   Tập kiểm thử (Test): {test_count:,} dòng ({100*test_count/clean_count:.1f}%)")
    sys.stdout.flush()
    
    # 6. Lắp ráp dây chuyền Machine Learning (Pipeline)
    print("\n[6/7] 🏗️  Đang xây dựng luồng Machine Learning (ML Pipeline)...")
    print(f"   Số lượng đặc trưng: {len(feature_cols)} biến")
    print(f"   Thuật toán: Hồi quy tuyến tính (Linear Regression) - 100 Vòng lặp")
    sys.stdout.flush()
    
    # Gom tất cả các cột thành 1 cột Vector duy nhất
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw"
    )
    
    # Chuẩn hóa Thang đo (Scale) dữ liệu
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    # Cấu hình thuật toán Hồi quy tuyến tính
    lr = LinearRegression(
        featuresCol="features",
        labelCol="target",
        predictionCol="prediction",
        maxIter=100,      # Chạy 100 lần để tối ưu
        regParam=0.1,     # Tránh học vẹt (Overfitting)
        elasticNetParam=0.0
    )
    
    # Liên kết các bước thành Dây chuyền
    pipeline = Pipeline(stages=[assembler, scaler, lr])
    print("✅ Dây chuyền đã sẵn sàng")
    sys.stdout.flush()
    
    # 7. TIẾN HÀNH HUẤN LUYỆN
    print("\n[7/7] 🎯 Bắt đầu huấn luyện mô hình (Bước này chạy lâu nhất, khoảng 5-15 phút)...")
    print("   AI đang tự động học quy luật qua 100 vòng lặp...")
    sys.stdout.flush()
    train_start = datetime.now(timezone.utc)
    model = pipeline.fit(train_df)
    train_duration = (datetime.now(timezone.utc) - train_start).total_seconds()
    print(f"✅ Huấn luyện thành công trong {train_duration:.1f} giây ({train_duration/60:.1f} phút)")
    sys.stdout.flush()
    
    # LÀM BÀI KIỂM TRA (Đánh giá Model)
    print("\n📈 Đang làm bài thi đánh giá mô hình...")
    sys.stdout.flush()
    eval_start = datetime.now(timezone.utc)
    train_predictions = model.transform(train_df)
    test_predictions = model.transform(test_df)
    eval_duration = (datetime.now(timezone.utc) - eval_start).total_seconds()
    print(f"✅ Đã có kết quả dự đoán bài thi trong {eval_duration:.1f} giây")
    sys.stdout.flush()
    
    # Cài đặt các tiêu chí chấm điểm
    evaluator_rmse = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="r2")
    evaluator_mae = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="mae")
    
    # Điểm thi trên tập Train
    train_rmse = evaluator_rmse.evaluate(train_predictions)
    train_r2 = evaluator_r2.evaluate(train_predictions)
    train_mae = evaluator_mae.evaluate(train_predictions)
    
    # Điểm thi trên tập Test (Điểm quan trọng nhất)
    test_rmse = evaluator_rmse.evaluate(test_predictions)
    test_r2 = evaluator_r2.evaluate(test_predictions)
    test_mae = evaluator_mae.evaluate(test_predictions)
    
    print("\n" + "=" * 80)
    print("📊 BÁO CÁO KẾT QUẢ ĐÁNH GIÁ MÔ HÌNH")
    print("=" * 80)
    print(f"Tập Học (Training Set):")
    print(f"  Sai số bình phương trung bình (RMSE): {train_rmse:.4f}%")
    print(f"  Sai số tuyệt đối trung bình (MAE):  {train_mae:.4f}%")
    print(f"  Hệ số xác định (R²):   {train_r2:.4f}")
    print(f"\nTập Kiểm thử (Test Set):")
    print(f"  Sai số bình phương trung bình (RMSE): {test_rmse:.4f}%")
    print(f"  Sai số tuyệt đối trung bình (MAE):  {test_mae:.4f}%")
    print(f"  Hệ số xác định (R²):   {test_r2:.4f}")
    
    # Lấy ra các hệ số hồi quy (Mức độ quan trọng của từng Đặc trưng)
    lr_model = model.stages[-1]
    print(f"\n📐 Hệ số của mô hình (Feature Importance):")
    for i, feature_name in enumerate(feature_cols):
        coef = lr_model.coefficients[i]
        print(f"  {feature_name:20s}: {coef:10.6f}")
    print(f"  Điểm cắt (Intercept): {lr_model.intercept:.6f}")
    
    # LƯU MÔ HÌNH VÀO Ổ CỨNG
    print(f"\n💾 Đang lưu mô hình vào thư mục: {MODEL_PATH}...")
    sys.stdout.flush()
    save_start = datetime.now(timezone.utc)
    os.makedirs(MODEL_PATH, exist_ok=True)
    
    # Thử lưu theo chuẩn của Spark (Có thể báo lỗi trên Windows nếu thiếu thư viện Hadoop)
    try:
        model.write().overwrite().save(MODEL_PATH)
        print("✅ Đã lưu Spark Pipeline chuẩn")
    except Exception as e:
        print(f"⚠️  Bỏ qua bước lưu Spark Pipeline nguyên gốc: {e}")
    sys.stdout.flush()

    # (THỦ THUẬT DOANH NGHIỆP): Trích xuất các tham số để lưu thành file JSON 
    # Giúp việc lấy ra dự đoán cực nhẹ mà không cần nổ máy Spark
    scaler_model = model.stages[1]
    try:
        mean_vec = list(map(float, scaler_model.mean)) if scaler_model.getWithMean() else [0.0] * len(feature_cols)
    except Exception:
        mean_vec = [0.0] * len(feature_cols)
    try:
        std_vec = list(map(float, scaler_model.std)) if scaler_model.getWithStd() else [1.0] * len(feature_cols)
    except Exception:
        std_vec = [1.0] * len(feature_cols)

    # Đóng gói Siêu dữ liệu (Metadata) và Tham số học được
    metadata = {
        "model_type": "LinearRegression",
        "features": feature_cols,
        "training_date": datetime.now(timezone.utc).isoformat(),
        "training_days": TRAINING_DAYS,
        "train_samples": train_df.count(),
        "test_samples": test_df.count(),
        "metrics": {
            "train_rmse": float(train_rmse),
            "train_mae": float(train_mae),
            "train_r2": float(train_r2),
            "test_rmse": float(test_rmse),
            "test_mae": float(test_mae),
            "test_r2": float(test_r2)
        },
        "coefficients": {feature_cols[i]: float(lr_model.coefficients[i]) for i in range(len(feature_cols))},
        "intercept": float(lr_model.intercept),
        "scaler_mean": {feature_cols[i]: float(mean_vec[i]) for i in range(len(feature_cols))},
        "scaler_std": {feature_cols[i]: float(std_vec[i]) for i in range(len(feature_cols))}
    }

    metadata_path = os.path.join(MODEL_PATH, "model.json")
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    save_duration = (datetime.now(timezone.utc) - save_start).total_seconds()
    print(f"✅ Đã lưu file tham số JSON thành công tại {metadata_path} (mất {save_duration:.1f} giây)")
    sys.stdout.flush()
    
    # Hiển thị vài ví dụ dự đoán thực tế
    print("\n🔍 Thử soi vài kết quả thực tế (Thực tế vs Dự đoán):")
    sample_preds = test_predictions.select(
        "symbol", "close", "target", "prediction"
    ).limit(10)
    
    sample_preds.show(truncate=False)
    
    total_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    print("\n" + "=" * 80)
    print("✅ ĐÃ HOÀN TẤT QUÁ TRÌNH HUẤN LUYỆN AI!")
    print(f"⏰ Tổng thời gian chạy: {total_duration:.1f} giây ({total_duration/60:.1f} phút)")
    print("=" * 80)
    sys.stdout.flush()
    
    spark.stop()


if __name__ == "__main__":
    main()