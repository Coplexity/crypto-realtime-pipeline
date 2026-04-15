#!/usr/bin/env python3
"""
Job Tái cấu trúc & Làm giàu Dữ liệu (Spark Data Transformation)
Bao phủ các yêu cầu chấm điểm nâng cao của Data Engineer:
1. Thao tác Join: Broadcast Join (Bảng nhỏ), Sort-Merge Join (Bảng siêu lớn)
2. Tổng hợp phức tạp: Pivot (Dọc -> Ngang) và Unpivot (Ngang -> Dọc) bằng hàm Stack
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# ==========================================
# CẤU HÌNH HỆ THỐNG
# ==========================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "CRYPTO")
KLINE_COLLECTION = "1h_kline"
PREDICTION_COLLECTION = "predictions"

def main():
    # Sửa lỗi Worker cho Windows
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    print("=" * 80)
    print("🚀 BẮT ĐẦU CHẠY SPARK DATA TRANSFORMATION (JOIN & PIVOT)")
    print("=" * 80)

    # Khởi tạo Spark Session có kết nối MongoDB
    spark = SparkSession.builder \
        .appName("Crypto-Data-Transformation") \
        .config("spark.mongodb.read.connection.uri", MONGO_URI) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Đã khởi tạo Spark Session")

    # =========================================================================
    # KỸ THUẬT 1: BROADCAST JOIN (Tối ưu hóa Join dữ liệu không cân bằng)
    # Join bảng lịch sử siêu lớn với bảng danh mục coin cực nhỏ
    # =========================================================================
    print("\n[1/3] 🔗 ĐANG THỰC HIỆN: BROADCAST JOIN...")
    
    # Tạo bảng Metadata nhỏ
    coin_metadata = [
        ("BTCUSDT", "Layer-1", "King"),
        ("ETHUSDT", "Layer-1", "Smart Contract"),
        ("SOLUSDT", "Layer-1", "High Speed"),
        ("BNBUSDT", "Exchange", "Binance"),
        ("UNIUSDT", "DeFi", "DEX"),
        ("LINKUSDT", "Oracle", "Infrastructure"),
        ("DOGEUSDT", "Meme", "Elon Musk")
    ]
    metadata_df = spark.createDataFrame(coin_metadata, ["symbol", "category", "tag"])

    try:
        # Tải dữ liệu Big Data từ MongoDB
        kline_df = spark.read.format("mongodb").option("database", MONGO_DB).option("collection", KLINE_COLLECTION).load()
        
        # Ép kiểu dữ liệu an toàn
        kline_df = kline_df.withColumn("volume", F.col("volume").cast("double")) \
                           .withColumn("close", F.col("close").cast("double"))

        # Broadcast Join: Đẩy bảng metadata nhỏ vào RAM của mọi Worker Node để khỏi bị xáo trộn dữ liệu (Shuffle)
        enriched_df = kline_df.join(broadcast(metadata_df), "symbol", "left")

        # Phân tích tổng hợp: Category nào có Volume giao dịch lớn nhất?
        category_stats = enriched_df.groupBy("category") \
            .agg(
                F.sum("volume").alias("total_volume"),
                F.avg("close").alias("avg_price")
            ).orderBy(F.desc("total_volume"))

        print("=> Kết quả Broadcast Join (Thống kê Dòng tiền theo Danh mục Coin):")
        category_stats.show(truncate=False)
        
    except Exception as e:
        print(f"⚠️ Lỗi ở phần Broadcast Join (Có thể do Database trống): {e}")

    # =========================================================================
    # KỸ THUẬT 2: SORT-MERGE JOIN (Xử lý Dữ liệu quy mô khổng lồ)
    # Ứng dụng: Backtest - Đánh giá độ chính xác của AI bằng cách so Giá thực tế và Giá dự đoán
    # =========================================================================
    print("\n[2/3] 🔗 ĐANG THỰC HIỆN: SORT-MERGE JOIN (HỆ THỐNG BACKTESTING)...")
    
    try:
        pred_df = spark.read.format("mongodb").option("database", MONGO_DB).option("collection", PREDICTION_COLLECTION).load()
        
        # Đổi tên cột để chuẩn bị Join 2 bảng lớn
        actuals = kline_df.select(
            F.col("symbol").alias("a_symbol"),
            F.col("openTime").alias("a_time"),
            F.col("close").alias("actual_close")
        )
        
        preds = pred_df.select(
            F.col("symbol").alias("p_symbol"),
            F.col("target_time").alias("p_time"), 
            F.col("predicted_price").cast("double").alias("predicted_close")
        )

        # Mặc định với 2 bảng siêu lớn, Spark tự động chuyển sang dùng Sort-Merge Join
        validation_df = actuals.join(
            preds,
            (F.col("a_symbol") == F.col("p_symbol")) & (F.col("a_time") == F.col("p_time")),
            "inner"
        )

        # Tính toán độ sai lệch (%) của AI
        comparison_df = validation_df.withColumn(
            "error_percent", 
            F.abs(F.col("actual_close") - F.col("predicted_close")) / F.col("actual_close") * 100
        ).select("a_symbol", "a_time", "actual_close", "predicted_close", "error_percent")

        print("=> Kết quả Sort-Merge Join (Backtest: So sánh Giá Thực tế vs Dự đoán):")
        comparison_df.orderBy(F.desc("a_time")).show(5)
        
    except Exception as e:
        print(f"⚠️ Lỗi ở phần Sort-Merge Join (Hãy chạy file predict_price.py trước để có dữ liệu): {e}")

    # =========================================================================
    # KỸ THUẬT 3: TỔNG HỢP PHỨC TẠP - PIVOT & UNPIVOT
    # Chuyển đổi cấu trúc bảng để phân tích tương quan giá (Correlation Matrix)
    # =========================================================================
    print("\n[3/3] 🌪️ ĐANG THỰC HIỆN: PIVOT (XOAY TRỤC) & UNPIVOT...")
    
    try:
        # Lấy 4 đồng coin phổ biến nhất
        top_coins = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]
        subset_df = kline_df.filter(F.col("symbol").isin(top_coins))

        # 3.1 PIVOT (Chuyển Hàng Dọc -> Cột Ngang)
        # Bí thuật Tối ưu: Bắt buộc truyền mảng top_coins vào pivot() để tránh Spark phải quét Database 2 lần
        pivot_df = subset_df.groupBy("openTime") \
            .pivot("symbol", top_coins) \
            .agg(F.first("close")) \
            .orderBy(F.desc("openTime")) \
            .limit(10) # Giới hạn show 10 dòng

        print("=> Dữ liệu sau PIVOT (Dạng Ngang - Dùng để so sánh giá các coin tại cùng 1 thời điểm):")
        pivot_df.show()

        # 3.2 UNPIVOT (Chuyển Cột Ngang -> Trở về Hàng Dọc)
        # Bí thuật Tối ưu: Sử dụng biểu thức 'stack' thay vì viết vòng lặp map
        unpivot_expr = f"""
            stack({len(top_coins)}, 
                'BTCUSDT', BTCUSDT, 
                'ETHUSDT', ETHUSDT, 
                'BNBUSDT', BNBUSDT, 
                'SOLUSDT', SOLUSDT
            ) as (symbol, close)
        """
        
        unpivot_df = pivot_df.select("openTime", F.expr(unpivot_expr)) \
            .filter(F.col("close").isNotNull()) \
            .orderBy(F.desc("openTime"), "symbol")

        print("=> Dữ liệu sau UNPIVOT (Phục hồi cấu trúc Dọc nguyên bản):")
        unpivot_df.show(5)

    except Exception as e:
        print(f"⚠️ Lỗi ở phần Pivot/Unpivot: {e}")

    print("\n" + "=" * 80)
    print("✅ HOÀN THÀNH CHẠY SPARK DATA TRANSFORMATION")
    print("=" * 80)
    
    spark.stop()

if __name__ == "__main__":
    main()