#!/usr/bin/env python3
"""
Job Kiểm tra sức khỏe hệ thống Spark (Simple Spark Test Job)
Chỉ test các chức năng lõi của Spark, không yêu cầu kết nối với Redis hay MongoDB.
Thường dùng để check lỗi lúc cấu hình cụm Kubernetes.
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, max as spark_max, min as spark_min

def main():
    try:
        # Sửa lỗi Worker cho Windows nếu chạy Local
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        # Khởi tạo Spark Session
        spark = SparkSession.builder \
            .appName("Spark-Health-Check-Job") \
            .getOrCreate()
        
        # Tắt bớt log INFO dài dòng
        spark.sparkContext.setLogLevel("WARN")

        print("=" * 60)
        print("🚀 BẮT ĐẦU JOB KIỂM TRA SỨC KHỎE SPARK")
        print("=" * 60)
        
        # ---------------------------------------------------------
        print("\n📊 Bài Test 1: Khởi tạo DataFrame với Dữ liệu giả (Mock Data)...")
        # Đã cập nhật số liệu giá sát với thực tế hơn
        sample_data = [
            ("BTCUSDT", 71500.0, 12.5, 1712707200000),
            ("BTCUSDT", 71884.5, 15.2, 1712707260000),
            ("BTCUSDT", 71420.3, 10.3, 1712707320000),
            ("ETHUSDT", 3500.0, 150.1, 1712707200000),
            ("ETHUSDT", 3550.5, 120.5, 1712707260000),
            ("ETHUSDT", 3490.8, 180.8, 1712707320000),
            ("BNBUSDT", 605.0, 500.0, 1712707200000),
            ("BNBUSDT", 612.5, 450.0, 1712707260000),
            ("BNBUSDT", 598.0, 600.0, 1712707320000),
        ]
        
        columns = ["symbol", "price", "volume", "timestamp"]
        df = spark.createDataFrame(sample_data, columns)
        
        print(f"✅ Đã tạo DataFrame chứa {df.count()} dòng")
        print("   Bảng dữ liệu mẫu:")
        df.show(truncate=False)
        
        # ---------------------------------------------------------
        print("\n📊 Bài Test 2: Thử nghiệm Lọc dữ liệu (Chỉ lấy BTCUSDT)...")
        btc_df = df.filter(col("symbol") == "BTCUSDT")
        btc_count = btc_df.count()
        print(f"✅ Đã lọc thành công {btc_count} dòng dữ liệu của BTCUSDT")
        btc_df.show(truncate=False)
        
        # ---------------------------------------------------------
        print("\n📊 Bài Test 3: Thử nghiệm Gom nhóm và Tính toán song song (GroupBy & Aggregation)...")
        aggregated_df = df.groupBy("symbol").agg(
            spark_sum("volume").alias("total_volume"),
            avg("price").alias("avg_price"),
            spark_max("price").alias("max_price"),
            spark_min("price").alias("min_price")
        )
        
        print("✅ Kết quả tính toán tổng hợp:")
        aggregated_df.show(truncate=False)
        
        # ---------------------------------------------------------
        print("\n📊 Bài Test 4: Thu thập kết quả về Master Node (Action: Collect)...")
        results = aggregated_df.collect()
        print(f"✅ Đã kéo thành công {len(results)} dòng kết quả về")
        
        for row in results:
            print(f"  - {row['symbol']}: Tổng Volume = {row['total_volume']:.2f}, "
                  f"Giá trung bình = ${row['avg_price']:.2f}, "
                  f"Đỉnh = ${row['max_price']:.2f}, "
                  f"Đáy = ${row['min_price']:.2f}")
        
        # ---------------------------------------------------------
        print("\n📊 Bài Test 5: Trích xuất thông tin môi trường Spark...")
        print(f"  - Phiên bản Spark (Version): {spark.version}")
        print(f"  - Tên ứng dụng (App Name): {spark.sparkContext.appName}")
        print(f"  - Điểm điều khiển (Master): {spark.sparkContext.master}")
        print(f"  - Mức độ tính toán song song mặc định: {spark.sparkContext.defaultParallelism} luồng")
        
        print("\n" + "=" * 60)
        print("✅ TUYỆT VỜI! TẤT CẢ CÁC BÀI TEST ĐỀU PASS 100%. LÕI SPARK ĐANG HOẠT ĐỘNG HOÀN HẢO!")
        print("=" * 60)
        
        # Dọn dẹp tài nguyên
        spark.stop()
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ Phát hiện lỗi trong hệ thống Spark: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        if 'spark' in locals():
            spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()