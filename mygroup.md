Work
IT-7 Giai đoạn 1: Khởi tạo & Thiết lập hạ tầng
IT-8 Viết file docker-compose.yml khởi tạo cụm Big Data (Kafka, Spark, HDFS, NoSQL)
IT-9 Thiết lập mạng Docker network và kiểm tra giao tiếp giữa các container
IT-10 Thống nhất quy chuẩn code, khởi tạo Git Repo và phân quyền + Viết tài liệu hướng dẫn (README) cách chạy hệ thống ở local
IT-11 Giai đoạn 2: Thu thập dữ liệu
IT-12 Viết script Python kết nối Binance WebSocket API (cặp BTC/USDT)
IT-13 Khởi tạo Kafka Topic (binance_trades) với cấu hình Partitions phù hợp + Viết code Kafka Producer để đẩy luồng JSON từ Binance vào Topic
IT-14 Viết code Kafka Consumer (nháp) để kiểm tra luồng dữ liệu đầu ra
IT-15 Giai đoạn 3: Xử lý với Spark - Processing
IT-16 Khởi tạo job Spark Structured Streaming lắng nghe Kafka Topic
IT-17 Ép kiểu Schema Parsing: Chuyển chuỗi JSON thành Spark DataFrame
IT-18 Áp dụng Window Function (Tumbling 1m, 5m) tạo nến giá OHLCV (Open, High, Low, Close, Volume)
IT-19 Cấu hình Watermarking (10 giây) để xử lý dữ liệu mạng bị trễ (Late data)
IT-20 Giai đoạn 4: Lưu trữ & Phân phối - Storage
IT-21 Thiết kế Schema/Cấu trúc lưu trữ cho database NoSQL (MongoDB/Redis)
IT-22 Cấu hình Spark Write Stream đẩy kết quả xử lý real-time vào NoSQL
IT-23 Cấu hình Spark Write Stream đẩy dữ liệu gốc định dạng Parquet vào HDFS (Batch backup)
IT-24 Tối ưu hóa tốc độ ghi (Write performance) cho cả hai luồng
IT-25 Giai đoạn 5: Trực quan hóa - Visualization
IT-26 Cài đặt và cấu hình kết nối công cụ Dashboard (Streamlit) với NoSQL + Thiết kế biểu đồ nến Nhật (Candlestick) thể hiện biến động giá
IT-27 Thiết kế biểu đồ đường/cột (Line/Bar) thể hiện khối lượng giao dịch (Volume)
IT-28 Cấu hình cơ chế tự động làm mới (Auto-refresh) mỗi 3-5 giây
IT-29 Giai đoạn cuối: Tối ưu & Báo cáo
IT-30 Kiểm thử chịu lỗi (Chaos testing): Thử ngắt Kafka/Spark để xem khả năng phục hồi
IT-31 Tối ưu hóa phân bổ RAM/CPU cho các Spark Executor
IT-32 Làm báo cáo + quay demo

