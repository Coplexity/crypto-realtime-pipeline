from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, max, min, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# 1. Khởi tạo Spark Session với thư viện kết nối Kafka
spark = SparkSession.builder \
    .appName("CryptoRealtimeProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Giảm bớt log rác của Spark để dễ nhìn kết quả
spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa cấu trúc (Schema) của cục JSON gửi từ Binance
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

print("⏳ Đang kết nối tới Kafka để lắng nghe dữ liệu...")

# 3. Đọc dữ liệu từ Kafka (Topic: binance_trades)
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "binance_trades") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Tiền xử lý dữ liệu
# Ép kiểu dữ liệu Kafka (đang ở dạng byte) sang String, rồi parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Chuyển đổi timestamp (mili-giây) sang định dạng thời gian chuẩn của Spark (TimestampType)
processed_df = parsed_df \
    .withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))

# 5. Phân tích & Tính toán: Tạo Nến giá 1 Phút (Tumbling Window)
# Áp dụng Watermark 10 giây để xử lý dữ liệu đến trễ do mạng lag
windowed_df = processed_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("symbol")
    ).agg(
        max("price").alias("high_price"),
        min("price").alias("low_price"),
        avg("price").alias("avg_price"),
        sum("quantity").alias("total_volume")
    )

# 6. Đẩy kết quả ra màn hình Terminal để xem trực tiếp
query = windowed_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()