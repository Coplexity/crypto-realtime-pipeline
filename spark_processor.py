"""
Spark Structured Streaming Processor

Xử lý dữ liệu stream real-time từ Kafka topic để tính toán OHLCV
và các chỉ số kỹ thuật. Hỗ trợ windowing và watermarking.

Phase 3: Stream processing và aggregation
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, max, min, avg, sum, count,
    first, last
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)


# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "CryptoProcessor") -> SparkSession:
    """Create optimized Spark session for streaming."""
    return SparkSession.builder \
        .appName(app_name) \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        ) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
        .getOrCreate()


def get_schema() -> StructType:
    """Define JSON schema for trade messages."""
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("trade_id", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("buyer_order_id", LongType(), True),
        StructField("seller_order_id", LongType(), True),
        StructField("trade_time", LongType(), True),
        StructField("is_buyer_maker", StringType(), True),
        StructField("is_best_match", StringType(), True),
        StructField("ingestion_timestamp", LongType(), True),
    ])


def main():
    """Main streaming processor."""
    
    logger.info("=" * 70)
    logger.info("PHASE 3: Spark Structured Streaming Processor")
    logger.info("=" * 70)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Kết nối tới Kafka...")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "binance_trades") \
        .option("startingOffsets", "latest") \
        .load()
    
    logger.info("✓ Kết nối Kafka thành công")
    
    # Parse JSON schema
    schema = get_schema()
    
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    # Convert timestamp to proper format
    processed_df = parsed_df \
        .withColumn("timestamp", (col("trade_time") / 1000).cast("timestamp"))
    
    logger.info("Tính toán OHLCV (1 phút)...")
    
    # Calculate 1-minute OHLCV
    ohlcv_df = processed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("symbol")
        ).agg(
            first("price").alias("open_price"),
            max("price").alias("high_price"),
            min("price").alias("low_price"),
            last("price").alias("close_price"),
            sum("quantity").alias("volume"),
            count("trade_id").alias("trade_count"),
            avg("price").alias("avg_price"),
        )
    
    # Add datetime column for better display
    final_df = ohlcv_df \
        .selectExpr(
            "window.start as start_time",
            "window.end as end_time",
            "symbol",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "trade_count",
            "avg_price",
        )
    
    # Write to console (can be replaced with sink)
    query = final_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 100) \
        .start()
    
    logger.info("Bắt đầu processing. (Ctrl+C để dừng)")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Dừng processing...")
    finally:
        query.stop()
        spark.stop()
        logger.info("✓ Spark session đóng")


if __name__ == "__main__":
    main()