#!/bin/bash

# IT-13: Kafka Topic Setup Script
# Script để khởi tạo Kafka Topic (binance_trades) với cấu hình phù hợp
# Chạy script này sau khi Kafka container đang chạy

set -e

# Cấu hình
KAFKA_CONTAINER="kafka"
KAFKA_INTERNAL_PORT="9092"
TOPIC_NAME="binance_trades"
NUM_PARTITIONS=3
REPLICATION_FACTOR=1
RETENTION_MS=$((30 * 24 * 60 * 60 * 1000))  # 30 ngày

echo "================================"
echo "🔧 PHASE 2 - Kafka Topic Setup"
echo "IT-13: Khởi tạo Kafka Topic"
echo "================================"

# Kiểm tra xem Kafka container có chạy không
if ! docker ps | grep -q "$KAFKA_CONTAINER"; then
    echo "❌ Lỗi: Container '$KAFKA_CONTAINER' không chạy"
    echo "💡 Hãy chạy: docker compose up -d zookeeper kafka"
    exit 1
fi

echo "✅ Kafka container đang chạy"

# Kiểm tra topic đã tồn tại chưa
echo ""
echo "📋 Danh sách topic hiện tại:"
docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server localhost:$KAFKA_INTERNAL_PORT --list

# Xoá topic cũ nếu tồn tại (optional)
if docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server localhost:$KAFKA_INTERNAL_PORT --list | grep -q "^${TOPIC_NAME}$"; then
    echo ""
    echo "⚠️  Topic '$TOPIC_NAME' đã tồn tại"
    read -p "Bạn có muốn xoá và tạo lại? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  Đang xoá topic cũ..."
        docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server localhost:$KAFKA_INTERNAL_PORT --delete --topic $TOPIC_NAME
        sleep 2
    else
        echo "⏭️  Bỏ qua bước xoá"
    fi
else
    echo "✅ Topic mới sẽ được tạo"
fi

# Tạo topic mới
echo ""
echo "🚀 Đang tạo topic '$TOPIC_NAME'..."
docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server localhost:$KAFKA_INTERNAL_PORT \
    --create \
    --topic $TOPIC_NAME \
    --partitions $NUM_PARTITIONS \
    --replication-factor $REPLICATION_FACTOR \
    --config retention.ms=$RETENTION_MS \
    --config compression.type=snappy \
    --config min.insync.replicas=1

echo ""
echo "✅ Topic tạo thành công!"

# Hiển thị thông tin topic
echo ""
echo "📊 Thông tin topic:"
docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server localhost:$KAFKA_INTERNAL_PORT \
    --describe \
    --topic $TOPIC_NAME

echo ""
echo "✅ Setup hoàn tất!"
echo ""
echo "💡 Các lệnh hữu ích:"
echo "   - Xem thông tin topic: docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic $TOPIC_NAME"
echo "   - Xem messages: docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $TOPIC_NAME --from-beginning"
echo "   - Đếm messages: docker exec kafka kafka-run-class kafka.tools.JmxTool --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"
