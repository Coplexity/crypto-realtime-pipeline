#!/bin/bash
# Submit Spark Streaming job
# Chạy: bash services/spark-streaming/submit.sh

docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
  --conf spark.sql.shuffle.partitions=6 \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.executor.memory=1g \
  /opt/spark-jobs/streaming/streaming_job.py