"""
Airflow DAG: Maintenance Tasks
- Clear Redis stale data (manual trigger)
- Test Spark health check (manual trigger)
Giữ lại logic từ redis_clear_and_history_fetch_dag.py và test_spark_k8s_dag.py
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

BATCH_JOB_PATH = "/opt/spark-jobs/batch"
REDIS_HOST     = "redis"
REDIS_PASSWORD = "redis123"

default_args = {
    "owner":           "crypto_team",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=1),
    "start_date":      datetime(2024, 1, 1),
}

# ── DAG: Clear Redis + kiểm tra hệ thống (manual trigger) ───────────────
with DAG(
    dag_id="maintenance_clear_redis",
    default_args=default_args,
    description="Dọn dẹp Redis stale data (manual trigger)",
    schedule=None,   # Chỉ chạy thủ công
    catchup=False,
    tags=["maintenance", "redis"],
    max_active_runs=1,
) as dag_maintenance:

    # Task 1: Clear Redis stale data
    def clear_redis_data():
        """Xóa data cũ trong Redis, giữ lại ticker và prediction"""
        import redis as redis_lib
        r = redis_lib.Redis(
            host=REDIS_HOST, port=6379,
            password=REDIS_PASSWORD, decode_responses=True
        )
        # Xóa 1m candles cũ hơn 24h
        pattern = "crypto:*:1m:*"
        cursor = 0
        deleted = 0
        while True:
            cursor, keys = r.scan(cursor, match=pattern, count=100)
            for key in keys:
                ttl = r.ttl(key)
                if ttl < 0:  # Key không có TTL hoặc đã hết hạn
                    r.delete(key)
                    deleted += 1
            if cursor == 0:
                break
        print(f"✅ Đã xóa {deleted} stale keys từ Redis")
        r.close()

    clear_redis = PythonOperator(
        task_id="clear_redis_stale_data",
        python_callable=clear_redis_data,
    )

    # Task 2: Kiểm tra kết nối sau khi clear
    check_connections = BashOperator(
        task_id="check_system_health",
        bash_command="""
            echo "🔍 Kiểm tra hệ thống..." && \
            echo "--- Redis ---" && \
            docker exec redis redis-cli -a redis123 ping && \
            echo "--- MongoDB ---" && \
            docker exec mongodb mongosh \
                -u admin -p admin123 \
                --authenticationDatabase admin \
                --eval "db.adminCommand('ping')" --quiet && \
            echo "--- Kafka ---" && \
            docker exec kafka kafka-broker-api-versions \
                --bootstrap-server localhost:9092 > /dev/null && \
            echo "✅ Tất cả services đang hoạt động!"
        """,
    )

    clear_redis >> check_connections


# ── DAG: Test Spark (manual trigger) ────────────────────────────────────
with DAG(
    dag_id="test_spark_health",
    default_args=default_args,
    description="Kiểm tra Spark hoạt động đúng (manual trigger)",
    schedule=None,
    catchup=False,
    tags=["test", "spark", "debug"],
    max_active_runs=1,
) as dag_test:

    # Task 1: Test kubectl/docker
    test_docker = BashOperator(
        task_id="test_docker_connection",
        bash_command="""
            echo "🐳 Kiểm tra Docker containers..." && \
            docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "spark|kafka|mongo|redis" && \
            echo "✅ Docker OK!"
        """,
    )

    # Task 2: Test Spark health check (từ test.py của bạn)
    test_spark = BashOperator(
        task_id="test_spark_simple",
        bash_command=f"""
            echo "⚡ Kiểm tra Spark..." && \
            docker exec spark-master /spark/bin/spark-submit \
                --master local[2] \
                {BATCH_JOB_PATH}/test.py && \
            echo "✅ Spark health check PASSED!"
        """,
    )

    # Task 3: Kiểm tra MongoDB collections
    test_mongodb = BashOperator(
        task_id="test_mongodb_collections",
        bash_command="""
            echo "🗄️  Kiểm tra MongoDB collections..." && \
            docker exec mongodb mongosh \
                -u admin -p admin123 \
                --authenticationDatabase admin \
                crypto_db --quiet \
                --eval "
                    var cols = db.listCollectionNames();
                    print('Collections: ' + cols.join(', '));
                    cols.forEach(c => print(c + ': ' + db[c].countDocuments() + ' docs'));
                " && \
            echo "✅ MongoDB OK!"
        """,
    )

    test_docker >> test_spark >> test_mongodb
