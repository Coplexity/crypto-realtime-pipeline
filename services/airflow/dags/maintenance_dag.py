from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

REDIS_HOST     = "redis"
REDIS_PASSWORD = "redis123"

default_args = {
    "owner":           "crypto_team",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=1),
    "start_date":      datetime(2024, 1, 1),
}

# ── DAG: Clear Redis (manual trigger) ───────────────
with DAG(dag_id="maintenance_clear_redis", default_args=default_args, schedule=None, catchup=False, tags=["maintenance", "redis"]) as dag_maintenance:

    def clear_redis_data():
        """Xóa data cũ trong Redis, giữ lại ticker và prediction"""
        import redis as redis_lib
        r = redis_lib.Redis(host=REDIS_HOST, port=6379, password=REDIS_PASSWORD, decode_responses=True)
        pattern = "crypto:*:1m:*"
        cursor = 0
        deleted = 0
        while True:
            cursor, keys = r.scan(cursor, match=pattern, count=100)
            for key in keys:
                if r.ttl(key) < 0:
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

# ── DAG: Test Spark (manual trigger) ────────────────────────────────────
with DAG(dag_id="test_spark_health", default_args=default_args, schedule=None, catchup=False, tags=["test", "spark"]) as dag_test:

    test_spark = KubernetesPodOperator(
        task_id="test_spark_simple",
        name="spark-health-check-pod",
        namespace="crypto-system",
        image="crypto-spark-batch:v1",
        image_pull_policy="Never",
        cmds=["/bin/bash", "-c"],
        arguments=["/spark/bin/spark-submit --master local[2] /opt/spark-jobs/batch/test.py"],
        env_vars={
            "MONGO_URI": "mongodb://admin:admin123@mongodb:27017",
            "MONGO_DB": "crypto_db"
        },
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
    )