from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    "owner":            "crypto_team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "start_date":       datetime(2024, 1, 1),
}

def create_spark_batch_task(task_id: str, script_name: str, dag: DAG) -> KubernetesPodOperator:
    """Tạo Pod chạy Spark Batch trên Kubernetes"""
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"spark-batch-{task_id.replace('_', '-')}",
        namespace="crypto-system",
        image="crypto-spark-batch:v1",
        image_pull_policy="Never",
        cmds=["/bin/bash", "-c"],
        arguments=[f"/spark/bin/spark-submit --master local[2] /opt/spark-jobs/batch/{script_name}"],
        env_vars={
            "MONGO_URI": "mongodb://admin:admin123@mongodb:27017",
            "MONGO_DB": "crypto_db"
        },
        is_delete_operator_pod=True, # Chạy xong tự dọn rác
        in_cluster=True,
        get_logs=True,
        dag=dag,
    )

# ── DAG 5m: Chạy mỗi 5 phút ─────────────────────────────────────────────
with DAG(dag_id="ohlc_5m_aggregator", default_args=default_args, schedule="*/5 * * * *", catchup=False, tags=["crypto", "5m"]) as dag_5m:
    aggregate_5m = create_spark_batch_task("spark_aggregate_5m", "ohlc_5m_aggregator.py", dag_5m)

# ── DAG 1h: Chạy đầu mỗi giờ ────────────────────────────────────────────
with DAG(dag_id="ohlc_1h_aggregator", default_args=default_args, schedule="2 * * * *", catchup=False, tags=["crypto", "1h"]) as dag_1h:
    aggregate_1h = create_spark_batch_task("spark_aggregate_1h", "ohlc_1h_aggregator.py", dag_1h)

# ── DAG 4h: Chạy mỗi 4 giờ ──────────────────────────────────────────────
with DAG(dag_id="ohlc_4h_aggregator", default_args=default_args, schedule="3 */4 * * *", catchup=False, tags=["crypto", "4h"]) as dag_4h:
    aggregate_4h = create_spark_batch_task("spark_aggregate_4h", "ohlc_4h_aggregator.py", dag_4h)

# ── DAG 1d: Chạy lúc 00:05 UTC mỗi ngày ────────────────────────────────
with DAG(dag_id="ohlc_1d_aggregator", default_args=default_args, schedule="5 0 * * *", catchup=False, tags=["crypto", "1d"]) as dag_1d:
    aggregate_1d = create_spark_batch_task("spark_aggregate_1d", "ohlc_1d_aggregator.py", dag_1d)