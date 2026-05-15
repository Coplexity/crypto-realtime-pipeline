"""
Airflow DAG: OHLC Aggregation Pipeline
Lịch chạy: 5m → 1h → 4h → 1d
Thay kubectl bằng docker exec spark-submit (Docker Compose mode)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_MASTER   = "spark://spark-master:7077"
BATCH_JOB_PATH = "/opt/spark-jobs/batch"

default_args = {
    "owner":            "crypto_team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "start_date":       datetime(2024, 1, 1),
}

def make_spark_cmd(job_file: str) -> str:
    """Tạo lệnh spark-submit chạy trong container spark-master"""
    return f"""
        echo "🚀 Chạy {job_file}..." && \
        docker exec spark-master /spark/bin/spark-submit \
            --master local[4] \
            --conf spark.sql.session.timeZone=UTC \
            {BATCH_JOB_PATH}/{job_file} && \
        echo "✅ {job_file} hoàn tất!"
    """

# ── DAG 5m: Chạy mỗi 5 phút ─────────────────────────────────────────────
with DAG(
    dag_id="ohlc_5m_aggregator",
    default_args=default_args,
    description="Gom nến 1m → 5m mỗi 5 phút",
    schedule="*/5 * * * *",
    catchup=False,
    tags=["crypto", "ohlc", "spark", "5m"],
    max_active_runs=1,
) as dag_5m:

    aggregate_5m = BashOperator(
        task_id="spark_aggregate_5m",
        bash_command=make_spark_cmd("ohlc_5m_aggregator.py"),
    )

# ── DAG 1h: Chạy đầu mỗi giờ ────────────────────────────────────────────
with DAG(
    dag_id="ohlc_1h_aggregator",
    default_args=default_args,
    description="Gom nến 5m → 1h mỗi giờ",
    schedule="2 * * * *",
    catchup=False,
    tags=["crypto", "ohlc", "spark", "1h"],
    max_active_runs=1,
) as dag_1h:

    aggregate_1h = BashOperator(
        task_id="spark_aggregate_1h",
        bash_command=make_spark_cmd("ohlc_1h_aggregator.py"),
    )

# ── DAG 4h: Chạy mỗi 4 giờ ──────────────────────────────────────────────
with DAG(
    dag_id="ohlc_4h_aggregator",
    default_args=default_args,
    description="Gom nến 1h → 4h mỗi 4 giờ",
    schedule="3 */4 * * *",
    catchup=False,
    tags=["crypto", "ohlc", "spark", "4h"],
    max_active_runs=1,
) as dag_4h:

    aggregate_4h = BashOperator(
        task_id="spark_aggregate_4h",
        bash_command=make_spark_cmd("ohlc_4h_aggregator.py"),
    )

# ── DAG 1d: Chạy lúc 00:05 UTC mỗi ngày ────────────────────────────────
with DAG(
    dag_id="ohlc_1d_aggregator",
    default_args=default_args,
    description="Gom nến 4h → 1d mỗi ngày lúc 00:05 UTC",
    schedule="5 0 * * *",
    catchup=False,
    tags=["crypto", "ohlc", "spark", "1d"],
    max_active_runs=1,
) as dag_1d:

    aggregate_1d = BashOperator(
        task_id="spark_aggregate_1d",
        bash_command=make_spark_cmd("ohlc_1d_aggregator.py"),
    )
