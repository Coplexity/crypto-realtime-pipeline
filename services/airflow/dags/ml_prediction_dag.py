"""
Airflow DAG: ML Training + Prediction Pipeline
- Training: Chạy hàng ngày lúc 02:00 UTC
- Prediction: Chạy mỗi 5 phút
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
import os

BATCH_JOB_PATH = "/opt/spark-jobs/batch"
MODEL_PATH     = "/tmp/crypto_lr_model"

default_args = {
    "owner":           "crypto_team",
    "depends_on_past": False,
    "retries":         2,
    "retry_delay":     timedelta(minutes=2),
    "start_date":      datetime(2024, 1, 1),
}

# ── DAG Training: Chạy 02:00 UTC hàng ngày ──────────────────────────────
with DAG(
    dag_id="crypto_ml_training",
    default_args=default_args,
    description="Train Linear Regression model hàng ngày",
    schedule="0 2 * * *",
    catchup=False,
    tags=["ml", "training", "crypto", "spark"],
    max_active_runs=1,
) as training_dag:

    train_model = BashOperator(
        task_id="train_linear_regression",
        bash_command=f"""
            echo "🤖 Bắt đầu training ML model..." && \
            docker exec spark-master /spark/bin/spark-submit \
                --master local[4] \
                --conf spark.sql.session.timeZone=UTC \
                --conf spark.driver.memory=2g \
                {BATCH_JOB_PATH}/train_price_prediction.py && \
            echo "✅ Training hoàn tất!" && \
            ls -la {MODEL_PATH}/model.json 2>/dev/null && \
            echo "📊 Model đã được lưu tại {MODEL_PATH}"
        """,
        execution_timeout=timedelta(hours=1),
    )

# ── DAG Prediction: Chạy mỗi 5 phút ────────────────────────────────────
with DAG(
    dag_id="crypto_ml_prediction",
    default_args=default_args,
    description="Dự đoán giá crypto mỗi 5 phút",
    schedule="*/5 * * * *",
    catchup=False,
    tags=["ml", "prediction", "crypto", "spark"],
    max_active_runs=1,
) as prediction_dag:

    # Kiểm tra model tồn tại trước khi chạy prediction
    def check_model_exists():
        model_json = f"{MODEL_PATH}/model.json"
        if os.path.exists(model_json):
            print(f"✅ Model tìm thấy tại {model_json}")
            return True
        print(f"⚠️  Model chưa có tại {model_json}. Chạy training trước!")
        return False

    check_model = ShortCircuitOperator(
        task_id="check_model_exists",
        python_callable=check_model_exists,
    )

    predict_prices = BashOperator(
        task_id="predict_prices",
        bash_command=f"""
            echo "🔮 Bắt đầu dự đoán giá..." && \
            docker exec spark-master /spark/bin/spark-submit \
                --master local[4] \
                --conf spark.sql.session.timeZone=UTC \
                {BATCH_JOB_PATH}/predict_price.py && \
            echo "✅ Prediction hoàn tất!"
        """,
        execution_timeout=timedelta(minutes=10),
    )

    check_model >> predict_prices
