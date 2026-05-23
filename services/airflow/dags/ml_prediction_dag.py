"""
Airflow DAG: ML Training + Prediction Pipeline (Kubernetes Production Ready)
- Training: Chạy hàng ngày lúc 02:00 UTC, lưu mô hình vào PVC dùng chung
- Prediction: Chạy mỗi 5 phút, tự động kiểm tra mô hình trực tiếp trong Pod trước khi dự đoán
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    "owner":           "crypto_team",
    "depends_on_past": False,
    "retries":         2,
    "retry_delay":     timedelta(minutes=2),
    "start_date":      datetime(2024, 1, 1),
}

# ── Cấu hình Ổ đĩa PVC dùng chung để chia sẻ file mô hình giữa các Pod ──
model_volume = k8s.V1Volume(
    name="model-storage",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="crypto-model-pvc")
)

model_volume_mount = k8s.V1VolumeMount(
    name="model-storage",
    mount_path="/tmp/crypto_lr_model"
)

# ── DAG Training: Chạy 02:00 UTC hàng ngày ──────────────────────────────
with DAG(
    dag_id="crypto_ml_training",
    default_args=default_args,
    description="Train Linear Regression model hàng ngày và lưu vào PVC",
    schedule="0 2 * * *",
    catchup=False,
    tags=["ml", "training", "crypto", "spark"],
    max_active_runs=1,
) as training_dag:

    train_model = KubernetesPodOperator(
        task_id="train_linear_regression",
        name="spark-ml-train",
        namespace="crypto-system",
        image="crypto-spark-batch:v1",
        image_pull_policy="Never",
        cmds=["/bin/bash", "-c"],
        arguments=["/spark/bin/spark-submit --master local[4] --conf spark.sql.session.timeZone=UTC /opt/spark-jobs/batch/train_price_prediction.py"],
        env_vars={
            "MONGO_URI": "mongodb://admin:admin123@mongodb:27017",
            "MONGO_DB": "crypto_db"
        },
        volumes=[model_volume],
        volume_mounts=[model_volume_mount],
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
    )

# ── DAG Prediction: Chạy mỗi 5 phút ────────────────────────────────────
with DAG(
    dag_id="crypto_ml_prediction",
    default_args=default_args,
    description="Dự đoán giá crypto mỗi 5 phút (Kiểm tra điều kiện an toàn inline)",
    schedule="*/5 * * * *",
    catchup=False,
    tags=["ml", "prediction", "crypto", "spark"],
    max_active_runs=1,
) as prediction_dag:

    predict_prices = KubernetesPodOperator(
        task_id="predict_prices",
        name="spark-ml-predict",
        namespace="crypto-system",
        image="crypto-spark-batch:v1",
        image_pull_policy="Never",
        cmds=["/bin/bash", "-c"],
        # Thay thế ShortCircuitOperator: Kiểm tra file trực tiếp bên trong ổ đĩa được mount của Pod
        arguments=[
            """
            if [ -f /tmp/crypto_lr_model/model.json ]; then
                echo "✅ Tìm thấy mô hình tại /tmp/crypto_lr_model/model.json. Bắt đầu dự đoán giá..."
                /spark/bin/spark-submit --master local[4] --conf spark.sql.session.timeZone=UTC /opt/spark-jobs/batch/predict_price.py
            else
                echo "⚠️ Mô hình chưa tồn tại. Vui lòng kích hoạt chạy DAG 'crypto_ml_training' trước. Bỏ qua lượt này!"
            fi
            """
        ],
        env_vars={
            "MONGO_URI": "mongodb://admin:admin123@mongodb:27017",
            "MONGO_DB": "crypto_db",
            "MODEL_PATH": "/tmp/crypto_lr_model"
        },
        volumes=[model_volume],
        volume_mounts=[model_volume_mount],
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
    )