FROM apache/airflow:2.8.0

# Trở lại user mặc định của Airflow
USER airflow

# Cài đặt các package cần thiết, ĐẶC BIỆT là provider cho KubernetesPodOperator
RUN pip install --no-cache-dir \
    redis \
    pymongo \
    apache-airflow-providers-cncf-kubernetes

# BƯỚC QUAN TRỌNG NHẤT: Copy thư mục dags vào trong image
# Giả định bạn đang đứng ở thư mục gốc project khi chạy lệnh build
COPY ./services/airflow/dags /opt/airflow/dags