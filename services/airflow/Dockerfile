FROM apache/airflow:2.8.0
USER root
# Cài Docker CLI để Airflow gọi được lệnh docker exec
RUN apt-get update && apt-get install -y docker.io && apt-get clean
USER airflow
RUN pip install --no-cache-dir redis pymongo