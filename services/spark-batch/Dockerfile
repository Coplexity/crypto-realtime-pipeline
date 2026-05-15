FROM bde2020/spark-master:3.2.1-hadoop3.2

USER root

# Bổ sung g++, linux-headers và python3-dev để biên dịch lõi C của Numpy
RUN apk add --no-cache gcc g++ musl-dev linux-headers python3-dev && \
    pip3 install --no-cache-dir \
        redis==4.6.0 \
        pymongo==3.12.3 \
        python-dotenv==0.21.1 \
        numpy \
        prometheus_client

COPY . /opt/spark-jobs/batch/