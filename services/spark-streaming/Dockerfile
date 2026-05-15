FROM bde2020/spark-master:3.2.1-hadoop3.2

USER root

# Cài gcc để build pymongo + các thư viện Python
RUN apk add --no-cache gcc musl-dev && \
    pip3 install --no-cache-dir \
        redis==4.6.0 \
        pymongo==3.12.3 \
        python-dotenv==0.21.1 \
        prometheus-client==0.20.0

# Download Kafka JARs cần thiết cho Spark 3.2.1
RUN curl -fsSL -o /spark/jars/spark-sql-kafka-0-10_2.12-3.2.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.1/spark-sql-kafka-0-10_2.12-3.2.1.jar \
 && curl -fsSL -o /spark/jars/spark-token-provider-kafka-0-10_2.12-3.2.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.2.1/spark-token-provider-kafka-0-10_2.12-3.2.1.jar \
 && curl -fsSL -o /spark/jars/kafka-clients-2.8.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar \
 && curl -fsSL -o /spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Copy streaming job vào image
COPY streaming_job.py /opt/spark-jobs/streaming/streaming_job.py

