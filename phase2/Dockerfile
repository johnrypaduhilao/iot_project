# Phase 2 - PyFlink feature engineering job
# Requires Java because PyFlink ships a JVM-based runtime.
FROM python:3.10-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flink_feature_engineering.py .
COPY flink-sql-connector-kafka-4.0.1-2.0.jar .

# Re-target the Kafka bootstrap from `localhost:9092` to the docker-compose
# service name without modifying the original script in git.
RUN sed -i 's|localhost:9092|kafka:29092|g' flink_feature_engineering.py

CMD ["python", "-u", "flink_feature_engineering.py"]
