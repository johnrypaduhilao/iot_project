FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# pickle5 only matters on <3.8; skip it on 3.11 to avoid a build failure.
RUN sed -i '/pickle5/d' requirements.txt \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir kafka-python==2.0.2

COPY app.py train_model.py peak_demand_alert.py ./
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Re-target Kafka bootstrap and dataset path for the container network, and
# make the training row cap configurable via TRAIN_NROWS (defaults to the
# original 5,000,000 so behaviour is unchanged unless overridden).
RUN sed -i "s|localhost:9092|kafka:29092|g" app.py \
    && sed -i "s|C:/Users/linfangyyu/Downloads/LOA-5min/LOA.csv|/datasets/LOA-5min/LOA.csv|g" train_model.py peak_demand_alert.py \
    && sed -i "1i import os" train_model.py \
    && sed -i 's|nrows=5000000|nrows=int(os.environ.get("TRAIN_NROWS", "5000000"))|' train_model.py

ENV PYTHONUNBUFFERED=1 \
    MODEL_PATH=/models/xgboost_model.pkl

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
