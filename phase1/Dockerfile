FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

ENV KAFKA_BOOTSTRAP=kafka:9092 \
    CSV_PATH=/datasets/LOA-5min/LOA.csv \
    REPLAY_SPEED=200 \
    PYTHONUNBUFFERED=1

CMD ["python", "-u", "producer.py"]
