FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Minimal install - the phase4 README doesn't ship a requirements file.
RUN pip install --no-cache-dir \
        confluent-kafka==2.5.0 \
        psycopg2-binary==2.9.9

COPY phase4_service.py grid_stress_job.py init_db.py ./

# Re-target hardcoded localhost references for the container network.
# DB_PORT is reset to 5432 (Postgres's actual in-container listening port);
# the source files hardcode 5900 to match a teammate's local host-port
# remap (5900:5432), which doesn't apply to the Phase 5 compose network.
RUN sed -i 's|"localhost"|"postgres"|g' phase4_service.py grid_stress_job.py init_db.py \
    && sed -i 's|localhost:9092|kafka:29092|g' phase4_service.py grid_stress_job.py \
    && sed -i 's|DB_PORT = 5900|DB_PORT = 5432|g' phase4_service.py grid_stress_job.py init_db.py

ENV PYTHONUNBUFFERED=1

# Default command is overridden per service in docker-compose.
CMD ["python", "-u", "phase4_service.py"]
