FROM python:3.11-slim

RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir requests python-dotenv kafka-python

# Copy ingestion scripts only
COPY src/ingestion/ ./src/ingestion/

RUN chown -R appuser:appuser /app
USER appuser

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV PYTHONPATH=/app

# Run both producers in background, consumer in foreground
CMD ["sh", "-c", \
     "python3 src/ingestion/producer_rte.py & \
      python3 src/ingestion/producer_weather.py & \
      python3 src/ingestion/consumer_bronze.py"]
