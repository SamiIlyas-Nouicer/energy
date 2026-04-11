import json
import time
import requests
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

# Config
KAFKA_BROKER = "localhost:9092"
TOPIC = "weather.paris"
POLL_INTERVAL_SECONDS = 3600  # Poll every hour as per Open-Meteo updates

def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1
    )

def fetch_weather():
    # Paris coordinates (Asnières-sur-Seine vicinity)
    url = "https://api.open-meteo.com/v1/forecast?latitude=48.85&longitude=2.35&hourly=temperature_2m&timezone=Europe%2FBerlin"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        log.error(f"❌ Weather fetch failed: {e}")
        return None

def main():
    producer = make_producer()
    log.info("Starting Weather producer...")
    
    while True:
        data = fetch_weather()
        if data:
            message = {
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "topic": TOPIC,
                "payload": data
            }
            producer.send(TOPIC, value=message)
            producer.flush()
            log.info(f"✅ Published weather update to {TOPIC}")
        
        log.info(f"Sleeping {POLL_INTERVAL_SECONDS // 60} minutes until next poll...")
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()