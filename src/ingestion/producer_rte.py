import json
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "explore"))
from test_rte import get_rte_token

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
POLL_INTERVAL_SECONDS = 1800  # 30 minutes

ENDPOINTS = {
    "rte.generation":     "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type",
    "rte.consumption":    "https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term",
    "rte.physical_flows": "https://digital.iservices.rte-france.com/open_api/physical_flow/v1/physical_flows",
}

# ── Kafka producer setup ──────────────────────────────────────────────────────
def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",           # wait for all replicas to confirm
        retries=3,
    )

# ── Fetch one endpoint with retry ─────────────────────────────────────────────
def fetch_with_retry(url, headers, max_retries=3):
    import requests
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=headers, timeout=30)
            if res.status_code == 200:
                return res.json()
            log.warning(f"HTTP {res.status_code} on attempt {attempt+1}: {res.text[:200]}")
        except Exception as e:
            log.warning(f"Request failed attempt {attempt+1}: {e}")
        time.sleep(10 * (attempt + 1))  # 10s, 20s, 30s backoff
    return None

# ── Publish one API response to Kafka ─────────────────────────────────────────
def publish(producer, topic, data):
    top_key = list(data.keys())[0]
    records = data[top_key]
    published = 0

    for record in records:
        # Filter consumption: only keep REALISED records, skip forecasts
        if topic == "rte.consumption" and record.get("type") != "REALISED":
            continue

        message = {
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "topic": topic,
            "payload": record,
        }

        producer.send(
            topic,
            key=topic,
            value=message,
        )
        published += 1

    producer.flush()
    log.info(f"✅ Published {published} records to {topic}")

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    log.info("Starting RTE producer...")
    producer = make_producer()
    token = None
    token_fetched_at = 0

    while True:
        # Refresh token if older than 90 minutes (expires at 120)
        if time.time() - token_fetched_at > 5400:
            log.info("Refreshing RTE token...")
            token = get_rte_token()
            token_fetched_at = time.time()

        headers = {"Authorization": f"Bearer {token}"}

        for topic, url in ENDPOINTS.items():
            log.info(f"Fetching {topic}...")
            data = fetch_with_retry(url, headers)
            if data:
                publish(producer, topic, data)
            else:
                log.error(f"❌ Failed to fetch {topic} after all retries")

        log.info(f"Sleeping {POLL_INTERVAL_SECONDS // 60} minutes until next poll...")
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()