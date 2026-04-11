import json
import os
from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

# Config
KAFKA_BROKER = "localhost:9092"
MINIO_URL = "localhost:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET_NAME = "energy-lake"

# Initialize MinIO client
s3 = Minio(MINIO_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

def main():
    # Subscribe to all 4 topics simultaneously
    consumer = KafkaConsumer(
        "rte.generation", 
        "rte.consumption", 
        "rte.physical_flows", 
        "weather.paris",
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        group_id="bronze-consumer-group",
        # CHANGE THIS LINE from value_serializer to value_deserializer
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("🚀 Bronze Consumer started. Waiting for messages...")

    for msg in consumer:
        topic = msg.topic
        data = msg.value
        
        # Hive-style partitioning: bronze/{topic}/year=YYYY/month=MM/day=DD/hour=HH/{timestamp}.json [cite: 70, 71]
        now = datetime.now()
        path = f"bronze/{topic}/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/{int(now.timestamp())}.json"
        
        # Write to MinIO 
        content = json.dumps(data).encode('utf-8')
        s3.put_object(BUCKET_NAME, path, BytesIO(content), len(content), content_type="application/json")
        print(f"📥 Saved message from {topic} to {path}")

if __name__ == "__main__":
    main()