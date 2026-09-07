import time
import json
from kafka import KafkaProducer


BOOTSTRAP_SERVERS="9092"

def initialize_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5
            )
            print("Connected to streaming event backbone.")
            return producer
        except Exception as e:
            print(f"Retrying connection to event broker... Error: {e}")
            time.sleep(5)