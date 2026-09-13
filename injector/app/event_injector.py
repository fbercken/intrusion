import os
import json
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","")

def main():
    # Initialize the kafka-python producer pointing to the Data Fabric Kafka gateway
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Optional: Add security configs here if SASL/SSL is enabled on your cluster
        # security_protocol='SASL_PLAINTEXT',
        # sasl_mechanism='PLAIN',
        # sasl_plain_username='your_user',
        # sasl_plain_password='your_password'
    )

    print(f"Producing messages to HPE Data Fabric via kafka-python [Topic: {KAFKA_TOPIC}]...")

    try:
        for i in range(10):
            payload = {
                'event_id': i,
                'status': 'active',
                'timestamp': time.time()
            }
            
            # Send message asynchronously
            future = producer.send(KAFKA_TOPIC, value=payload)
            
            # Block briefly for confirmation to ensure delivery
            record_metadata = future.get(timeout=10)
            print(f"Delivered to [Partition: {record_metadata.partition}, Offset: {record_metadata.offset}]")
            
            time.sleep(1)

    except Exception as e:
        print(f"An error occurred during production: {e}")
        
    finally:
        # Ensure all buffered messages are sent and connection closes cleanly
        producer.flush()
        producer.close()
        print("Producer closed.")

if __name__ == '__main__':
    main()