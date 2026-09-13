import os
import json
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","datafabric01.ezmeral.demo.hpelabs.fr:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","video-event")

def main():
    # Initialize the kafka-python producer pointing to the Data Fabric Kafka gateway
    print(f"Open Producer {KAFKA_BOOTSTRAP_SERVERS}  topic: {KAFKA_TOPIC}")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        enable_idempotence=False,
        api_version=(3, 6, 0),
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username='mapr', 
        sasl_plain_password='mapr'
    )

    print(f"Producing messages to HPE Data Fabric via kafka-python [Topic: {KAFKA_TOPIC}]...")

    try:
        i = 0
        while True:
        #for i in range(10):
            payload = {
                'event_id': i,
                'status': 'active', 
                'timestamp': time.time()
            }
            
            # Send message asynchronously
            message_bytes = json.dumps(payload).encode('utf-8')
            #future = producer.send(KAFKA_TOPIC, value=message_bytes)
            
            # Block briefly for confirmation to ensure delivery
            #record_metadata = future.get(timeout=10)
            #print(f"Delivered to [Partition: {record_metadata.partition}, Offset: {record_metadata.offset}]")
            
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