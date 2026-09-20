import os
import json
import time
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","datafabric01.ezmeral.demo.hpelabs.fr:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","video-event")

def main():
    # Initialize the kafka-python producer pointing to the Data Fabric Kafka gateway
    print(f"Open Consumer {KAFKA_BOOTSTRAP_SERVERS}  topic: {KAFKA_TOPIC}")

    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],                
        api_version=(3, 6, 0),
        auto_offset_reset='earliest',
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username='mapr', 
        sasl_plain_password='mapr'
    )


    consumer.subscribe(topics=[KAFKA_TOPIC])
    numMsgConsumed = 0
    for _ in range(100):
        records = consumer.poll(timeout_ms=500)
        for topic_data, consumer_records in records.items():
            for consumer_record in consumer_records:
                print("Received message: " + str(consumer_record.value.decode('utf-8')))
                numMsgConsumed += 1

    print("Messages consumed: " + str(numMsgConsumed))



if __name__ == '__main__':
    main()