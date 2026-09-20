import os
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","datafabric01.ezmeral.demo.hpelabs.fr:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","test")

def main():
    admin_client = KafkaAdminClient( 
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(3, 6, 0),
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username='mapr', 
        sasl_plain_password='mapr'
    )

    try:
        result = admin_client.delete_topics(topics=KAFKA_TOPIC, timeout_ms=30000)
        print(f"Topic '{KAFKA_TOPIC}' successfully deleted.")
    except UnknownTopicOrPartitionError:
        print(f"Topic '{KAFKA_TOPIC}' does not exist.")
    except Exception as e:
        print(f"Failed to delete topic '{KAFKA_TOPIC}': {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    main()