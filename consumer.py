import json
import logging

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ["localhost:19092"]
TOPIC = "financial-transactions"
GROUP_ID = "test-consumer-group"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            key_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
        )
        logger.info("Kafka consumer created successfully")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        return None


def consume_messages():
    consumer = create_consumer()
    if not consumer:
        return

    try:
        logger.info("Starting to consume messages...")
        for message in consumer:
            logger.info(f"Received message: Key={message.key}, Partition={message.partition}, Offset={message.offset}")
            logger.info(f"Transaction: {json.dumps(message.value, indent=2)}")
            print("-" * 80)

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_messages()
