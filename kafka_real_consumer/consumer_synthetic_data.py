import logging
import os
import threading
import json
import kafka
from confluent_kafka import Consumer, KafkaException, KafkaError

# Configure logging for detailed output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read Kafka configuration from environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')  # Kafka broker URL
VEHICLE_NAME=os.getenv('VEHICLE_NAME', 'e700_4801')

# Validate that KAFKA_BROKER and TOPIC_NAME are set
if not KAFKA_BROKER:
    raise ValueError("Environment variable KAFKA_BROKER is missing.")
if not VEHICLE_NAME:
    raise ValueError("Environment variable VEHICLE_NAME is missing.")

# Kafka consumer configuration
conf_cons = {
    'bootstrap.servers': KAFKA_BROKER,  # Kafka broker URL
    'group.id': f"{VEHICLE_NAME}-consumer-group",  # Consumer group ID for message offset tracking
    'auto.offset.reset': 'earliest'  # Start reading from the earliest message if no offset is present
}

topics=[f"{VEHICLE_NAME}_anomalies", f"{VEHICLE_NAME}_normal_data"]

def deserialize_message(msg):
    """
    Deserialize the JSON-serialized data received from the Kafka Consumer.

    Args:
        msg (Message): The Kafka message object.

    Returns:
        dict or None: The deserialized Python dictionary if successful, otherwise None.
    """
    try:
        # Decode the message and deserialize it into a Python dictionary
        message_value = json.loads(msg.value().decode('utf-8'))
        logging.info(f"Received message from topic {msg.topic}: {message_value}")
        return message_value
    except json.JSONDecodeError as e:
        logging.error(f"Error deserializing message: {e}")
        return None

def kafka_consumer_thread():
    """
    Kafka consumer thread function that reads and processes messages from the Kafka topic.

    This function continuously polls the Kafka topic for new messages, deserializes them,
    and appends them to the global simulate_msg_list or real_msg_list based on the message structure.
    """
    consumer = Consumer(conf_cons)
    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f"End of partition reached: {msg.error()}")
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                continue

            # Deserialize the JSON value of the message
            deserialized_data = deserialize_message(msg)
            if deserialized_data:
                logging.info(f"Received message from topic {msg.topic()}: {deserialized_data}")
            else:
                logging.warning("Deserialized message is None")
    except Exception as e:
        logging.error(f"Error while reading message: {e}")
    finally:
        consumer.close()  # Close the Kafka consumer gracefully

# Start the Kafka consumer thread as a daemon to run in the background
#threading.Thread(target=kafka_consumer_thread, daemon=True).start()

if __name__ == "__main__":
    logging.info(f"Avvio del consumatore per i topic: {topics}")
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
