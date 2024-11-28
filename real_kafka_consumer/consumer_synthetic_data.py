import argparse
import logging
import os
import threading
import json
import time
from lib2to3.pgen2.parse import Parser

from confluent_kafka import Consumer, KafkaError

# Configure logging for detailed output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read Kafka configuration from environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')  # Kafka broker URL
VEHICLE_NAME=os.getenv('VEHICLE_NAME', 'e700')
active_consumers={}

# Validate that KAFKA_BROKER and TOPIC_NAME are set
if not KAFKA_BROKER:
    raise ValueError("Environment variable KAFKA_BROKER is missing.")

# List to store received messages and a constant for the maximum number of stored messages
real_msg_list = []  # Stores real sensor messages
anomalies_msg_list = []
normal_msg_list = []

received_all_real_msg = 0
received_anomalies_msg = 0
received_normal_msg = 0


logging.info(f"Starting consumer system for vehicle: {VEHICLE_NAME}")

def create_consumer(vehicle_name):
    # Kafka consumer configuration
    conf_cons = {
        'bootstrap.servers': KAFKA_BROKER,  # Kafka broker URL
        'group.id': f'{vehicle_name}-consumer-group',  # Consumer group ID for message offset tracking
        'auto.offset.reset': 'earliest'  # Start reading from the earliest message if no offset is present
    }
    return Consumer(conf_cons)


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
        logging.info(f"Received message from topic {msg.topic()}: {message_value}")
        return message_value
    except json.JSONDecodeError as e:
        logging.error(f"Error deserializing message: {e}")
        return None


def consume_vehicle_data(vehicle_name):
    global real_msg_list, anomalies_msg_list, normal_msg_list, received_all_real_msg, received_anomalies_msg, received_normal_msg
    consumer = create_consumer(vehicle_name)
    topic_anomalies = f"{vehicle_name}_anomalies"
    topic_normal_data = f"{vehicle_name}_normal_data"

    consumer.subscribe([topic_anomalies, topic_normal_data])
    logging.info(f"Started consumer for [{vehicle_name}] ...")

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll per 1 secondo
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f"End of partition reached for {msg.topic()}")
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                continue

            deserialized_data = deserialize_message(msg)
            if deserialized_data:
                logging.info(f"Processing message from topic {msg.topic()}: {deserialized_data}")
                if msg.topic() == topic_anomalies:
                    logging.info(f"ANOMALIES - Deserialized message: {deserialized_data}")
                    anomalies_msg_list.append(deserialized_data)
                    real_msg_list.append(deserialized_data)

                    received_all_real_msg += 1
                    received_anomalies_msg += 1

                    print(f"DATA - {deserialized_data}")

                elif msg.topic() == topic_normal_data:
                    logging.info(f"NORMAL DATA - Deserialized message: {deserialized_data}")
                    normal_msg_list.append(deserialized_data)
                    real_msg_list.append(deserialized_data)

                    received_all_real_msg += 1
                    received_normal_msg += 1

                    print(f"DATA - {deserialized_data}")

    except Exception as e:
        logging.error(f"Error in consumer for {vehicle_name}: {e}")
    finally:
        consumer.close()
        logging.info(f"Consumer for {vehicle_name} closed.")

def start_new_consumers(vehicle_name):
    if vehicle_name in active_consumers:
        logging.info(f"Consumer for {vehicle_name} is already running.")
        return
    thread_consumer = threading.Thread(target=consume_vehicle_data, args=(vehicle_name,))
    thread_consumer.daemon=True
    active_consumers[vehicle_name] = thread_consumer
    thread_consumer.start()
    logging.info(f"Consumer thread started for vehicle: {vehicle_name}")

def monitor_start_consumers(vehicle_list):
    for vehicle_name in vehicle_list:
        if vehicle_name not in active_consumers or not active_consumers[vehicle_name].is_alive():
            logging.info(f"Restarting consumer for {vehicle_name}")
            start_new_consumers(vehicle_name)


def discover_vehicle_topics():
    from confluent_kafka.admin import AdminClient
    try:
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
        topic_metadata = admin_client.list_topics(timeout=10)
        topics = topic_metadata.topics.keys()
    except Exception as e:
        logging.error(f"Failed to fetch topic metadata: {e}")
        return []

    # Filtra i topic con convenzione di naming '_anomalies' o '_normal_data'
    vehicle_list=set()
    for topic in topics:
        if topic.endswith('_anomalies') or topic.endswith('_normal_data'):
            vehicle_name="_".join(topic.split('_')[:2])
            vehicle_list.add(vehicle_name)

    return list(vehicle_list)

if __name__=="__main__":
    logging.info("Starting dynamic Kafka consumer system >>> ")
    try:
        while True:
            vehicle_list=discover_vehicle_topics()
            logging.info(f"Discovered vehicles: {vehicle_list}")
            monitor_start_consumers(vehicle_list)
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Shutting down dynamic consumer system")
        for consumer in active_consumers.values():
            consumer.join()
            logging.info("All consumers have been shut down.")