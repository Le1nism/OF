import logging
import os
import threading
import json

import kafka
from flask import Flask, jsonify, render_template, request
from confluent_kafka import Consumer, KafkaException, KafkaError
from pyexpat.errors import messages

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# Read Kafka configuration from environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'train-sensor-data')

# Validate that KAFKA_BROKER and TOPIC_NAME are properly set
if not KAFKA_BROKER:
    raise ValueError("Environment variable KAFKA_BROKER is missing.")
if not TOPIC_NAME:
    raise ValueError("Environment variable TOPIC_NAME is missing.")

msg_list = []
MAX_MESSAGES = 100

# Configure the Kafka consumer
conf_cons = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'kafka-consumer-group-1',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start reading at the earliest message
}

def deserialize_message(msg):
    """Deserialize the JSON-serialized data received from the Consumer.

    Args:
        msg (Message): The Kafka message object.

    Returns:
        dict or None: The deserialized Python dictionary if successful, otherwise None.
    """
    try:
        # Decode the message and deserialize it into a Python dictionary
        message_value = json.loads(msg.value().decode('utf-8'))
        return message_value
    except json.JSONDecodeError as e:
        logging.error(f"Error deserializing message: {e}")
        return None

def kafka_consumer_thread():
    """Kafka consumer thread function that reads and processes messages from the Kafka topic."""
    consumer = Consumer(conf_cons)
    consumer.subscribe([TOPIC_NAME])
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
                logging.info(f"Deserialized message: {deserialized_data}")
                msg_list.append(deserialized_data)
            else:
                logging.warning("Deserialized message is None")

            # Keep only the last MAX_MESSAGES messages
            if len(msg_list) > MAX_MESSAGES:
                msg_list.pop(0)
    except Exception as e:
        logging.error(f"Error while reading message: {e}")
    finally:
        consumer.close()

# Start the Kafka consumer thread
threading.Thread(target=kafka_consumer_thread, daemon=True).start()

# Start the Flask web application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

# Flask route to retrieve the last 100 messages
@app.route('/', methods=['GET'])
def home():
    """Render the home page.

    Returns:
        str: The rendered template for the index page.
    """
    return render_template('index.html')

@app.route('/datavisualization')
def get_data():
    """Render the data visualization page with the last 100 messages.

    Returns:
        str: The rendered template with message data.
    """
    return render_template('trainsensordatavisualization.html', messages=msg_list[-100:])

@app.route('/by-type')
def get_data_by_type():
    """Render the data visualization page sorted by type.

    Returns:
        str: The rendered template with sorted message data by type.
    """
    sorted_data_by_type = sort_data_by_type(msg_list[-100:])
    return render_template('trainsensordatavisualization.html', messages=sorted_data_by_type)

def order_by(str1, str2):
    """Retrieve the value of a specific request argument or return a default.

    Args:
        str1 (str): The name of the request argument to retrieve.
        str2 (str): The default value to return if the argument is not found.

    Returns:
        str: The value of the argument or the default value.
    """
    return request.args.get(str1, str2)

def order_by_type():
    """Get the order parameter for sensor_type.

    Returns:
        str: The order parameter for sensor_type, defaults to 'sensor_type'.
    """
    return order_by('order_by_type', 'sensor_type')

def sort_data_by_type(var):
    """Sort the given sensor data by type.

    Args:
        var (list): The list of sensors to sort.

    Returns:
        list: The sorted list of sensors by type.
    """
    return sorted(var, key=lambda x: x[order_by_type()])
