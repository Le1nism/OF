import logging
import os
import threading
import json
import kafka
from flask import Flask, jsonify, render_template, request
from confluent_kafka import Consumer, KafkaException, KafkaError
from pyexpat.errors import messages

# Configure logging for detailed output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a Flask app instance
app = Flask(__name__)

# Read Kafka configuration from environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'train-sensor-data')

# Validate that KAFKA_BROKER and TOPIC_NAME are set
if not KAFKA_BROKER:
    raise ValueError("Environment variable KAFKA_BROKER is missing.")
if not TOPIC_NAME:
    raise ValueError("Environment variable TOPIC_NAME is missing.")

# List to store received messages and a constant for the maximum number of stored messages
simulate_msg_list = []
real_msg_list = []
MAX_MESSAGES = 100

# Kafka consumer configuration
conf_cons = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'kafka-consumer-group-1',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start reading at the earliest message
}

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
        return message_value
    except json.JSONDecodeError as e:
        logging.error(f"Error deserializing message: {e}")
        return None

def kafka_consumer_thread():
    """
    Kafka consumer thread function that reads and processes messages from the Kafka topic.

    This function continuously polls the Kafka topic for new messages, deserializes them,
    and appends them to the global simulate_msg_list.
    """
    consumer = Consumer(conf_cons)
    consumer.subscribe([TOPIC_NAME])
    global simulate_msg_list, real_msg_list
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
                num_keys=len(deserialized_data.keys())
                if num_keys == 5:
                    logging.info(f"5K - Deserialized message: {deserialized_data}")
                    simulate_msg_list.append(deserialized_data)
                    simulate_msg_list=simulate_msg_list[-MAX_MESSAGES:]
                else:
                    logging.info(f"RK - Deserialized message: {deserialized_data}")
                    real_msg_list.append(deserialized_data)
                    real_msg_list=real_msg_list[-MAX_MESSAGES:]
            else:
                logging.warning("Deserialized message is None")

            # Keep only the last MAX_MESSAGES messages
            if len(simulate_msg_list) > MAX_MESSAGES:
                simulate_msg_list.pop(0)
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
    """
    Render the home page.

    Returns:
        str: The rendered template for the index page.
    """
    return render_template('index.html')

@app.route('/my-all-data')
def get_data():
    """
    Render the data visualization page with the last 100 messages.

    Returns:
        str: The rendered template with message data.
    """
    return render_template('trainsensordatavisualization.html', messages=simulate_msg_list[-100:])

@app.route('/my-all-data-by-type')
def get_data_by_type():
    """
    Render the data visualization page sorted by sensor type.

    Returns:
        str: The rendered template with sorted message data by type.
    """
    sorted_data_by_type = sort_data_by_type(simulate_msg_list[-100:])
    return render_template('trainsensordatavisualization.html', messages=sorted_data_by_type)

@app.route('/real-all-data')
def get_real_data():
    """
    Render the data visualization page with the last 100 messages.

    Returns:
        str: The rendered template with message data.
    """
    return render_template('realdatavisualization.html', messages=real_msg_list[-100:])

def order_by(param_name, default_value):
    """
    Retrieve the value of a specific request argument or return a default.

    Args:
        param_name (str): The name of the request argument to retrieve.
        default_value (str): The default value to return if the argument is not found.

    Returns:
        str: The value of the argument or the default value.
    """
    return request.args.get(param_name, default_value)

def order_by_type():
    """
    Get the order parameter for sensor_type.

    Returns:
        str: The order parameter for sensor_type, defaults to 'sensor_type'.
    """
    return order_by('order_by_type', 'sensor_type')

def sort_data_by_type(data_list):
    """
    Sort the given sensor data by type.

    Args:
        data_list (list): The list of sensor data dictionaries to sort.

    Returns:
        list: The sorted list of sensor data by type.
    """
    return sorted(data_list, key=lambda x: x[order_by_type()])
