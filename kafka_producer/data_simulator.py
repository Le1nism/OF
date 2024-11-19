import json
import os
import time
import random
import logging
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

# Configure logging for detailed information
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables for Kafka broker and topic name
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')  # Default Kafka broker address
TOPIC_NAME = os.getenv('TOPIC_NAME', 'train-sensor-data')  # Default Kafka topic

# Validate that KAFKA_BROKER and TOPIC_NAME are set
if not KAFKA_BROKER:
    raise ValueError("Environment variable KAFKA_BROKER is missing.")
if not TOPIC_NAME:
    raise ValueError("Environment variable TOPIC_NAME is missing.")

# Kafka producer configuration
conf_prod = {
    'bootstrap.servers': KAFKA_BROKER,  # Kafka broker URL
    'key.serializer': StringSerializer('utf_8'),  # Serializer for message keys
    'value.serializer': lambda x, ctx: json.dumps(x).encode('utf-8')  # Serialize message values to JSON
}
producer = SerializingProducer(conf_prod)

def generate_timestamp():
    """
    Generate the current Unix timestamp.

    Returns:
        int: Current Unix timestamp.
    """
    return int(time.time())  # Current Unix timestamp

def generate_train_id():
    """
    Generate a random train ID.

    Returns:
        str: Randomly generated train ID (e.g., "train_10032").
    """
    return f"train_{10000 + random.randint(0, 1000)}"  # Random train ID within a specific range

def generate_sensor_id(sensor_type):
    """
    Generate a random sensor ID based on the sensor type.

    Args:
        sensor_type (str): The type of sensor.

    Returns:
        str: Randomly generated sensor ID (e.g., "speed_sensor_23").
    """
    return f"{sensor_type}_sensor_{random.randint(0, 1000)}"  # Random sensor ID

def generate_speed():
    """
    Generate a random speed value between 0 and 200 km/h.

    Returns:
        float: Random speed value.
    """
    return round(random.uniform(0, 200), 2)  # Speed in km/h with two decimal precision

def generate_gps():
    """
    Generate a random GPS coordinate within a specific range.

    Returns:
        dict: Dictionary with random latitude and longitude.
    """
    return {
        "lat": round(random.uniform(45.0, 46.0), 6),  # Latitude in a specific range
        "lon": round(random.uniform(9.0, 10.0), 6)  # Longitude in a specific range
    }

def generate_engine_temp():
    """
    Generate a random engine temperature between 50 and 120 degrees Celsius.

    Returns:
        float: Random engine temperature.
    """
    return round(random.uniform(50, 120), 2)  # Temperature in degrees Celsius

def generate_brake_temp():
    """
    Generate a random brake temperature between 50 and 500 degrees Celsius.

    Returns:
        float: Random brake temperature.
    """
    return round(random.uniform(50, 500), 2)  # Brake temperature in degrees Celsius

def generate_vibration():
    """
    Generate a random vibration level between 0.01 and 2 Gs.

    Returns:
        float: Random vibration level.
    """
    return round(random.uniform(0.01, 2), 2)  # Vibration in Gs

def generate_brake_pressure():
    """
    Generate a random brake pressure between 0 and 10 bar.

    Returns:
        float: Random brake pressure.
    """
    return round(random.uniform(0, 10), 2)  # Pressure in bar

def generate_water_level():
    """
    Generate a random water level percentage between 0% and 100%.

    Returns:
        float: Random water level percentage.
    """
    return round(random.uniform(0, 100), 2)  # Water level percentage

# Dictionary mapping sensor types to their data generator functions
sensor_value_generators = {
    "speed": generate_speed,
    "gps": generate_gps,
    "engine_temp": generate_engine_temp,
    "brake_temp": generate_brake_temp,
    "vibration": generate_vibration,
    "brake_pressure": generate_brake_pressure,
    "water_level": generate_water_level
}

def generate_sensor_data(sensor_type):
    """
    Generate sensor data for a given sensor type.

    Args:
        sensor_type (str): The type of sensor to generate data for.

    Returns:
        dict: A dictionary containing sensor data with fields for timestamp, train ID,
              sensor ID, sensor type, and the generated value.
    """
    timestamp = generate_timestamp()  # Current Unix timestamp
    train_id = generate_train_id()  # Randomly generated train ID
    sensor_id = generate_sensor_id(sensor_type)  # Random sensor ID based on sensor type
    value = sensor_value_generators[sensor_type]()  # Generate the sensor value using the appropriate function
    return {
        "timestamp": timestamp,
        "train_id": train_id,
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "value": value
    }

def produce_message(sensor_type):
    """
    Produce a message to Kafka for a specific sensor type.

    Args:
        sensor_type (str): The type of sensor to produce a message for.
    """
    try:
        sensor_data = generate_sensor_data(sensor_type)  # Generate sensor data
        logging.info(f"Producing message >>> {sensor_data}")
        producer.produce(topic=TOPIC_NAME, value=sensor_data)  # Send the message to Kafka
        producer.flush()  # Ensure the message is sent immediately
        logging.info(f"Message sent >>> {sensor_data}")
    except Exception as e:
        print(f"Error while producing message: {e}")  # Print error if message production fails

def send_to_kafka(sensor_type):
    """
    Wrapper function to send a sensor type message to Kafka.

    Args:
        sensor_type (str): The type of sensor to send data for.
    """
    logging.info("Starting message production >>> ")
    produce_message(sensor_type)  # Produce a message for the specified sensor type
    print("Finished producing message!")  # Log completion

if __name__ == '__main__':
    # List of sensor types to produce data for
    sensor_types = ["speed", "gps", "engine_temp", "brake_temp", "vibration", "brake_pressure", "water_level"]
    while True:
        for sensor_type in sensor_types:
            send_to_kafka(sensor_type)  # Send data for each sensor type
        time.sleep(1)  # Wait 1 second before sending the next batch of data
