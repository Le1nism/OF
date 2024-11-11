import json
import os
import time
import random

import logging
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables for Kafka broker, Schema Registry, and topic name
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'train-sensor-data')

# Validate that KAFKA_BROKER and TOPIC_NAME are properly set
if not KAFKA_BROKER:
    raise ValueError("Environment variable KAFKA_BROKER is missing.")
if not TOPIC_NAME:
    raise ValueError("Environment variable TOPIC_NAME is missing.")


conf_prod = {
    'bootstrap.servers': KAFKA_BROKER,      # Kafka broker URL
    'key.serializer': StringSerializer('utf_8'),  # Serializer for message keys
    'value.serializer': lambda x, ctx: json.dumps(x).encode('utf-8')
}
producer = SerializingProducer(conf_prod)

# Functions to generate random sensor values for each type of sensor
def generate_speed():
    return round(random.uniform(0, 200), 2)

def generate_gps():
    return {
        "lat": round(random.uniform(45.0, 46.0), 6),
        "lon": round(random.uniform(9.0, 10.0), 6)
    }

def generate_engine_temp():
    return round(random.uniform(50, 120), 2)

def generate_brake_temp():
    return round(random.uniform(50, 500), 2)

def generate_vibration():
    return round(random.uniform(0.01, 2), 2)

def generate_brake_pressure():
    return round(random.uniform(0, 10), 2)

def generate_water_level():
    return round(random.uniform(0, 100), 2)


# Dictionary to map sensor types to their respective data generator functions
sensor_value_generators = {
    "speed": generate_speed,
    "gps": generate_gps,
    "engine_temp": generate_engine_temp,
    "brake_temp": generate_brake_temp,
    "vibration": generate_vibration,
    "brake_pressure": generate_brake_pressure,
    "water_level": generate_water_level
}


# Function to generate sensor data for a given sensor type
def generate_sensor_data(sensor_type):
    timestamp = int(time.time())  # Current Unix timestamp
    train_id = f"train_{10000 + random.randint(0, 1000)}"  # Random train ID
    sensor_id = f"{sensor_type}_sensor_{random.randint(0, 1000)}"  # Random sensor ID

    # Generate the sensor value using the corresponding function
    value = sensor_value_generators[sensor_type]()

    # Return a dictionary containing all relevant sensor data fields
    return {
        "timestamp": timestamp,
        "train_id": train_id,
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "value": value
    }


# Function to produce a message to Kafka for a specific sensor type
def produce_message(sensor_type):
    try:
        # Generate sensor data using the specified type
        sensor_data = generate_sensor_data(sensor_type)
        logging.info(f"Producing message >>> {sensor_data}")
        # Produce the message to Kafka on the specified topic
        producer.produce(topic=TOPIC_NAME, value=sensor_data)
        producer.flush()  # Ensure the message is immediately sent
        logging.info(f"Message sent >>> {sensor_data}")
    except Exception as e:
        print(f"Error while producing message: {e}")


# Wrapper function to send a message to Kafka
def send_to_kafka(sensor_type):
    logging.info("Starting message production >>> ")
    produce_message(sensor_type)
    print("Finished producing message!")

# Main loop to continuously send data for different sensor types
if __name__ == '__main__':
    sensor_types = ["speed", "gps", "engine_temp", "brake_temp", "vibration", "brake_pressure", "water_level"]
    while True:
        for sensor_type in sensor_types:
            send_to_kafka(sensor_type)  # Send data for each sensor type
        time.sleep(1)  # Wait 1 second before sending the next set of messages