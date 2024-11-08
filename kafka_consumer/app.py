import os
import threading
import json

from flask import Flask, jsonify, render_template
from confluent_kafka import Consumer, KafkaException, KafkaError


app = Flask(__name__)

# Read Kafka configuration from environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'train-sensor-data')

# Print environment variables to verify they are set correctly
print(f"KAFKA_BROKER: {KAFKA_BROKER}")
#print(f"SCHEMA_REGISTRY_URL: {SCHEMA_REGISTRY_URL}")
print(f"TOPIC_NAME: {TOPIC_NAME}")

# Validate that KAFKA_BROKER and TOPIC_NAME are properly set
if not KAFKA_BROKER:
    raise ValueError("Environment variable KAFKA_BROKER is missing.")
if not TOPIC_NAME:
    raise ValueError("Environment variable TOPIC_NAME is missing.")

# Initialize the Schema Registry client
#schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
#schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Initialize the AvroDeserializer to decode Avro messages
#avro_deserializer = AvroDeserializer(schema_registry_client)

msg_list = []
MAX_MESSAGES = 100

# Configure the Kafka consumer
conf_cons = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'kafka-consumer-group-1',      # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start reading at the earliest message
}


def kafka_consumer_thread():
    consumer = Consumer(conf_cons)
    consumer.subscribe([TOPIC_NAME])
    try:
        while True:
            try:
                msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second
                if msg is None:
                    continue
                if msg.error:
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Fine della partizione raggiunta: {msg.error()}")
                    else:
                        print(f"Errore del Consumer: {msg.error()}")
                    continue

                # Deserializza il valore JSON del messaggio
                message_value = json.loads(msg.value().decode('utf-8'))
                msg_list.append(message_value)
                print(f"Received message: {msg.value}")
            except Exception as e:
                print(f"Error deserializing message: {e}")
            # Mantieni solo gli ultimi MAX_MESSAGES messaggi
            if len(msg_list) > MAX_MESSAGES:
               msg_list.pop(0)
    except Exception as e:
        print(f"Errore durante la lettura del messaggio: {e}")
    finally:
        consumer.close()

            #if msg.error():
                # Handle any errors in message consumption
             #   if msg.error().code() != KafkaError._PARTITION_EOF:
              #      print(f"Reached the end of partition: {msg.error()}")
              #      break
               # else:
                #    print(f"Error consuming message: {msg.error()}")
                 #   break  # Break the loop on other errors
            #else:
                # Deserialize the message value using the Avro deserializer
             #   try:
                    #schema_id=msg.headers().get("sensor_data_schema.avsc") if msg.headers() else None
              #      msg_value = json.loads(msg.decode('utf-8'))  # 'None' for key if not using keys
               #     messages.append(msg_value)  # Append the deserialized message to the list

                    # Maintain a maximum of 100 messages in the list
# KafkaConsumer(
#   TOPIC_NAME,
#    bootstrap_servers=KAFKA_BROKER,
#    group_id='kafka-consumer-group-1',
#   auto_offset_reset='earliest',
#   enable_auto_commit=True,
#
#)
#consumer.subscribe([TOPIC_NAME])  # Subscribe to the specified topic


# Start the Kafka consumer thread

threading.Thread(target=kafka_consumer_thread, daemon=True).start()

# Flask route to retrieve the last 100 messages
@app.route('/', methods=['GET'])
def get_messages():
    return jsonify(msg_list[-100:])  # Return the last 100 messages as JSON

# Start the Flask web application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
