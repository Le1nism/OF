from confluent_kafka import Consumer, KafkaError
import logging
import time
import json
import threading

# Patterns to match different types of Kafka topics
topics_dict = {
    "anomalies": "^.*_anomalies$",  # Topics containing anomalies
    "normal_data": '^.*_normal_data$', # Topics with normal data
    "statistics" : '^.*_statistics$' # Topics with statistics data
}


class MessageConsumer:
    def __init__(self, parent, cfg):

        self.parent = parent
        self.logger = logging.getLogger("MESSAGE_CONSUMER")
        self.logger.setLevel(cfg.logging_level.upper())
        self.retry_delay = 1

        configs = {'bootstrap.servers': cfg.dashboard.kafka_broker_url,  # Kafka broker URL
                        'group.id': cfg.dashboard.kafka_consumer_group_id,  # Consumer group for offset management
                        'auto.offset.reset': cfg.dashboard.kafka_auto_offset_reset  # Start reading messages from the beginning if no offset is present
                        }
        self.consumer = Consumer(configs)
        self.resubscribe()
        self.readining_thread = threading.Thread(target=self.read_messages)
        self.readining_thread.daemon = True
        

    def resubscribe(self):
        self.consumer.subscribe(list(topics_dict.values()))
        self.logger.debug(f"Started consuming messages from topics: {list(topics_dict.values())}")


    def deserialize_message(self, msg):
        try:
            # Decode the message value from bytes to string and parse JSON
            message_value = json.loads(msg.value().decode('utf-8'))
            self.logger.info(f"Received message from topic {msg.topic()}")
            return message_value
        except json.JSONDecodeError as e:
            self.logger.error(f"Error deserializing message: {e}")
            return None
    

    def read_messages(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Poll for new messages with a timeout of 1 second
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.info(f"End of partition reached: {msg.error()}")
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Deserialize the message and process it
                deserialized_data = self.deserialize_message(msg)
                if deserialized_data:
                    self.logger.info(f"Processing message from topic {msg.topic()}")
                    self.parent.process_message_routine(msg.topic(), deserialized_data)
                else:
                    self.logger.warning("Deserialized message is None")

                retry_delay = 1  # Reset retry delay on success
        except Exception as e:
            self.logger.error(f"Error while reading message: {e}")
            self.logger.info(f"Retrying in {self.retry_delay} seconds...")
            time.sleep(self.retry_delay)
            retry_delay = min(self.retry_delay * 2, 60)  # Exponential backoff, max 60 seconds
        finally:
            self.consumer.close()  # Close the Kafka consumer on exit