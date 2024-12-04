import logging
import os
import threading
import json
import time
from omegaconf import DictConfig, OmegaConf 
import hydra
from flask import Flask,  render_template, request
from confluent_kafka import Consumer, KafkaError


DASHBOARD_NAME = "DASH"





# Initialize caches to store incoming messages
message_cache = {
    "simulate" : [], # Cache for simulated messages
    "real" : [], # Cache for real messages
    "anomalies" : [],  # Cache for anomaly messages
    "normal" : [] # Cache for normal messages
}

# Initialize a cache to store vehicle statistics
vehicle_stats_cache={}

# Maximum number of messages to store in the cache
MAX_MESSAGES = 100  # Limit for the number of stored messages


def deserialize_message(msg):
    """
    Deserialize a Kafka message from JSON format.

    Args:
        msg (Message): The Kafka message object.

    Returns:
        dict or None: A dictionary containing the deserialized data, or None if deserialization fails.
    """
    try:
        # Decode the message value from bytes to string and parse JSON
        message_value = json.loads(msg.value().decode('utf-8'))
        logger.info(f"Received message from topic {msg.topic()}")
        return message_value
    except json.JSONDecodeError as e:
        logger.error(f"Error deserializing message: {e}")
        return None


def kafka_consumer_thread(cfg):
    """
    Background thread to consume messages from Kafka topics.

    Args:
        topics (list): List of topics to subscribe to.
    """
    # Patterns to match different types of Kafka topics
    topics_dict = {
        "anomalies": "^.*_anomalies$",  # Topics containing anomalies
        "normal_data": '^.*_normal_data$', # Topics with normal data
        "statistics" : '^.*_statistics$' # Topics with statistics data
    }

    
    consumer = Consumer(
                        {'bootstrap.servers': cfg.dashboard.kafka_broker_url,  # Kafka broker URL
                        'group.id': cfg.dashboard.kafka_consumer_group_id,  # Consumer group for offset management
                        'auto.offset.reset': cfg.dashboard.kafka_auto_offset_reset  # Start reading messages from the beginning if no offset is present
                        }
                        )
    
    consumer.subscribe(list(topics_dict.values()))
    logger.debug(f"Started consuming messages from topics: {list(topics_dict.values())}")

    retry_delay = 1  # Initial delay in seconds

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for new messages with a timeout of 1 second
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached: {msg.error()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            # Deserialize the message and process it
            deserialized_data = deserialize_message(msg)
            if deserialized_data:
                logger.info(f"Processing message from topic {msg.topic()}")
                processing_message(msg.topic(), deserialized_data)
            else:
                logger.warning("Deserialized message is None")

            retry_delay = 1  # Reset retry delay on success
    except Exception as e:
        logger.error(f"Error while reading message: {e}")
        logger.info(f"Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 60)  # Exponential backoff, max 60 seconds
    finally:
        consumer.close()  # Close the Kafka consumer on exit


def processing_message(topic, msg):
    """
    Process a Kafka message based on its topic type.

    Args:
        topic (str): The topic from which the message was received.
        msg (dict): The deserialized message data.
    """
    try:
        if topic.endswith("_anomalies"):
            logger.debug(f"ANOMALIES ({topic})")
            add_to_cache("anomalies", msg)
            add_to_cache("real", msg)
        elif topic.endswith("_normal_data"):
            logger.debug(f"NORMAL DATA ({topic})")
            add_to_cache("normal", msg)
            add_to_cache("real", msg)
        elif len(msg.keys()) == 5:
            logger.debug(f"5K ({topic})")
            add_to_cache("simulate", msg)
        elif topic.endswith("_statistics"):
            logger.debug(f"STATISTICS ({topic})")
            process_stat_message(msg)
        else:
            logger.warning(f"Uncategorized message from topic {topic}: {msg}")
    except Exception as e:
        logger.error(f"Error processing message from topic {topic}: {e}")

def add_to_cache(cache_key, message):
    """
    Add a message to the appropriate cache, maintaining a maximum cache size.

    Args:
        cache_key (str): The cache key to store the message under.
        message (dict): The message to store in the cache.
    """
    message_cache[cache_key].append(message)
    # Limit the cache size to the last MAX_MESSAGES entries
    message_cache[cache_key] = message_cache[cache_key][-MAX_MESSAGES:]

def process_stat_message(msg):
    """
    Update vehicle statistics based on a received statistics message.

    Args:
        msg (dict): The statistics message data.
    """
    try:
        vehicle_name=msg.get("vehicle_name", "unknown_vehicle")
        logger.info(f"Processing statistics for vehicle: {vehicle_name}")

        # Initialize statistics for the vehicle if not already present
        if vehicle_name not in vehicle_stats_cache:
            vehicle_stats_cache[vehicle_name] = {'total_messages': 0, 'anomalies_messages': 0, 'normal_messages': 0}

        # Update statistics for the vehicle
        for key in vehicle_stats_cache[vehicle_name]:
            previous_value = vehicle_stats_cache[vehicle_name][key]
            increment = msg.get(key, 0)
            vehicle_stats_cache[vehicle_name][key] += increment
            logger.info(f"Updated {key} for {vehicle_name}: {previous_value} -> {vehicle_stats_cache[vehicle_name][key]}")
    except Exception as e:
        logger.error(f"Error while processing statistics: {e}")


def start_consuming(cfg):
    """
    Start consuming Kafka messages in a separate thread.
    """
    thread = threading.Thread(
        target=kafka_consumer_thread, 
        args=[cfg])
    thread.daemon = True
    thread.start()


@hydra.main(config_path="../config", config_name="default", version_base="1.2")
def create_app(cfg: DictConfig) -> None:
    global logger

    if cfg.override != "":
        try:
            # Load the variant specified from the command line
            config_overrides = OmegaConf.load(hydra.utils.get_original_cwd() + f'/config/overrides/{cfg.override}.yaml')
            # Merge configurations, with the variant overriding the base config
            cfg = OmegaConf.merge(cfg, config_overrides)
        except:
            print('Unsuccesfully tried to use the configuration override: ',cfg.override)


    # Create a Flask app instance
    app = Flask(__name__)

    # Configure logging for detailed output
    logger = logging.getLogger(DASHBOARD_NAME)
    # Set the log level
    logger.setLevel(cfg.logging_level.upper())

    # Start consuming Kafka messages
    start_consuming(cfg)

    @app.route('/', methods=['GET'])
    def home():
        """
        Render the home page.

        Returns:
            str: The HTML for the home page.
        """
        return render_template('index.html')


    @app.route('/real-all-data')
    def get_all_real_data():
        """
        Render the page displaying the last 100 real messages.

        Returns:
            str: The HTML for the real data visualization page.
        """
        return render_template('realdatavisualization.html', messages=message_cache["real"])


    @app.route('/real-anomalies-data')
    def get_anomalies_real_data():
        """
        Render the page displaying the last 100 anomaly messages.

        Returns:
            str: The HTML for the anomaly data visualization page.
        """
        return render_template('realdatavisualization.html', messages=message_cache["anomalies"])


    @app.route('/real-normal-data')
    def get_normal_real_data():
        """
        Render the page displaying the last 100 normal messages.

        Returns:
            str: The HTML for the normal data visualization page.
        """
        return render_template('realdatavisualization.html', messages=message_cache["normal"])


    @app.route('/statistics')
    def get_statistics():
        """
        Render the statistics page with vehicle statistics.

        Returns:
            str: The HTML for the statistics page.
        """
        sorted_stats = {k: vehicle_stats_cache[k] for k in sorted(vehicle_stats_cache)}
        return render_template('statistics.html', all_stats=sorted_stats)

    app.run(host=cfg.dashboard.host, port=cfg.dashboard.port)



if __name__ == "__main__":
    create_app()
    