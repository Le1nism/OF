import logging
import threading
import json
import time
from omegaconf import DictConfig, OmegaConf 
import hydra
from flask import Flask,  render_template
from confluent_kafka import Consumer, KafkaError
from cache import MessageCache
from metrics_logger import MetricsLogger

DASHBOARD_NAME = "DASH"


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
            msg_cache.add("anomalies", msg)
            msg_cache.add("all", msg)
        elif topic.endswith("_normal_data"):
            logger.debug(f"DIAGNOSTICS ({topic})")
            msg_cache.add("diagnostics", msg)
            msg_cache.add("all", msg)
        elif topic.endswith("_statistics"):
            logger.debug(f"STATISTICS ({topic})")
            metrics_logger.process_stat_message(msg)
        else:
            logger.warning(f"Uncategorized message from topic {topic}: {msg}")
    except Exception as e:
        logger.error(f"Error processing message from topic {topic}: {e}")


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
    global logger, message_cache_len, msg_cache, metrics_logger

    if cfg.override != "":
        try:
            # Load the variant specified from the command line
            config_overrides = OmegaConf.load(hydra.utils.get_original_cwd() + f'/config/overrides/{cfg.override}.yaml')
            # Merge configurations, with the variant overriding the base config
            cfg = OmegaConf.merge(cfg, config_overrides)
        except:
            print('Unsuccesfully tried to use the configuration override: ',cfg.override)

    msg_cache = MessageCache(cfg.dashboard.message_cache_len)
    metrics_logger = MetricsLogger(cfg)

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
        return render_template('realdatavisualization.html', messages=msg_cache["all"])


    @app.route('/real-anomalies-data')
    def get_anomalies_real_data():
        """
        Render the page displaying the last 100 anomaly messages.

        Returns:
            str: The HTML for the anomaly data visualization page.
        """
        return render_template('realdatavisualization.html', messages=msg_cache["anomalies"])


    @app.route('/real-normal-data')
    def get_normal_real_data():
        """
        Render the page displaying the last 100 normal messages.

        Returns:
            str: The HTML for the normal data visualization page.
        """
        return render_template('realdatavisualization.html', messages=msg_cache["diagnostics"])


    @app.route('/statistics')
    def get_statistics():
        """
        Render the statistics page with vehicle statistics.

        Returns:
            str: The HTML for the statistics page.
        """
        sorted_stats = {k: metrics_logger.metrics[k] for k in sorted(metrics_logger.metrics)}
        return render_template('statistics.html', all_stats=sorted_stats)

    app.run(host=cfg.dashboard.host, port=cfg.dashboard.port)



if __name__ == "__main__":
    create_app()
    