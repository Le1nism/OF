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
from consumer import MessageConsumer

DASHBOARD_NAME = "DASH"


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
    # associate processing message rountine:
    app.process_message_routine = processing_message

    # Configure logging for detailed output
    logger = logging.getLogger(DASHBOARD_NAME)
    # Set the log level
    logger.setLevel(cfg.logging_level.upper())

    # Start consuming Kafka messages
    # start_consuming(cfg)

    message_consumer = MessageConsumer(parent=app, cfg=cfg)
    message_consumer.readining_thread.start()

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
    