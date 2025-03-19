from omegaconf import DictConfig, OmegaConf 
import hydra
from flask import Flask,  render_template, request
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from OpenFAIR import MessageCache, MetricsLogger, KafkaMessageConsumer, ContainerManager


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
            # app.logger.debug(f"ANOMALIES ({topic})")
            msg_cache.add("anomalies", msg)
            msg_cache.add("all", msg)
        elif topic.endswith("_normal_data"):
            # app.logger.debug(f"DIAGNOSTICS ({topic})")
            msg_cache.add("diagnostics", msg)
            msg_cache.add("all", msg)
        elif topic.endswith("_statistics"):
            # app.logger.debug(f"STATISTICS ({topic})")
            metrics_logger.process_stat_message(msg)
        else:
            app.logger.warning(f"Uncategorized message from topic {topic}: {msg}")
    except Exception as e:
        app.logger.error(f"Error processing message from topic {topic}: {e}")


@hydra.main(config_path="../config", config_name="default", version_base="1.2")
def create_app(cfg: DictConfig) -> None:
    global app, msg_cache, metrics_logger

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
    # associate processing message routine and logger:
    app = Flask(__name__)
    app.process_message_routine = processing_message
    app.logger.name = DASHBOARD_NAME
    app.logger.setLevel(cfg.logging_level.upper())


    # Create a ConainerManager instance
    container_manager = ContainerManager(cfg)

    # Create a MessageConsumer instance
    message_consumer = KafkaMessageConsumer(parent=app, cfg=cfg)
    message_consumer.start()


    def prepare_botmaster_buttons(cfg):
        vehicle_names = []
        for vehicle in cfg.vehicles:
            if type(vehicle) == str:
                vehicle_names.append(vehicle)
            else: 
                vehicle_names.append(list(vehicle.keys())[0])
        return vehicle_names
    

    @app.route('/', methods=['GET'])
    def home():
        """
        Render the home page.

        Returns:
            str: The HTML for the home page.
        """
        rendering_params = {
            "botmaster_buttons" : prepare_botmaster_buttons(cfg)
        }

        return render_template('index.html', rendering_params=rendering_params)

    @app.route("/start-attack", methods=["POST"], )
    def start_attack():
        data = request.get_json()
        pressed_button_id = data['pressed_button_id']
        attacking_vehicle = pressed_button_id.split("_")[0]
        return container_manager.start_attack_from_vehicle(cfg, attacking_vehicle)
    
    @app.route("/stop-attack", methods=["POST"])
    def stop_attack():
        data = request.get_json()
        pressed_button_id = data['pressed_button_id']
        attacking_vehicle = pressed_button_id.split("_")[0]
        return container_manager.stop_attack_from_vehicle(attacking_vehicle)
    
    @app.route("/start-preconf-attack", methods=["POST"])
    def start_preconf_attack():
        return container_manager.start_preconf_attack(cfg)
    
    @app.route("/stop-preconf-attack", methods=["POST"])
    def stop_preconf_attack():
        return container_manager.stop_preconf_attack(cfg)

    @app.route("/produce-all", methods=["POST"])
    def produce_all():
        return container_manager.produce_all()
    

    @app.route("/stop-producing-all", methods=["POST"])
    def stop_produce_all():
        return container_manager.stop_producing_all()
    

    @app.route("/consume-all", methods=["POST"])
    def consume_all():
        return container_manager.consume_all()
    

    @app.route("/stop-consuming-all", methods=["POST"])
    def stop_consuming_all():
        return container_manager.stop_consuming_all()


    @app.route('/start-federated-learning', methods=['POST'])
    def start_federated_learning():
        return container_manager.start_federated_learning(cfg)
    

    @app.route('/stop-federated-learning', methods=['POST'])
    def stop_federated_learning():
        return container_manager.stop_federated_learning()


    @app.route('/start-wandb', methods=['POST'])
    def start_wandb():
        return container_manager.start_wandb(cfg)

    @app.route('/start-security-manager', methods=['POST'])
    def start_security_manager():
        return container_manager.start_security_manager(cfg)
    
    @app.route('/stop-security-manager', methods=['POST'])
    def stop_security_manager():
        return container_manager.stop_security_manager()

    @app.route('/create-vehicles', methods=['POST'])
    def create_vehicles():
        return container_manager.create_vehicles()
    

    @app.route('/delete-vehicles', methods=['POST'])
    def delete_vehicles():
        return container_manager.delete_vehicles()
    

    @app.route('/stop-wandb', methods=['POST'])
    def stop_wandb():
        return container_manager.stop_wandb()


    @app.route('/real-all-data')
    def get_all_real_data():
        """
        Render the page displaying the last 100 real messages.

        Returns:
            str: The HTML for the real data visualization page.
        """
        return render_template('realdatavisualization.html', messages=msg_cache.cache["all"])


    @app.route('/real-anomalies-data')
    def get_anomalies_real_data():
        """
        Render the page displaying the last 100 anomaly messages.

        Returns:
            str: The HTML for the anomaly data visualization page.
        """
        return render_template('realdatavisualization.html', messages=msg_cache.cache["anomalies"])


    @app.route('/real-normal-data')
    def get_normal_real_data():
        """
        Render the page displaying the last 100 normal messages.

        Returns:
            str: The HTML for the normal data visualization page.
        """
        return render_template('realdatavisualization.html', messages=msg_cache.cache["diagnostics"])


    @app.route('/statistics')
    def get_statistics():
        """
        Render the statistics page with vehicle statistics.

        Returns:
            str: The HTML for the statistics page.
        """
        sorted_stats = {k: metrics_logger.metrics[k] for k in sorted(metrics_logger.metrics)}
        return render_template('statistics.html', all_stats=sorted_stats)


    # Run the Flask app
    app.run(host=cfg.dashboard.host, port=cfg.dashboard.port)


if __name__ == "__main__":
    create_app()
    