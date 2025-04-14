import requests
import logging
from omegaconf import DictConfig

class ConsumerManager:

    def __init__(self, cfg):

        self.logger = logging.getLogger("CONSUMER_MANAGER")
        self.cfg = cfg
        self.logging_level = cfg.logging_level.upper()
        self.default_consumer_config = dict(cfg.default_consumer_config)
        self.default_consumer_config["kafka_topic_update_interval_secs"] = cfg.kafka_topic_update_interval_secs
        self.consumer_configs = {}
        self.override = cfg.override
        self.train_servers = {} # Map of the train name to server URL

        # Initialize train configurations
        for vehicle in cfg.vehicles:

            if type(vehicle) == str:
                vehicle_name = vehicle

            else:
                vehicle_name = list(vehicle.keys())[0]

            self.consumer_configs[vehicle_name] = self.default_consumer_config.copy()

            if type(vehicle) == DictConfig:
                self.consumer_configs[vehicle_name].update(vehicle[vehicle_name])

            if self.consumer_configs[vehicle_name]["anomaly_classes"] == "all":
                self.consumer_configs[vehicle_name]["anomaly_classes"] = list(range(1, 19))

            if self.consumer_configs[vehicle_name]["diagnostics_classes"] == "all":
                self.consumer_configs[vehicle_name]["diagnostics_classes"] = list(range(1, 19))

            # Register train server URL
            self.train_servers[vehicle_name] = f"http://{vehicle_name}_server:8000"

    def start_all_consumers(self):

        # Start all consumers via HTTP
        for train_name, server_url in self.train_servers.items():
            self.start_consumer(train_name, server_url)

        return "All consumers started!"

    def start_consumer(self, train_name, server_url):

        try:

            # Prepare configuration for this train
            config = self.consumer_configs[train_name].copy()
            config["vehicle_name"] = train_name

            # Send HTTP request to start consumer
            response = requests.post(
                f"{server_url}/start",
                json = config,
                timeout = 10
            )

            if response.status_code == 200:
                self.logger.info(f"Started consumer for {train_name})

            else:
                self.logger.error(f"Failed to start consumer for {train_name}: {response.text}")

        except Exception as e:

            self.logger.error(f"Error starting consumer for {train_name}: {e}")

    def stop_consumer(self, train_name):

        try:

            server_url = self.train_servers[train_name]
            response = requests.post(f"{server_url}/stop", timeout = 10)

            if response.status_code == 200:
                self.logger.info(f"Stopped consumer for {train_name}")

            else:
                self.logger.error(f"Failed to stop consumer for {train_name}: {response.text}")

        except Exception as e:

            self.logger.error(f"Error stopping consumer for {train_name}: {e}")
            
    def stop_all_consumers(self):

        for train_name in self.train_servers:
            self.stop_consumer(train_name)
