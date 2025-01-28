import threading
import logging

class ConsumerManager:

    def __init__(self, cfg, consumers, CONSUMER_COMMAND="python consume.py"):
        self.consumers = consumers
        self.threads = {}
        self.consumer_command = CONSUMER_COMMAND
        self.logger = logging.getLogger("CONSUMER_MANAGER")
        self.logging_level = cfg.logging_level.upper()
        self.logger.setLevel(self.logging_level)
        self.default_consumer_config = dict(cfg.default_consumer_config)
        self.default_consumer_config["kafka_topic_update_interval_secs"] = cfg.kafka_topic_update_interval_secs
        self.vehicle_names = []
        self.consumer_configs = {}
        for vehicle in cfg.vehicles:
            vehicle_name = list(vehicle.keys())[0]
            self.vehicle_names.append(vehicle_name)
            self.consumer_configs[vehicle_name] = self.default_consumer_config.copy()
            self.consumer_configs[vehicle_name].update(vehicle[vehicle_name])
            if self.consumer_configs[vehicle_name]["anomaly_classes"] == "all":
                self.consumer_configs[vehicle_name]["anomaly_classes"] = list(range(1, 15))
            if self.consumer_configs[vehicle_name]["diagnostics_classes"] == "all":
                self.consumer_configs[vehicle_name]["diagnostics_classes"] = list(range(1, 15))


    def start_all_consumers(self):
        # Start all consumers
        for consumer_name, vehicle_name in zip(self.consumers.keys(), self.vehicle_names):
            self.start_consumer(
                consumer_name, 
                self.consumers[consumer_name], 
                vehicle_name)
        return "All consumers started!"
    

    def start_consumer(self, consumer_name, consumer_container, vehicle_name):
        def run_consumer():

            consumer_config = self.consumer_configs[vehicle_name]

            command_to_exec = self.consumer_command + " --vehicle_name=" + vehicle_name + \
                " --container_name=" + vehicle_name + \
                " --kafka_broker=" + consumer_config["kafka_broker"] + \
                " --buffer_size=" + str(consumer_config["buffer_size"]) + \
                " --batch_size=" + str(consumer_config["batch_size"]) + \
                " --logging_level=" + str(self.logging_level) + \
                " --weights_push_freq_seconds=" + str(consumer_config["weights_push_freq_seconds"]) + \
                " --kafka_topic_update_interval_secs=" + str(consumer_config["kafka_topic_update_interval_secs"])

            return_tuple = consumer_container.exec_run(
                command_to_exec,
                stream=True, 
                tty=True, 
                stdin=True
            )
            for line in return_tuple[1]:
                print(line.decode().strip())

        thread = threading.Thread(target=run_consumer, name=consumer_name)
        thread.start()
        self.threads[consumer_name] = thread
        self.logger.debug(f"Started consumer from {consumer_name}")


    def stop_consumer(self, consumer_name):
        container = self.consumers[consumer_name]
        try:
            # Try to find and kill the process
            pid_result = container.exec_run(f"pgrep -f '{self.consumer_command}'")
            pid = pid_result[1].decode().strip()
            
            if pid:
                container.exec_run(f"kill -SIGINT {pid}")
                print(f"Stopped consumer from {consumer_name}")
            else:
                print(f"No running process found for {consumer_name}")
        except Exception as e:
            print(f"Error stopping {consumer_name}: {e}")


    def stop_all_consumers(self):
        for consumer_name in self.consumers:
            self.stop_consumer(consumer_name)