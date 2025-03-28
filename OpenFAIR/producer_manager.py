import threading
import logging
from omegaconf import DictConfig

class ProducerManager:

    def __init__(self, cfg, producers, PRODUCER_COMMAND="python produce.py"):
        self.logger = logging.getLogger("PRODUCER_MANAGER")
        self.logging_level = cfg.logging_level.upper()
        self.logger.setLevel(self.logging_level)
        self.producers = producers
        self.threads = {}
        self.producer_command = PRODUCER_COMMAND
        self.default_vehicle_config = cfg.default_vehicle_config
        self.vehicle_names = []
        self.vehicle_configs = {}
        self.probe_metrics = cfg.security_manager.probe_metrics
        for vehicle in cfg.vehicles:
            if type(vehicle) == str:
                vehicle_name = vehicle
            else:
                vehicle_name = list(vehicle.keys())[0]
            self.vehicle_names.append(vehicle_name)    
            self.vehicle_configs[vehicle_name] = self.default_vehicle_config.copy()
            if type(vehicle) == DictConfig:
                self.vehicle_configs[vehicle_name].update(vehicle[vehicle_name])
            if self.vehicle_configs[vehicle_name]["anomaly_classes"] == "all":
                self.vehicle_configs[vehicle_name]["anomaly_classes"] = list(range(1, 15))
            if self.vehicle_configs[vehicle_name]["diagnostics_classes"] == "all":
                self.vehicle_configs[vehicle_name]["diagnostics_classes"] = list(range(1, 15))


    def start_all_producers(self):
        # Start all producers
        for producer_name, vehicle_name in zip(self.producers.keys(), self.vehicle_names):
            self.start_producer(
                producer_name,
                self.producers[producer_name],
                self.vehicle_configs[vehicle_name])
        return "All producers started!"
    

    def start_producer(self, producer_name, producer_container, vehicle_config):
        def run_producer():

            command_to_exec = self.producer_command + \
                    " --kafka_broker=" + vehicle_config["kafka_broker"] + \
                    " --mu_anomalies=" + str(vehicle_config["mu_anomalies"]) + \
                    " --mu_normal=" + str(vehicle_config["mu_normal"]) + \
                    " --alpha=" + str(vehicle_config["alpha"]) + \
                    " --beta=" + str(vehicle_config["beta"]) + \
                    " --logging_level=" + str(self.logging_level) + \
                    " --anomaly_classes=" + ",".join(map(str,vehicle_config["anomaly_classes"])) + \
                    " --diagnostics_classes=" + ",".join(map(str,vehicle_config["diagnostics_classes"])) + \
                    " --ping_thread_timeout=" + str(vehicle_config["ping_thread_timeout"]) + \
                    " --ping_host=" + str(vehicle_config["ping_host"]) + \
                    " --probe_frequency_seconds=" + str(vehicle_config["probe_frequency_seconds"]) +\
                    " --probe_metrics=" + ",".join(map(str,self.probe_metrics))
            
            if vehicle_config["time_emulation"]:
                command_to_exec += " --time_emulation" 
            
            return_tuple = producer_container.exec_run(
                command_to_exec,
                stream=True, 
                tty=True, 
                stdin=True
            )
            for line in return_tuple[1]:
                self.logger.info(line.decode().strip())

        thread = threading.Thread(target=run_producer, name=producer_name)
        thread.start()
        self.threads[producer_name] = thread
        self.logger.debug(f"Started producer from {producer_name}")


    def stop_producer(self, producer_name):
        container = self.producers[producer_name]
        try:
            # Try to find and kill the process
            pid_result = container.exec_run(f"pgrep -f '{self.producer_command}'")
            pid = pid_result[1].decode().strip()
            
            if pid:
                container.exec_run(f"kill -SIGINT {pid}")
                self.logger.info(f"Sent SIGINT to {producer_name}")
            else:
                self.logger.info(f"No running process found for {producer_name}")
        except Exception as e:
            self.logger.info(f"Error stopping {producer_name}: {e}")


    def stop_all_producers(self):
        for producer_name in self.producers:
            self.stop_producer(producer_name)