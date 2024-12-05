import threading
import logging

class ProducerManager:

    def __init__(self, cfg, producers, PRODUCER_COMMAND="python produce.py"):
        self.logger = logging.getLogger("PRODUCER_MANAGER")
        self.logging_level = cfg.logging_level.upper()
        self.logger.setLevel(self.logging_level)
        self.producers = producers
        self.threads = {}
        self.producer_command = PRODUCER_COMMAND
        self.default_vehicle_config = cfg.default_vehicle_config
        self.vehicle_names = cfg.vehicles
        self.vehicle_configs = {}
        for vehicle_name in self.vehicle_names:
            self.vehicle_configs[vehicle_name] = self.default_vehicle_config


    def start_all_producers(self):
        # Start all producers
        for producer_name, vehicle_name in zip(self.producers.keys(), self.vehicle_names):
            self.start_producer(
                producer_name,
                self.producers[producer_name],
                vehicle_name,
                self.vehicle_configs[vehicle_name])
        return "All producers started!"
    

    def start_producer(self, producer_name, producer_container, vehicle_name, vehicle_config):
        def run_producer():

            command_to_exec = self.producer_command + " --vehicle_name=" + vehicle_name + \
                    " --container_name=" + vehicle_name + \
                    " --kafka_broker=" + vehicle_config["kafka_broker"] + \
                    " --mu_anomalies=" + str(vehicle_config["mu_anomalies"]) + \
                    " --mu_normal=" + str(vehicle_config["mu_normal"]) + \
                    " --alpha=" + str(vehicle_config["alpha"]) + \
                    " --beta=" + str(vehicle_config["beta"]) + \
                    " --logging_level=" + str(self.logging_level)
            
            return_tuple = producer_container.exec_run(
                command_to_exec,
                stream=True, 
                tty=True, 
                stdin=True
            )
            for line in return_tuple[1]:
                print(f"{producer_name}: {line.decode().strip()}")

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
                print(f"Stopped producer from {producer_name}")
            else:
                print(f"No running process found for {producer_name}")
        except Exception as e:
            print(f"Error stopping {producer_name}: {e}")


    def stop_all_producers(self):
        for producer_name in self.producers:
            self.stop_producer(producer_name)