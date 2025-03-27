import docker
import logging
from OpenFAIR.producer_manager import ProducerManager
from OpenFAIR.consumer_manager import ConsumerManager
from OpenFAIR.attack_agent import AttackAgent
from OpenFAIR.dash_monitor import DashBoardMonitor
import threading
import subprocess
import time
import signal
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import json
import requests
import os


WANDBER_COMMAND = "python wandber.py"
FL_COMMAND = "python federated_learning.py"
SM_COMMAND = "python security_manager.py"
ATTACK_COMMAND = "python attack.py"
HEALTHY = "HEALTHY"
INFECTED = "INFECTED"


class ContainerManager:
    
    def __init__(self, cfg):
        self.logger = logging.getLogger("CONTAINER_MANAGER")
        self.logger.setLevel(cfg.logging_level.upper())
        # Connect to the Docker daemon
        self.client = docker.from_env()
        self.wandber = {
            'container': None,
            'thread': None}
        self.federated_learner = {
            'container': None,
            'thread': None
        }
        self.security_manager = {
            'container': None,
            'thread': None
        }
        self.producers = {}
        self.consumers = {}
        self.containers_ips = {}
        self.cfg = cfg
        self.vehicle_names = []
        for vehicle in cfg.vehicles:
            if type(vehicle) == str:
                vehicle_name = vehicle
            else:
                vehicle_name = list(vehicle.keys())[0]
            self.vehicle_names.append(vehicle_name) 

        self.vehicle_status_dict = self.init_vehicle_status_dict()
        self.last_attack_started_at = {vehicle_name: time.time() for vehicle_name in self.vehicle_names}
        self.refresh_containers()
        self.host_ip = self.get_my_ip()
        self.logger.info(f"Host IP: {self.host_ip}")
        self.attack_agent = AttackAgent(self, cfg)
        self.start_dashboard_monitor()
        if cfg.dashboard.proxy:
            self.proxy_configuration()


    def proxy_configuration(self):
        # get the value of the no_proxy env var:
        no_proxy = os.environ.get('no_proxy')
        for node_ip in self.containers_ips.values():
            if node_ip not in no_proxy:
                no_proxy += f",{node_ip}"
        os.environ['no_proxy'] = no_proxy


    def start_dashboard_monitor(self):
        # Start the dashboard monitor
        conf_prod = {
            'bootstrap.servers': self.cfg.dashboard.kafka_broker_url,
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': lambda x, ctx: json.dumps(x).encode('utf-8')
        }
        self.producer = SerializingProducer(conf_prod)
        signal.signal(signal.SIGINT, lambda sig, frame: self.signal_handler(sig, frame))
        self.monitor = DashBoardMonitor(self.logger, self.cfg)
        self.monitor_thread = threading.Thread(target=self.health_probes_thread)
        self.monitor_thread.daemon = True
        self.monitor_alive = True
        self.monitor_thread.start()
        

    def start_automatic_attacks(self):
        self.logger.info("Starting automatic Attack Agent")
        self.attack_agent.alive = True
        self.attack_agent.thread.start()
        return "Automatic Attack Agent started!"


    def stop_automatic_attacks(self):
        self.logger.info("Stopping automatic Attack Agent")
        self.attack_agent.alive = False
        if self.attack_agent.thread.is_alive():
            self.attack_agent.thread.join(1)
        self.attack_agent.stop_all_attacks()
        self.logger.info("Attack Agent stopped correctly.")
        return "Automatic Attack Agent stopped!"

    def produce_message(self, data, topic_name):

        try:
            self.producer.produce(topic=topic_name, value=data)  # Send the message to Kafka
            self.logger.debug(f"sent health probes.")
        except Exception as e:
            print(f"Error while producing message to {topic_name} : {e}")


    def signal_handler(self, sig, frame):
        self.monitor_alive = False
        self.monitor_thread.join(1)
        self.logger.info(f"Dashboard monitor stopped correctly.")
        if self.attack_agent.alive:
            self.stop_automatic_attacks()
        self.producer.flush()
        self.logger.info(f"Exiting...")
        exit(0)


    def health_probes_thread(self):
        self.logger.info(f"Starting thread for dashboard health probes")
        while self.monitor_alive:
            health_dict = self.monitor.probe_health()
            self.produce_message(
                data=health_dict, 
                topic_name=f"DASHBOARD_PROBES")
            time.sleep(self.cfg.dashboard.probe_frequency_seconds)


    def get_my_ip(self):
        cmd = "hostname -I | cut -d' ' -f1"
        return subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE).stdout.decode().strip()


    def init_vehicle_status_dict(self):
        vehicle_status_dict = {}
        for vehicle_name in self.vehicle_names:
            vehicle_status_dict[vehicle_name] = HEALTHY
        self.logger.info("Vehicle State Dictionary:")
        for vehicle, state in vehicle_status_dict.items():
            self.logger.info(f"  {vehicle}: {state}")  

        return vehicle_status_dict


    def create_vehicles(self):

        for vehicle_name in self.vehicle_names:

            self.create_producer(vehicle_name)
            self.create_consumer(vehicle_name)

        self.refresh_containers()
        return "Vehicles created!"


    def delete_vehicles(self):
        self.logger.info("Deleting vehicles")
        for producer in self.producers.values():
            producer.stop()
            producer.remove()
        for consumer in self.consumers.values():
            consumer.stop()
            consumer.remove()
        return "Vehicles deleted!"


    def create_producer(self, vehicle_name):
        container_name = f"{vehicle_name}_producer"


        cmd = [
            "docker", "run", "-d",
            "--name", container_name,
            "--network", "of_trains_network",
            "--env", f"VEHICLE_NAME={vehicle_name}",
            "--env", f"HOST_IP={self.host_ip}",
            "--cpuset-cpus", self.producer_manager.vehicle_configs[vehicle_name]['cpu_cores'],
            "--cpu-period", str(self.producer_manager.vehicle_configs[vehicle_name]['cpu_period']),
            "--cpu-quota", str(self.producer_manager.vehicle_configs[vehicle_name]['cpu_quota']),
            "open_fair-producer",
            "tail", "-f", "/dev/null"
        ]
        subprocess.run(cmd)
        

    def create_consumer(self, vehicle_name):
        container_name = f"{vehicle_name}_consumer"
        cmd = [
            "docker", "run", "-d",
            "--name", container_name,
            "--network", "of_trains_network",
            "--env", f"VEHICLE_NAME={vehicle_name}",
            "--env", f"HOST_IP={self.host_ip}",
            "--cpuset-cpus", self.consumer_manager.consumer_configs[vehicle_name]['cpu_cores'],
            "--cpu-period", str(self.consumer_manager.consumer_configs[vehicle_name]['cpu_period']),
            "--cpu-quota", str(self.consumer_manager.consumer_configs[vehicle_name]['cpu_quota']),
            "open_fair-consumer",
            "tail", "-f", "/dev/null"
        ]
        subprocess.run(cmd)


    def refresh_containers(self):     

        for container in self.client.containers.list():
            container_info = self.client.api.inspect_container(container.id)
            # Extract the IP address of the container from its network settings
            container_img_name = container_info['Config']['Image']
            container_ip = container_info['NetworkSettings']['Networks']['of_trains_network']['IPAddress']
            self.logger.info(f'Found {container.name} container with ip {container_ip}')
            if 'producer' in container_img_name:
                self.producers[container.name] = container
            elif 'consumer' in container.name:
                self.consumers[container.name] = container
            elif 'wandber' in container.name:
                self.wandber['container'] = container
                self.federated_learner['container'] = container
                self.security_manager['container'] = container

            self.containers_ips[container.name] = container_ip 
        

        self.producer_manager = ProducerManager(self.cfg, self.producers)
        self.consumer_manager = ConsumerManager(self.cfg, self.consumers)
            
    
    def produce_all(self):
        return self.producer_manager.start_all_producers()


    def stop_producing_all(self):
        self.producer_manager.stop_all_producers()
        return "All producers stopped!"


    def consume_all(self):
        return self.consumer_manager.start_all_consumers()


    def stop_consuming_all(self):
        self.consumer_manager.stop_all_consumers()
        return "All consumers stopped!"


    def stop_wandb(self):
        self.wandber['container']
        try:
            # Try to find and kill the process
            pid_result = self.wandber['container'].exec_run(f"pgrep -f '{WANDBER_COMMAND}'")
            pid = pid_result[1].decode().strip()
            
            if pid:
                self.wandber['container'].exec_run(f"kill -SIGINT {pid}")
                self.logger.info(f"Stopping wandber...")
                return "Stopping wandber..."
            else:
                self.logger.info(f"No running process found for wandber")
                return "No running process found for wandber"
        except Exception as e:
            self.logger.error(f"Error stopping wandber: {e}")
            return "Error stopping wandber"


    def start_wandb(self):

        start_command = f"python wandber.py " + \
            f" --logging_level={self.cfg.logging_level} " + \
            f" --project_name={self.cfg.wandb.project_name} " + \
            f" --run_name={self.cfg.wandb.run_name} " + \
            f" --kafka_broker_url={self.cfg.wandb.kafka_broker_url} " + \
            f" --kafka_consumer_group_id={self.cfg.wandb.kafka_consumer_group_id} " + \
            f" --kafka_auto_offset_reset={self.cfg.wandb.kafka_auto_offset_reset} " + \
            f" --kafka_topic_update_interval_secs={self.cfg.kafka_topic_update_interval_secs}"
                
        if self.cfg.wandb.online:
            start_command += " --online"
        
        def run_wandber(self):
            return_tuple = self.wandber['container'].exec_run(
                 start_command,
                 tty=True,
                 stream=True,
                 stdin=True)
            for line in return_tuple[1]:
                print(line.decode().strip())
        
        thread = threading.Thread(target=run_wandber, args=(self,))
        thread.start()
        return "Wandber consumer started!"


    def start_federated_learning(self):

        start_command = FL_COMMAND + \
            f" --logging_level={self.cfg.logging_level} " + \
            f" --project_name={self.cfg.wandb.project_name} " + \
            f" --run_name={self.cfg.wandb.run_name} " + \
            f" --kafka_broker_url={self.cfg.wandb.kafka_broker_url} " + \
            f" --kafka_consumer_group_id={self.cfg.wandb.kafka_consumer_group_id} " + \
            f" --kafka_auto_offset_reset={self.cfg.wandb.kafka_auto_offset_reset} " + \
            f" --kafka_topic_update_interval_secs={self.cfg.kafka_topic_update_interval_secs}" +\
            f" --aggregation_strategy={self.cfg.federated_learning.aggregation_strategy}" +\
            f" --initialization_strategy={self.cfg.federated_learning.initialization_strategy}" +\
            f" --aggregation_interval_secs={self.cfg.federated_learning.aggregation_interval_secs}" +\
            f" --weights_buffer_size={self.cfg.federated_learning.weights_buffer_size}" +\
            f" --output_dim={self.cfg.anomaly_detection.output_dim}" + \
            f" --h_dim={self.cfg.anomaly_detection.h_dim}" + \
            f" --num_layers={self.cfg.anomaly_detection.num_layers}" +\
            f" --input_dim={self.cfg.anomaly_detection.input_dim}"
        
        if self.cfg.wandb.online:
            start_command += " --online"
        if self.cfg.anomaly_detection.layer_norm:
            start_command += " --layer_norm"
        
        def run_federated_learning(self):
            return_tuple = self.wandber['container'].exec_run(
                 start_command,
                 tty=True,
                 stream=True,
                 stdin=True)
            for line in return_tuple[1]:
                print(line.decode().strip())
        
        thread = threading.Thread(target=run_federated_learning, args=(self,))
        thread.start()
        return "Federated learning started!"
    

    def stop_federated_learning(self):
        try:
            # Try to find and kill the process
            pid_result = self.federated_learner['container'].exec_run(f"pgrep -f '{FL_COMMAND}'")
            pid = pid_result[1].decode().strip()
            
            if pid:
                self.federated_learner['container'].exec_run(f"kill -SIGINT {pid}")
                m = "Stopping FL..."
                self.logger.info(m)
                return m
            else:
                m = "No running process found for federated learning"
                self.logger.info(m)
                return m
        except Exception as e:
            m = f"Error stopping federated learning: {e}"
            self.logger.error(m)
            return m

        
    def start_security_manager(self):

        assert len(self.vehicle_names) > 0, "No vehicles found. Please create vehicles first."
        vehicle_param_str = self.vehicle_names[0]
        for vehicle in self.vehicle_names[1:]:
            vehicle_param_str += f"\ {vehicle}"


        start_command = SM_COMMAND + \
            f" --logging_level={self.cfg.logging_level} " + \
            f" --kafka_broker_url={self.cfg.wandb.kafka_broker_url} " + \
            f" --kafka_consumer_group_id={self.cfg.wandb.kafka_consumer_group_id} " + \
            f" --kafka_auto_offset_reset={self.cfg.wandb.kafka_auto_offset_reset} " + \
            f" --kafka_topic_update_interval_secs={self.cfg.kafka_topic_update_interval_secs}" +\
            f" --initialization_strategy={self.cfg.security_manager.initialization_strategy}" +\
            f" --buffer_size={self.cfg.security_manager.buffer_size}" +\
            f" --batch_size={self.cfg.security_manager.batch_size}" +\
            f" --learning_rate={self.cfg.security_manager.learning_rate}" +\
            f" --epoch_size={self.cfg.security_manager.epoch_size}" +\
            f" --training_freq_seconds={self.cfg.security_manager.training_freq_seconds}" +\
            f" --save_model_freq_epochs={self.cfg.security_manager.save_model_freq_epochs}" +\
            f" --model_saving_path={self.cfg.security_manager.model_saving_path}" + \
            f" --vehicle_names={vehicle_param_str}" + \
            f" --initialization_strategy={self.cfg.security_manager.initialization_strategy}" + \
            f" --input_dim={len(self.cfg.security_manager.probe_metrics)}" + \
            f" --output_dim={self.cfg.security_manager.output_dim}" + \
            f" --h_dim={self.cfg.security_manager.hidden_dim}" + \
            f" --num_layers={self.cfg.security_manager.num_layers}" + \
            f" --dropout={self.cfg.security_manager.dropout}" + \
            f" --optimizer={self.cfg.security_manager.optimizer}" + \
            f" --manager_port={self.cfg.dashboard.port}" + \
            f" --sm_port={self.cfg.security_manager.sm_port}"
            
        
        if self.cfg.security_manager.mitigation:
            start_command += f" --mitigation"
        if self.cfg.security_manager.layer_norm:
            start_command += f" --layer_norm"
        
        def run_security_manager(self):
            return_tuple = self.wandber['container'].exec_run(
                 start_command,
                 tty=True,
                 stream=True,
                 stdin=True)
            for line in return_tuple[1]:
                print(line.decode().strip())
        
        thread = threading.Thread(target=run_security_manager, args=(self,))
        thread.start()
        return "Federated learning started!"
    

    def stop_security_manager(self):
        try:
            # Try to find and kill the process
            pid_result = self.security_manager['container'].exec_run(f"pgrep -f '{SM_COMMAND}'")
            pid = pid_result[1].decode().strip()
            
            if pid:
                self.security_manager['container'].exec_run(f"kill -SIGINT {pid}")
                m = "Stopping SM..."
                self.logger.info(m)
                return m
            else:
                m = "No running process found for security manager"
                self.logger.info(m)
                return m
        except Exception as e:
            m = f"Error stopping security manager: {e}"
            self.logger.error(m)
            return m
        
    
    def start_attack_from_vehicle(self, vehicle_name, origin):

        assert f"{vehicle_name}_producer" in self.producers

        attacking_container = self.producers[f"{vehicle_name}_producer"]

        assert self.cfg.attack.victim_container in self.containers_ips
        try:
            assert self.cfg.attack.victim_container in self.containers_ips
        except AssertionError:
            m = f"Error: Victim container {self.cfg.attack.victim_container} not found in container IPs."
            m += f"\n try one of these :{list(self.containers_ips.keys())}"
            self.logger.error(m)
            return m


        start_attack_command = f"{ATTACK_COMMAND}" + \
            f" --logging_level={self.cfg.logging_level} " + \
            f" --target_ip={self.containers_ips[self.cfg.attack.victim_container]} " + \
            f" --target_port={self.cfg.attack.target_port}" + \
            f" --duration={self.cfg.attack.duration}" + \
            f" --packet_size={self.cfg.attack.packet_size}" + \
            f" --delay={self.cfg.attack.delay}"  

        
        def run_attack():
            return_tuple = attacking_container.exec_run(
                 start_attack_command,
                 tty=True,
                 stream=True,
                 stdin=True)
            for line in return_tuple[1]:
                print(line.decode().strip())
        
        self.vehicle_status_dict[vehicle_name] = INFECTED
        self.last_attack_started_at[vehicle_name] = time.time()
        thread = threading.Thread(target=run_attack)
        thread.start()
        if origin == "AI":
            prefix = "Automatically"
        else:
            prefix = "Manually"
        return f"{prefix} starting attack from {vehicle_name}"


    def stop_attack_from_vehicle(self, vehicle_name, origin):

        assert f"{vehicle_name}_producer" in self.producers

        attacking_container = self.producers[f"{vehicle_name}_producer"]
        reactive_mitigation_time = None
        try:
            # Try to find and kill the process
            pid_result = attacking_container.exec_run(f"pgrep -f '{ATTACK_COMMAND}'")
            pid = pid_result[1].decode().strip()
            
            if pid:
                attacking_container.exec_run(f"kill -SIGINT {pid}")
                m = f"Stopping attack from {vehicle_name}..."
                self.logger.info(m)
                reactive_mitigation_time = time.time() - self.last_attack_started_at[vehicle_name]
                # approx to 3 decimal places
                reactive_mitigation_time = round(reactive_mitigation_time, 3)
                if origin == "AI":     
                    self.logger.info(f"Vehicle {vehicle_name} was automatically healed after {reactive_mitigation_time} seconds.")
                else:
                    self.logger.info(f"Vehicle {vehicle_name} was manually healed after {reactive_mitigation_time} seconds.")
                return {"message" :m, "mitigation_time": reactive_mitigation_time}
            else:
                m = f"No attacking process found in {vehicle_name}"
                self.logger.info(m)
                return {"message" :m, "mitigation_time": reactive_mitigation_time}
        except Exception as e:
            m = f"Error stopping attack from {vehicle_name}: {e}"
            self.logger.error(m)
            return {"message" :m, "mitigation_time": reactive_mitigation_time}
        finally:

            self.vehicle_status_dict[f"{vehicle_name}"] = HEALTHY
            self.logger.debug(f"Vehicle State Dictionary:")
            for vehicle, state in self.vehicle_status_dict.items():
                self.logger.debug(f"  {vehicle}: {state}")
         

    def start_preconf_attack(self):
        for attacking_vehicle_name in self.cfg.attack.preconf_attacking_vehicles:
            self.start_attack_from_vehicle(attacking_vehicle_name, origin="MANUAL")
        return "Preconfigured attack started!"
    

    def stop_preconf_attack(self):
        for attacking_vehicle_name in self.cfg.attack.preconf_attacking_vehicles:
            self.stop_attack_from_vehicle(attacking_vehicle_name, "MANUALLY")
        return "Preconfigured attack stopped!"
    

    def get_vehicle_status(self, vehicle_name):
        return self.vehicle_status_dict[vehicle_name]
    

    def start_mitigation(self):
        security_manager_ip = self.containers_ips['wandber']
        mitigation_service_port = self.cfg.security_manager.sm_port
        url = f"http://{security_manager_ip}:{mitigation_service_port}/start-mitigation"
        response = requests.post(url, json={})
        
        if response.status_code == 200:
            m = "Mitigation started successfully."
            self.logger.info(m)
        else:
            m = f"Failed to start mitigation. Status code: {response.status_code}"
            self.logger.error(m)
        return m, response.status_code


    def stop_mitigation(self):
        security_manager_ip = self.containers_ips['wandber']
        mitigation_service_port = self.cfg.security_manager.sm_port
        url = f"http://{security_manager_ip}:{mitigation_service_port}/stop-mitigation"
        response = requests.post(url, json={})
        
        if response.status_code == 200:
            m = "Mitigation stopped successfully."
            self.logger.info(m)
        else:
            m = f"Failed to stop mitigation. Status code: {response.status_code}"
            self.logger.error(m)
        return m, response.status_code

