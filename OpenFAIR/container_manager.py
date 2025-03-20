import docker
import logging
from OpenFAIR.producer_manager import ProducerManager
from OpenFAIR.consumer_manager import ConsumerManager
import threading
import subprocess


WANDBER_COMMAND = "python wandber.py"
FL_COMMAND = "python federated_learning.py"
SM_COMMAND = "python security_manager.py"
ATTACK_COMMAND = "python attack.py"

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

        self.refresh_containers()


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


    def start_wandb(self, cfg):

        start_command = f"python wandber.py " + \
            f" --logging_level={cfg.logging_level} " + \
            f" --project_name={cfg.wandb.project_name} " + \
            f" --run_name={cfg.wandb.run_name} " + \
            f" --kafka_broker_url={cfg.wandb.kafka_broker_url} " + \
            f" --kafka_consumer_group_id={cfg.wandb.kafka_consumer_group_id} " + \
            f" --kafka_auto_offset_reset={cfg.wandb.kafka_auto_offset_reset} " + \
            f" --kafka_topic_update_interval_secs={cfg.kafka_topic_update_interval_secs}"
                
        if cfg.wandb.online:
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


    def start_federated_learning(self, cfg):

        start_command = FL_COMMAND + \
            f" --logging_level={cfg.logging_level} " + \
            f" --project_name={cfg.wandb.project_name} " + \
            f" --run_name={cfg.wandb.run_name} " + \
            f" --kafka_broker_url={cfg.wandb.kafka_broker_url} " + \
            f" --kafka_consumer_group_id={cfg.wandb.kafka_consumer_group_id} " + \
            f" --kafka_auto_offset_reset={cfg.wandb.kafka_auto_offset_reset} " + \
            f" --kafka_topic_update_interval_secs={cfg.kafka_topic_update_interval_secs}" +\
            f" --aggregation_strategy={cfg.federated_learning.aggregation_strategy}" +\
            f" --initialization_strategy={cfg.federated_learning.initialization_strategy}" +\
            f" --aggregation_interval_secs={cfg.federated_learning.aggregation_interval_secs}" +\
            f" --weights_buffer_size={cfg.federated_learning.weights_buffer_size}"
                
        if cfg.wandb.online:
            start_command += " --online"
        
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

        
    def start_security_manager(self, cfg):

        assert len(self.vehicle_names) > 0, "No vehicles found. Please create vehicles first."
        vehicle_param_str = self.vehicle_names[0]
        for vehicle in self.vehicle_names[1:]:
            vehicle_param_str += f"\ {vehicle}"


        start_command = SM_COMMAND + \
            f" --logging_level={cfg.logging_level} " + \
            f" --kafka_broker_url={cfg.wandb.kafka_broker_url} " + \
            f" --kafka_consumer_group_id={cfg.wandb.kafka_consumer_group_id} " + \
            f" --kafka_auto_offset_reset={cfg.wandb.kafka_auto_offset_reset} " + \
            f" --kafka_topic_update_interval_secs={cfg.kafka_topic_update_interval_secs}" +\
            f" --initialization_strategy={cfg.security_manager.initialization_strategy}" +\
            f" --buffer_size={cfg.security_manager.buffer_size}" +\
            f" --batch_size={cfg.security_manager.batch_size}" +\
            f" --learning_rate={cfg.security_manager.learning_rate}" +\
            f" --epoch_size={cfg.security_manager.epoch_size}" +\
            f" --training_freq_seconds={cfg.security_manager.training_freq_seconds}" +\
            f" --save_model_freq_epochs={cfg.security_manager.save_model_freq_epochs}" +\
            f" --model_saving_path={cfg.security_manager.model_saving_path}" + \
            f" --vehicle_names={vehicle_param_str}" + \
            f" --initialization_strategy={cfg.security_manager.initialization_strategy}" + \
            f" --input_dim={cfg.security_manager.input_dim}" + \
            f" --output_dim={cfg.security_manager.output_dim}" + \
            f" --h_dim={cfg.security_manager.hidden_dim}" + \
            f" --num_layers={cfg.security_manager.num_layers}" + \
            f" --dropout={cfg.security_manager.dropout}" + \
            f" --optimizer={cfg.security_manager.optimizer}"
        
        if len(cfg.attack.preconf_attacking_vehicles) > 0:
            preconf_attackers_str_param = cfg.attack.preconf_attacking_vehicles[0]
            for vehicle in cfg.attack.preconf_attacking_vehicles:
                preconf_attackers_str_param += f"\ {vehicle}"
            start_command += \
            f" --preconf_attacking_vehicles={preconf_attackers_str_param}"
        
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
        
    
    def start_attack_from_vehicle(self, cfg, vehicle_name):

        assert f"{vehicle_name}_producer" in self.producers

        attacking_container = self.producers[f"{vehicle_name}_producer"]

        assert cfg.attack.victim_container in self.containers_ips
        try:
            assert cfg.attack.victim_container in self.containers_ips
        except AssertionError:
            m = f"Error: Victim container {cfg.attack.victim_container} not found in container IPs."
            m += f"\n try one of these :{list(self.containers_ips.keys())}"
            self.logger.error(m)
            return m


        start_attack_command = f"{ATTACK_COMMAND}" + \
            f" --logging_level={cfg.logging_level} " + \
            f" --target_ip={self.containers_ips[cfg.attack.victim_container]} " + \
            f" --target_port={cfg.attack.target_port}" + \
            f" --duration={cfg.attack.duration}" + \
            f" --packet_size={cfg.attack.packet_size}" + \
            f" --delay={cfg.attack.delay}"  

        
        def run_attack(self):
            return_tuple = attacking_container.exec_run(
                 start_attack_command,
                 tty=True,
                 stream=True,
                 stdin=True)
            for line in return_tuple[1]:
                print(line.decode().strip())
        
        thread = threading.Thread(target=run_attack, args=(self,))
        thread.start()
        return f"Starting attack from {vehicle_name}"


    def stop_attack_from_vehicle(self, vehicle_name):

        assert f"{vehicle_name}_producer" in self.producers

        attacking_container = self.producers[f"{vehicle_name}_producer"]

        try:
            # Try to find and kill the process
            pid_result = attacking_container.exec_run(f"pgrep -f '{ATTACK_COMMAND}'")
            pid = pid_result[1].decode().strip()
            
            if pid:
                attacking_container.exec_run(f"kill -SIGINT {pid}")
                m = f"Stopping attack from {vehicle_name}..."
                self.logger.info(m)
                return m
            else:
                m = f"No attacking process found in {vehicle_name}"
                self.logger.info(m)
                return m
        except Exception as e:
            m = f"Error stopping attack from {vehicle_name}: {e}"
            self.logger.error(m)
            return m
    

    def start_preconf_attack(self, cfg):
        for attacking_vehicle_name in cfg.attack.preconf_attacking_vehicles:
            self.start_attack_from_vehicle(cfg, attacking_vehicle_name)
        return "Preconfigured attack started!"
    

    def stop_preconf_attack(self, cfg):
        for attacking_vehicle_name in cfg.attack.preconf_attacking_vehicles:
            self.stop_attack_from_vehicle(attacking_vehicle_name)
        return "Preconfigured attack stopped!"