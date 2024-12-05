import docker
import logging
from OpenFAIR.producer_manager import ProducerManager
from OpenFAIR.consumer_manager import ConsumerManager
from omegaconf import DictConfig
import threading


WANDBER_COMMAND = "python wandber.py"
class ContainerManager:
    
    def __init__(self, cfg):
        self.logger = logging.getLogger("CONTAINER_MANAGER")
        self.logger.setLevel(cfg.logging_level.upper())
        # Connect to the Docker daemon
        self.client = docker.from_env()
        self.containers_dict = {}
        self.wandber = {
            'container': None,
            'thread': None}
        self.producers = {}
        self.consumers = {}
        self.containers_ips = {}
        self.cfg = cfg

        self.refresh_containers()


    def refresh_containers(self):     

        for container in self.client.containers.list():
            container_info = self.client.api.inspect_container(container.id)
            # Extract the IP address of the container from its network settings
            container_info_str = container_info['Config']['Hostname']
            container_img_name = container_info_str.split('(')[0]
            container_ip = container_info['NetworkSettings']['Networks']['open_fair_trains_network']['IPAddress']
            self.logger.info(f'{container_img_name} is {container.name} with ip {container_ip}')
            if 'producer' in container_img_name:
                self.producers[container_img_name] = container
            elif 'consumer' in container_img_name:
                self.consumers[container_img_name] = container
            elif 'wandber' in container_img_name:
                self.wandber['container'] = container
            self.containers_dict[container_img_name] = container
            self.containers_ips[container_img_name] = container_ip 
        

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
                self.logger.info(f"Stopped wandber")
                return "Wandber stopped!"
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
        self.wandber['thread'] = thread
        return "Wandber consumer started!"
