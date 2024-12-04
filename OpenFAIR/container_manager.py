import docker
import logging
from OpenFAIR.producer_manager import ProducerManager
from OpenFAIR.consumer_manager import ConsumerManager

class ContainerManager:
    
    def __init__(self, cfg):
        self.logger = logging.getLogger("CONTAINER_MANAGER")
        self.logger.setLevel(cfg.logging_level.upper())
        
        # Connect to the Docker daemon
        self.client = docker.from_env()
        self.containers_dict = {}
        self.producers = {}
        self.consumers = {}
        self.containers_ips = {}
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
            self.containers_dict[container_img_name] = container
            self.containers_ips[container_img_name] = container_ip 
        

        self.producer_manager = ProducerManager(self.producers)
        self.consumer_manager = ConsumerManager(self.consumers)
            