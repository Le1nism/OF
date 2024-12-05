import docker
import logging
from OpenFAIR.producer_manager import ProducerManager
from OpenFAIR.consumer_manager import ConsumerManager
from omegaconf import DictConfig


class VehicleManager:
    def __init__(self, cfg):
        self.logger = logging.getLogger("VEHICLE_MANAGER")
        self.logger.setLevel(cfg.logging_level.upper())
        self.default_vehicle_config = cfg.default_vehicle_config
        self.vehicle_names = cfg.vehicles
        self.vehicle_configs = {}
        for vehicle_name in self.vehicle_names:
            self.vehicle_configs[vehicle_name] = self.default_vehicle_config

class ContainerManager:
    
    def __init__(self, cfg):
        self.logger = logging.getLogger("CONTAINER_MANAGER")
        self.logger.setLevel(cfg.logging_level.upper())
        self.vehicle_manager = VehicleManager(cfg)
        # Connect to the Docker daemon
        self.client = docker.from_env()
        self.containers_dict = {}
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
            self.containers_dict[container_img_name] = container
            self.containers_ips[container_img_name] = container_ip 
        

        self.producer_manager = ProducerManager(self.cfg, self.producers)
        self.consumer_manager = ConsumerManager(self.cfg, self.consumers)
            
    
    def produce_all(self):
        # Start all producers
        for producer_name, vehicle_name in zip(self.producers.keys(), self.vehicle_manager.vehicle_names):
            self.producer_manager.start_producer(
                producer_name,
                self.producers[producer_name],
                vehicle_name,
                self.vehicle_manager.vehicle_configs[vehicle_name])
        return "All producers started!"


    def stop_producing_all(self):
        self.producer_manager.stop_all_producers()
        return "All producers stopped!"


    def consume_all(self):
        # Start all consumers
        for consumer_name, vehicle_name in zip(self.consumers.keys(), self.vehicle_manager.vehicle_names):
            self.consumer_manager.start_consumer(
                consumer_name, 
                self.consumers[consumer_name], 
                vehicle_name)
        return "All consumers started!"


    def stop_consuming_all(self):
        self.consumer_manager.stop_all_consumers()
        return "All consumers stopped!"
