import docker
import logging
import requests
import time
import yaml
import os

class ProducerManager:
    def __init__(self, cfg, producers, containers_ips, PRODUCER_COMMAND="python produce.py"):
        self.cfg = cfg
        self.producers = producers
        self.containers_ips = containers_ips
        self.producer_command = PRODUCER_COMMAND
        self.logging_level = cfg.logging_level
        self.mode = cfg.mode
        self.manager_port = cfg.container_manager_port
        self.probe_metrics = cfg.security_manager.probe_metrics
        self.no_proxy_host = cfg.dashboard.proxy
        self.attack_config = cfg.attack
        
        # Initialize vehicle configurations
        self.vehicle_configs = {}
        self.vehicle_names = []
        
        for vehicle in cfg.vehicles:
            if type(vehicle) == str:
                vehicle_name = vehicle
                vehicle_config = cfg.default_vehicle_config.copy()
            else:
                vehicle_name = list(vehicle.keys())[0]
                vehicle_config = cfg.default_vehicle_config.copy()
                vehicle_config.update(vehicle[vehicle_name])
            
            self.vehicle_names.append(vehicle_name)
            self.vehicle_configs[vehicle_name] = vehicle_config
            
            # Set default classes if not specified
            if vehicle_config.get("anomaly_classes") == "all":
                self.vehicle_configs[vehicle_name]["anomaly_classes"] = list(range(0, 19))
            if vehicle_config.get("diagnostics_classes") == "all":
                self.vehicle_configs[vehicle_name]["diagnostics_classes"] = list(range(1, 15))

    def start_all_producers(self):
        """Start all producers using HTTP API"""
        results = []
        for producer_name, vehicle_name in zip(self.producers.keys(), self.vehicle_names):
            result = self.start_producer(producer_name, self.producers[producer_name], self.vehicle_configs[vehicle_name])
            results.append(result)
        return "All producers started!", results

    def start_producer(self, producer_name, producer_container, vehicle_config):
        """Start producer using HTTP API instead of command execution"""
        try:
            # Get container IP
            container_ip = self.containers_ips.get(producer_name)
            if not container_ip:
                return f"Failed to start producer {producer_name}: Container IP not found"
            
            api_url = f"http://{container_ip}:5000"
            
            # Step 1: Configure the producer
            config_data = self._build_config_data(vehicle_config)
            config_response = requests.post(
                f"{api_url}/configure",
                json=config_data,
                timeout=30
            )
            config_response.raise_for_status()
            
            # Step 2: Start the producer
            start_response = requests.post(
                f"{api_url}/start",
                timeout=30
            )
            start_response.raise_for_status()
            
            # Step 3: Verify it's running
            status_response = requests.get(f"{api_url}/status", timeout=10)
            status_response.raise_for_status()
            
            logging.getLogger("PRODUCER_MANAGER").info(f"Producer {producer_name} started successfully")
            return f"Producer {producer_name} started successfully"
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to start producer {producer_name}: {e}"
            logging.getLogger("PRODUCER_MANAGER").error(error_msg)
            return error_msg
        except Exception as e:
            error_msg = f"Failed to start producer {producer_name}: {e}"
            logging.getLogger("PRODUCER_MANAGER").error(error_msg)
            return error_msg

    def stop_producer(self, producer_name, producer_container):
        """Stop producer using HTTP API"""
        try:
            container_ip = self.containers_ips.get(producer_name)
            if not container_ip:
                return f"Failed to stop producer {producer_name}: Container IP not found"
            
            api_url = f"http://{container_ip}:5000"
            response = requests.post(f"{api_url}/stop", timeout=30)
            response.raise_for_status()
            
            return f"Producer {producer_name} stopped successfully"
        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to stop producer {producer_name}: {e}"
            logging.getLogger("PRODUCER_MANAGER").error(error_msg)
            return error_msg

    def get_producer_status(self, producer_name, producer_container):
        """Get producer status via HTTP API"""
        try:
            container_ip = self.containers_ips.get(producer_name)
            if not container_ip:
                return {"error": "Container IP not found"}
            
            api_url = f"http://{container_ip}:5000"
            response = requests.get(f"{api_url}/status", timeout=10)
            response.raise_for_status()
            
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

    def update_producer_config(self, producer_name, producer_container, new_config):
        """Update producer configuration via HTTP API"""
        try:
            container_ip = self.containers_ips.get(producer_name)
            if not container_ip:
                return f"Failed to update producer {producer_name}: Container IP not found"
            
            api_url = f"http://{container_ip}:5000"
            response = requests.put(
                f"{api_url}/config",
                json=new_config,
                timeout=30
            )
            response.raise_for_status()
            
            return f"Producer {producer_name} configuration updated successfully"
        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to update producer {producer_name}: {e}"
            logging.getLogger("PRODUCER_MANAGER").error(error_msg)
            return error_msg

    def _build_config_data(self, vehicle_config):
        """Build configuration data for HTTP API"""
        config_data = {
            'vehicle_name': vehicle_config.get('vehicle_name'),
            'kafka_broker': vehicle_config.get('kafka_broker', 'kafka:9092'),
            'logging_level': self.logging_level,
            'manager_port': self.manager_port,
            'mode': self.mode,
            
            # Network configuration
            'target_ip': self.attack_config.get('target_ip', '172.18.0.4'),
            'target_port': self.attack_config.get('target_port', 80),
            'bot_port': self.attack_config.get('bot_port', 5002),
            
            # Timing parameters
            'probe_frequency_seconds': vehicle_config.get('probe_frequency_seconds', 2),
            'ping_thread_timeout': vehicle_config.get('ping_thread_timeout', 5),
            'ping_host': vehicle_config.get('ping_host', 'www.google.com'),
            
            # Attack parameters
            'duration': self.attack_config.get('duration', 0),
            'packet_size': self.attack_config.get('packet_size', 1024),
            'delay': self.attack_config.get('delay', 0.001),
            
            # Data generation parameters
            'mu_anomalies': vehicle_config.get('mu_anomalies', 157),
            'mu_normal': vehicle_config.get('mu_normal', 115),
            'alpha': vehicle_config.get('alpha', 0.2),
            'beta': vehicle_config.get('beta', 1.9),
            'time_emulation': vehicle_config.get('time_emulation', False),
            
            # Probe metrics
            'probe_metrics': self.probe_metrics,
            
            # Anomaly and diagnostics classes
            'anomaly_classes': vehicle_config.get('anomaly_classes', list(range(0, 19))),
            'diagnostics_classes': vehicle_config.get('diagnostics_classes', list(range(0, 15)))
        }
        
        return config_data

    def stop_all_producers(self):
        """Stop all producers using HTTP API"""
        results = []
        for producer_name in self.producers.keys():
            result = self.stop_producer(producer_name, self.producers[producer_name])
            results.append(result)
        return "All producers stopped!", results

    def get_all_producer_statuses(self):
        """Get status of all producers"""
        statuses = {}
        for producer_name, producer_container in self.producers.items():
            status = self.get_producer_status(producer_name, producer_container)
            statuses[producer_name] = status
        return statuses