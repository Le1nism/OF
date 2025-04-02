import logging
import os
import signal
import time
from typing import Dict, List, Any
from dashboard.container_api_client import ContainerAPIClient


class ContainerManager:

    def __init__(self, cfg):

        """
        Initialize the container manager

        Args:
            cfg: Configuration object
        """

        self.cfg = cfg
        self.logger = logging.getLogger("ContainerManager")
        self.api_client = ContainerAPIClient()

        # Parse vehicle names from config or environment
        self.vehicle_names = cfg.vehicles.names if hasattr(cfg, 'vehicles') and hasattr(cfg.vehicles, 'names') else []

        # Register containers based on configuration
        self._register_containers()

    def _register_containers(self):

        """ Register all containers with the API client """

        # Register vehicle containers
        for vehicle_name in self.vehicle_names:

            # Extract base vehicle name (e.g., "e700" from "e700_4801")
            base_name = vehicle_name.split("_")[0]

            # Register producer container
            self.api_client.register_container(

                name = f"{vehicle_name}_producer",
                host = f"{base_name}-producer", # Hostname matches container name in docker-compose
                port = 5000,
                container_type = "vehicle_producer"
            )

            # Register consumer container
            self.api_client.register_container(

                name=f"{vehicle_name}_consumer",
                host=f"{base_name}-consumer",  # Hostname matches container name in docker-compose
                port=5000,
                container_type="vehicle_consumer"
            )

        # Register other service containers
        self.api_client.register_container(

            name = "security_manager",
            host = "security-manager",
            port = 5000,
            container_type = "security_manager"
        )

        self.api_client.register_container(

            name = "federated_learning",
            host = "federated-learning",
            port = 5000,
            container_type = "federated_learning"
        )

        self.api_client.register_container(

            name = "wandb",
            host = "wandb",
            port = 5000,
            container_type = "wandb"
        )

    def get_vehicle_status(self, vehicle_name: str) -> Dict:

        """
        Get status information for a vehicle

        Args:
            vehicle_name (str): Vehicle name/ID

        Returns:
            dict: Status information
        """

        try:

            producer_status = self.api_client.get_status(f"{vehicle_name}_producer")
            consumer_status = self.api_client.get_status(f"{vehicle_name}_consumer")

            return {

                "vehicle_name": vehicle_name,
                "producer": producer_status,
                "consumer": consumer_status,
                "status": "ok"
            }

        except Exception as e:

            self.logger.error(f"Error getting status for vehicle {vehicle_name}: {str(e)}")

            return {

                "vehicle_name": vehicle_name,
                "status": "error",
                "message": str(e)
            }

    def start_attack_from_vehicle(self, vehicle_name: str, origin: str = "MANUAL") -> str:

        """
        Start an attack from a specific vehicle

        Args:
            vehicle_name (str): Vehicle to launch attack from
            origin (str): Origin of attack request (MANUAL or AUTO)

        Returns:
            str: Status message
        """

        try:

            # Get all other vehicles as targets
            targets = [v for v in self.vehicle_names if v.split("_")[0] != vehicle_name]
            if not targets:
                return "No targets available"

            # Choose the first target for simplicity
            target = targets[0]

            # Send attack command to the producer container
            result = self.api_client.send_command(

                f"{vehicle_name}_producer",
                "start_attack",
                {
                    "target": target,
                    "origin": origin
                }
            )

            return f"Attack started from {vehicle_name} targeting {target}"

        except Exception as e:

            self.logger.error(f"Error starting attack from {vehicle_name}: {str(e)}")

            return f"Error starting attack: {str(e)}"

    def stop_attack_from_vehicle(self, vehicle_name: str, origin: str = "MANUAL") -> str:

        """
        Stop an attack from a specific vehicle

        Args:
            vehicle_name (str): Vehicle to stop attack from
            origin (str): Origin of stop request (MANUAL or AUTO)

        Returns:
            str: Status message
        """

        try:

            result = self.api_client.send_command(

                f"{vehicle_name}_producer",
                "stop_attack",
                {"origin": origin}
            )

            return f"Attack stopped from {vehicle_name}"

        except Exception as e:

            self.logger.error(f"Error stopping attack from {vehicle_name}: {str(e)}")

            return f"Error stopping attack: {str(e)}"

    def produce_all(self) -> str:

        """
        Start all producers

        Returns:
            str: Status message
        """

        try:

            producer_containers = self.api_client.get_containers_by_type("vehicle_producer")
            results = self.api_client.batch_command(producer_containers, "start", {"producer": True})

            success_count = sum(1 for result in results.values() if result.get("status") != "error")
            return f"Started {success_count}/{len(producer_containers)} producers"

        except Exception as e:

            self.logger.error(f"Error starting producers: {str(e)}")

            return f"Error starting producers: {str(e)}"

    def stop_producing_all(self) -> str:

        """
        Stop all producers

        Returns:
            str: Status message
        """

        try:

            producer_containers = self.api_client.get_containers_by_type("vehicle_producer")
            results = self.api_client.batch_command(producer_containers, "stop", {"producer": True})

            success_count = sum(1 for result in results.values() if result.get("status") != "error")
            return f"Stopped {success_count}/{len(producer_containers)} producers"

        except Exception as e:

            self.logger.error(f"Error stopping producers: {str(e)}")

            return f"Error stopping producers: {str(e)}"

    def consume_all(self) -> str:

        """
        Start all consumers

        Returns:
            str: Status message
        """

        try:

            consumer_containers = self.api_client.get_containers_by_type("vehicle_consumer")
            results = self.api_client.batch_command(consumer_containers, "start", {"consumer": True})

            success_count = sum(1 for result in results.values() if result.get("status") != "error")
            return f"Started {success_count}/{len(consumer_containers)} consumers"

        except Exception as e:

            self.logger.error(f"Error starting consumers: {str(e)}")

            return f"Error starting consumers: {str(e)}"

    def stop_consuming_all(self) -> str:

        """
        Stop all consumers

        Returns:
            str: Status message
        """

        try:

            consumer_containers = self.api_client.get_containers_by_type("vehicle_consumer")
            results = self.api_client.batch_command(consumer_containers, "stop", {"consumer": True})

            success_count = sum(1 for result in results.values() if result.get("status") != "error")
            return f"Stopped {success_count}/{len(consumer_containers)} consumers"

        except Exception as e:

            self.logger.error(f"Error stopping consumers: {str(e)}")

            return f"Error stopping consumers: {str(e)}"

    # TODO: Implement other methods in a similar way

    def start_security_manager(self) -> str:

        """
        Start the security manager

        Returns:
            str: Status message
        """

        try:

            result = self.api_client.start_container("security_manager")

            return "Security manager started"

        except Exception as e:

            self.logger.error(f"Error starting security manager: {str(e)}")

            return f"Error starting security manager: {str(e)}"

    def stop_security_manager(self) -> str:

        """
        Stop the security manager

        Returns:
            str: Status message
        """

        try:

            result = self.api_client.stop_container("security_manager")

            return "Security manager stopped"

        except Exception as e:

            self.logger.error(f"Error stopping security manager: {str(e)}")

            return f"Error stopping security manager: {str(e)}"

    def signal_handler(self, sig, frame):

        """
        Handle shutdown signals
        """

        self.logger.info("Shutting down all containers...")

        try:

            # Stop all containers
            all_containers = list(self.api_client.containers.keys())
            for container in all_containers:
                try:
                    self.api_client.stop_container(container)
                except Exception as e:
                    self.logger.error(f"Error stopping container {container}: {str(e)}")

        except Exception as e:
            self.logger.error(f"Error in shutdown: {str(e)}")
