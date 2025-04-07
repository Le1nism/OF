import requests
import logging
from typing import Dict, List, Any

class HTTPSContainerAPIClient:
    """ API Client that uses HTTPS to communicate with containers """

    def __init__(self):

        self.logger = logging.getLogger("HTTPSContainerAPIClient")
        self.containers = {} # Store container information
        self.timeout = 5 # Request timeout in seconds

    def register_container(self, name: str, host: str, port: int, container_type: str):

        """ Register a container with the client """
        self.containers[name] = {

            "host": host,
            "port": port,
            "type": container_type,
            "url": f"https://{host}:{port}"
        }

        self.logger.info(f"Registered container: {name} ({container_type}) at {host}:{port}")

    def get_containers_by_type(self, container_type: str) -> List[str]:

        """ Get all container names of a specific type """
        return [name for name, info in self.containers.items()
                if info["type"] == container_type]

    def get_status(self, container_name: str) -> Dict:

        """ Get status of a container using HTTPS """
        if container_name not in self.containers:
            raise ValueError(f"Container {container_name} not registered")

        url = f"{self.containers[container_name]['url']}/status"
        try:

            response = requests.get(url, timeout = self.timeout, verify = False)
            response.raise_for_status()

            return response.json()

        except Exception as e:

            self.logger.error(f"Error getting status for {container_name}: {str(e)}")

            return {"status": "error", "message": str(e)}

    def start_container(self, container_name: str, params: Dict = None) -> Dict:

        """ Start a container using HTTPS """
        if container_name not in self.containers:
            raise ValueError(f"Container {container_name} not registered")

        url = f"{self.containers[container_name]['url']}/start"
        try:

            response = requests.post(url, json = params or {}, timeout = self.timeout, verify = False)
            response.raise_for_status()

            return response.json()

        except Exception as e:

            self.logger.error(f"Error starting container {container_name}: {str(e)}")

            return {"status": "error", "message": str(e)}

    def stop_container(self, container_name: str, params: Dict = None) -> Dict:

        """ Stop a container using HTTPS """

        if container_name not in self.containers:
            raise ValueError(f"Container {container_name} not registered")

        url = f"{self.containers[container_name]['url']}/stop"
        try:

            response = requests.post(url, json=params or {}, timeout=self.timeout, verify=False)
            response.raise_for_status()

            return response.json()

        except Exception as e:

            self.logger.error(f"Error stopping container {container_name}: {str(e)}")

            return {"status": "error", "message": str(e)}

    def send_command(self, container_name: str, command: str, params: Dict = None) -> Dict:

        """Send a command to a container using HTTPS"""

        if container_name not in self.containers:
            raise ValueError(f"Container {container_name} not registered")

        url = f"{self.containers[container_name]['url']}/command"
        payload = {

            "command": command,
            "params": params or {}
        }

        try:

            response = requests.post(url, json=payload, timeout=self.timeout, verify=False)
            response.raise_for_status()

            return response.json()

        except Exception as e:

            self.logger.error(f"Error sending command {command} to {container_name}: {str(e)}")

            return {"status": "error", "message": str(e)}

    def batch_command(self, container_names: List[str], command: str, params: Dict = None) -> Dict[str, Any]:

        """Send a command to multiple containers"""

        results = {}
        for container in container_names:

            if command == "start":
                results[container] = self.start_container(container, params)

            elif command == "stop":
                results[container] = self.stop_container(container, params)

            else:
                results[container] = self.send_command(container, command, params)

        return results