import logging
import time
from OpenFAIR.container_manager import ContainerManager
from https_container_api_client import HTTPSContainerAPIClient

class HTTPSContainerManager(ContainerManager):

    """ Container manager that uses HTTPS API client"""

    def __init__(self, cfg):

        """
        Initialize with HTTPS API client instead of the default
        """

        # Manually initialize properties without calling super().__init__
        self.cfg = cfg
        self.logger = logging.getLogger("HTTPSContainerManager")

        # Use HTTPS client instead of the original
        self.api_client = HTTPSContainerAPIClient()

        # Parse vehicle names from config or environment
        self.vehicle_names = cfg.vehicles.names if hasattr(cfg, 'vehicles') and hasattr(cfg.vehicles, 'names') else []

        # Register containers based on configuration
        self._register_containers()