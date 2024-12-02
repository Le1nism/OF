import docker
import threading
import time
import subprocess
import json
import random
import yaml
import http.server
import socketserver
import json
from omegaconf import DictConfig, OmegaConf 
import hydra


def run_command_in_container(container, command):
    # Run the command in the container shell to obtain the PID
    exec_result = container.exec_run(f"sh -c '{command} & echo $!'")
    pid = exec_result.output.decode("utf-8").strip()
    return pid

def refresh_containers():
    global containers_dict, containers_ips
    # Connect to the Docker daemon
    client = docker.from_env()

    for container in client.containers.list():

        container_info = client.api.inspect_container(container.id)
        # Extract the IP address of the container from its network settings
        container_info_str = container_info['Config']['Hostname']
        container_img_name = container_info_str.split('(')[0]
        container_ip = container_info['NetworkSettings']['Networks']['trainsensordatavisualization_trains_network']['IPAddress']
        print(f'{container_img_name} is {container.name} with ip {container_ip}')
        containers_dict[container_img_name] = container
        containers_ips[container_img_name] = container_ip 
    print('\n\n\n')
            

class MyRequestHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/refresh_containers':
                refresh_containers()
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {'message': 'Containers refreshed!'}
                self.wfile.write(json.dumps(response).encode())
            else:
                super().do_GET()


# Create a TCPServer instance with SO_REUSEADDR option
class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True  # This allows the server to reuse the address


@hydra.main(config_path="config", config_name="default", version_base="1.2")
def main(cfg: DictConfig) -> None:
    global containers_dict, containers_ips
    print("\n________________________________________________________________\n\n"+\
          "               OPEN FAIR Container Manager \n" +\
          "________________________________________________________________\n"+\
          "\n"+\
          "IMPORTANT:  - Parameters are read from the open_fair.yaml file at project's root dir. \n" +\
          "            - Re-launch this script each time you change container status (through node restart). \n\n\n")
    
    containers_dict = {}
    containers_ips = {}
    refresh_containers() 
    
    with ReusableTCPServer(("", cfg.container_manager_port), MyRequestHandler) as httpd:
        print(f"Serving at port {cfg.container_manager_port}")
        httpd.serve_forever()
    
    
    
    
        
    

if __name__ == "__main__":
    main()

    

    



