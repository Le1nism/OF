from flask import Flask, jsonify, request
import logging

class ContainerAPI:

    def __init__(self, container_type, container_name, port = 5000):

        """
        Initialize container API server

        Args:
            container_type (str): Type of container (vehicle, security_manager, etc.)
            container_name (str): Unique name of this container instance
            port (int): Port to run the server on
        """

        self.app = Flask(__name__)
        self.container_type = container_type
        self.container_name = container_name
        self.port = port
        self.status = "initialized"

        # Register routes
        self.register_routes()

        # Configure logging
        self.logger = self.app.logger
        self.logger.setLevel(logging.INFO)

    def register_routes(self):

        @self.app.route('/status', methods = ['GET'])
        def get_status():

            return jsonify({

                "container_type": self.container_type,
                "container_name": self.container_name,
                "status": self.status,
                "details": self.get_detailed_status()
            })

        @self.app.route('/start', methods = ['POST'])
        def start():

            try:

                data = request.get_json() or {}
                result = self.handle_start(data)
                self.status = "running"

                return jsonify({"status": "success", "result": result})

            except Exception as e:

                self.logger.error(f"Error starting container: {str(e)}")

                return jsonify({"status": "error", "message": str(e)}), 500

        @self.app.route('/stop', methods = ['POST'])
        def stop():

            try:

                data = request.get_json() or {}
                result = self.handle_stop(data)
                self.status = "stopped"

                return jsonify({"status": "success", "result": result})

            except Exception as e:

                self.logger.error(f"Error stopping container: {str(e)}")

                return jsonify({"status": "error", "message": str(e)}), 500

        @self.app.route('/command', methods = ['POST'])
        def command():

            try:

                data = request.get_json()
                if not data or 'command' not in data:
                    return jsonify({"status": "error", "message": "Missing command parameter"}), 400

                result = self.handle_command(data['command'], data.get('params', {}))
                return jsonify({"status": "success", "result": result})

            except Exception as e:

                self.logger.error(f"Error executing command: {str(e)}")

                return jsonify({"status": "error", "message": str(e)}), 500

    def get_detailed_status(self):

        """ Override this method to provide container-specific status details """
        return {}

    def handle_start(self, data):

        """ Override this method to handle container start """
        return {"message": "Container started"}

    def handle_stop(self, data):

        """ Override this method to handle container stop """
        return {"message": "Container stopped"}

    def handle_command(self, command, params):

        """
        Override this method to provide container-specific status details

        Args:
            command (str): Command to execute
            params (dict): Command parameters
        """
        return {"message": f"Executed command: {command}", "params": params}

    def run(self):

        """ Run the API server """
        self.app.run(host = '0.0.0.0', port = self.port)

