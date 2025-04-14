from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import subprocess
import os
import signal
import json

app = FastAPI(title = "Train Server")

class TrainConfig(BaseModel):

	kafka_broker: str
	buffer_size: int
	batch_size: int
	logging_level: str
	# Other configuration parameters

class TrainController:

	def __init__(self):

		self.process = None
		self.config = None

	def start_consumer(self, config: TrainConfig):

		if self.process:
			return {"status": "already_running"}
		
		# Convert config to command line arguments
		cmd = self._build_command(config)
		
		# Start the consumer process
		self.process = subprocess.Popen(
			cmd,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
			text=True
		)
		
		self.config = config
		return {"status": "started", "pid": self.process.pid}
	
	def stop_consumer(self):

		if not self.process:
			return {"status": "not_running"}
		
		# Send SIGINT to the process
		self.process.send_signal(signal.SIGINT)
		self.process.wait(timeout=10)
		
		if self.process.poll() is None:
			# Force kill if it doesn't terminate
			self.process.kill()
		
		self.process = None
		self.config = None
		return {"status": "stopped"}
	
	def get_status(self):

		if not self.process:
			return {"status": "stopped"}
		
		return {
			"status": "running",
			"pid": self.process.pid,
			"config": self.config.dict() if self.config else None
		}
	
	def _build_command(self, config: TrainConfig):

		# Build the command line arguments from the config
		cmd = ["python", "consume.py"]
		for key, value in config.dict().items():
			cmd.extend([f"--{key}", str(value)])
		return cmd

controller = TrainController()

@app.post("/start")
async def start_train(config: TrainConfig):
	return controller.start_consumer(config)

@app.post("/stop")
async def stop_train():
	return controller.stop_consumer()

@app.get("/status")
async def get_status():
	return controller.get_status()

@app.get("/health")
async def health_check():
	return {"status": "healthy"}

if __name__ == "__main__":
	uvicorn.run(app, host="0.0.0.0", port=8000)