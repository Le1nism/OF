# Use a base image of Python (Alpine version for a smaller size)
# This version of Python with Alpine Linux helps reduce the overall image size and improve build speed.
FROM python:3.10-alpine

# Set the working directory inside the container
# All subsequent commands and actions will be executed in this directory.
WORKDIR /data_sensor_simulator_kafka_producer

# Set environment variables for Kafka connection
# KAFKA_BROKER: Address of the Kafka broker for message production.
# TOPIC_NAME: Name of the Kafka topic to publish sensor data to.
ENV KAFKA_BROKER="kafka:9092"
ENV TOPIC_NAME="train-sensor-data"

# Install required build tools and libraries for native dependencies and librdkafka
# This ensures that Python packages with native C/C++ code compile and run correctly.
RUN apk update && apk add --no-cache gcc g++ musl-dev linux-headers librdkafka librdkafka-dev libc-dev python3-dev bash

# Copy the wait-for-it.sh script into the container and set it as executable
# This script is commonly used to wait for dependent services (like Kafka) to be up before starting the main script.
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

# Copy all project files into the working directory of the container
# This includes the Python scripts, configuration files, and any other necessary resources.
COPY . .

# Upgrade pip to the latest version to ensure the latest dependencies can be installed without issues.
RUN pip install --no-cache-dir --upgrade pip

# Install the dependencies specified in the requirements file
# The requirements file should list all Python packages needed for the project.
RUN pip install --no-cache-dir -r requirements_producer.txt

# Command to start the data simulator when the container runs
# This command runs the main Python script to start the data simulation and Kafka message production.
CMD ["python", "data_simulator.py"]
