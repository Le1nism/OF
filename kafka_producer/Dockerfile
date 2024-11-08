# Use a base image of Python (Alpine version for a smaller size)
FROM python:3.10-alpine

# Set the working directory inside the container
WORKDIR /data_sensor_simulator_kafka_producer

# Set environment variables
ENV KAFKA_BROKER="kafka:9092"
ENV TOPIC_NAME="train-sensor-data"
#ENV SCHEMA_REGISTRY_URL="http://schema-registry:8081"

# Install build tools required for native dependencies and librdkafka
RUN apk update
RUN apk add --no-cache gcc g++ musl-dev linux-headers librdkafka librdkafka-dev libc-dev python3-dev

# Copy all project files into the working directory of the container
COPY . .

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir kafka-python

# Install the dependencies specified in requirements_all.txt
# This will also help in case you add more dependencies later.
RUN pip install --no-cache-dir -r requirements_producer.txt

# Command to start the data simulator when the container runs
CMD ["python", "data_simulator.py"]
