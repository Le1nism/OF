# Use a base image of Python (Alpine version for a smaller size)
FROM python:3.10-alpine

# Set the working directory inside the container
WORKDIR /synthetic_data_sensor_generator_kafka_producer

# Set environment variables for Kafka connection
ENV KAFKA_BROKER="kafka:9092"
ENV TOPIC_NAME="train-sensor-data"

# Install required build tools and libraries for native dependencies and librdkafka
RUN apk update && apk add --no-cache gcc g++ musl-dev linux-headers librdkafka librdkafka-dev libc-dev python3-dev bash

# Copy the wait-for-it.sh script and set executable permissions
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

# Copy all project files into the working directory of the container
COPY . .

# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip
# Install the dependencies specified in the requirements file and others dependencing
RUN pip install --no-cache-dir -r requirements_real_producer.txt
RUN pip install --no-cache-dir seaborn matplotlib scikit-learn scipy pandas numpy copulas

# Command to start the data simulator when the container runs
CMD ["python", "synthetic_data_generator.py"]
