# Use a base image of Python (Alpine version for a smaller size)
# Alpine is chosen for its lightweight nature, which reduces the overall image size.
FROM python:3.10-alpine

# Set the working directory inside the container
# All subsequent commands and operations will be executed in this directory.
WORKDIR /flask_app_kafka_consumer

# Set environment variables for Flask
# FLASK_APP: Specifies the main application file to run when Flask starts.
# FLASK_RUN_HOST: Configures Flask to listen on all network interfaces, allowing external access.
# FLASK_RUN_PORT: Defines the port number on which Flask will listen for incoming requests.
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000

# Set additional environment variables for Kafka connection
# KAFKA_BROKER: Address of the Kafka broker.
# TOPIC_NAME: The name of the Kafka topic that the consumer will listen to.
ENV KAFKA_BROKER="kafka:9092"
ENV TOPIC_NAME="train-sensor-data"

# Install required build tools and libraries for native dependencies and librdkafka
# These packages ensure that Python modules with C/C++ extensions compile and run properly.
RUN apk update && apk add --no-cache gcc g++ musl-dev linux-headers librdkafka librdkafka-dev libc-dev python3-dev bash

# Copy the wait-for-it.sh script into the container and set executable permissions
# The script is used to wait for dependent services (such as Kafka) to become available before running the main app.
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

# Copy all project files into the working directory of the container
# This includes Python scripts, Flask configuration, and any additional resources needed by the application.
COPY . .

# Upgrade pip to the latest version
# Ensures that the latest packages and features are available.
RUN pip install --no-cache-dir --upgrade pip

# Install the dependencies specified in the requirements file
# The requirements file should list all Python packages needed for the Flask app and Kafka consumer.
RUN pip install --no-cache-dir -r requirements_consumer.txt

# Expose port 5000 to allow external access to the Flask app
# This makes the port accessible outside the container.
EXPOSE 5000

# Command to start the Flask application when the container runs
# The `flask run` command launches the Flask server using the settings defined by the environment variables.
CMD ["flask", "run"]
