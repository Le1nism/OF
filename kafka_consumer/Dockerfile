# Use a base image of Python (Alpine version for a smaller size)
FROM python:3.10-alpine

# Set the working directory inside the container
WORKDIR /flask_app_kafka_consumer

# Set environment variables for Flask
# Specify the main Flask application file
ENV FLASK_APP=app.py
# Set the host to allow connections from any IP address
ENV FLASK_RUN_HOST=0.0.0.0
# Set the port on which Flask will listen
ENV FLASK_RUN_PORT=5000
# Set additional environment variables for Kafka connection
ENV KAFKA_BROKER="kafka:9092"
ENV TOPIC_NAME="train-sensor-data"


# Install build tools required for native dependencies and librdkafka
RUN apk update && apk add --no-cache gcc g++ musl-dev linux-headers librdkafka librdkafka-dev libc-dev python3-dev bash

# Copy the wait-for-it.sh script and set executable permissions
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

# Copy all project files into the working directory of the container
COPY . .

# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip
# Install the dependencies specified in the requirements file
RUN pip install --no-cache-dir -r requirements_consumer.txt

# Expose port 5000 to allow access to the Flask app
EXPOSE 5000

# Command to start the application when the container runs
CMD ["flask", "run"]
