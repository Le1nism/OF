#!/bin/bash

# Name of your Kafka container
KAFKA_CONTAINER_NAME="kafka"

# Command to delete all topics inside the Kafka container
docker exec -it $KAFKA_CONTAINER_NAME /bin/bash -c '
for topic in $(/usr/bin/kafka-topics --bootstrap-server localhost:9092 --list); do
    echo deleting topic $topic
    /usr/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic "$topic"
done
echo "All topics deleted"
'
