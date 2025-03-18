#!/bin/bash

# Get all container IDs whose names end with "consumer" or "producer"
containers=$(docker ps -a --format "{{.ID}} {{.Names}}" | grep -E "(consumer|producer)$" | awk '{print $1}')

# Check if there are any containers to remove
if [ -z "$containers" ]; then
  echo "No containers found with names ending in 'consumer' or 'producer'."
  exit 0
fi

# Remove the containers
for container in $containers; do
  echo "Removing container ID: $container"
  docker rm -f $container
done

echo "All matching containers have been removed."