#!/bin/bash
# List of container names
containers=("claude_consumer" \
            "claude_producer" \
            "angela_consumer" \
            "angela_producer" \
            "bob_consumer" \
            "bob_producer" \
            "frank_consumer" \
            "frank_producer" \
            "emily_consumer" \
            "emily_producer" \
            "daniel_consumer" \
            "daniel_producer" \
            "wandber")

# Loop through each container and perform git pull
for container in "${containers[@]}"; do
    echo "Reinstalling reqs in $container..."
    docker exec -it $container /bin/bash -c 'pip install -r requirements.txt'
done
