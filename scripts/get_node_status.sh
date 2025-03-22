#!/bin/bash

# List of vehicle names
vehicle_names=("angela" "bob" "claude" "emily" "daniel" "frank")

# Loop through each vehicle name and make the POST request
for vehicle_name in "${vehicle_names[@]}"; do
    echo "Vehicle: ${vehicle_name}"
    curl -X POST -H "Content-Type: application/json" \
         -d "{\"vehicle_name\": \"${vehicle_name}\"}" \
         http://localhost:5000/vehicle-status
    echo # Print a blank line for separation
done