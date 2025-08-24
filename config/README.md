# Configuration System

This directory contains configuration files for the OpenFAIR system, implementing a secure and maintainable approach to configuration management.

## Overview

The new configuration system replaces unsafe command-line argument injection with:
1. **Environment Variables** - For basic, non-changing parameters
2. **YAML Configuration Files** - For complex, vehicle-specific settings
3. **Validation** - Input validation and type safety
4. **Health Checks** - Built-in monitoring endpoints

## Directory Structure

```
config/
├── producers/           # Producer-specific configurations
│   ├── angela.yaml     # Configuration for producer-angela
│   ├── bob.yaml        # Configuration for producer-bob
│   └── ...
└── README.md           # This file
```

## Configuration Priority

The system loads configuration in the following order (later values override earlier ones):

1. **Environment Variables** - Highest priority
2. **YAML Configuration Files** - Medium priority  
3. **Default Values** - Lowest priority

## Environment Variables

### Required Variables
- `VEHICLE_NAME` - Name of the vehicle (e.g., "angela", "bob")

### Optional Variables (with defaults)
- `KAFKA_BROKER` - Kafka broker URL (default: "kafka:9092")
- `LOGGING_LEVEL` - Logging level (default: "INFO")
- `MANAGER_PORT` - Manager service port (default: 5000)
- `MODE` - Operation mode (default: "OF")

### Network Configuration
- `TARGET_IP` - Attack target IP (default: "172.18.0.4")
- `TARGET_PORT` - Attack target port (default: 80)
- `BOT_PORT` - Backdoor port (default: 5002)

### Timing Parameters
- `PROBE_FREQUENCY_SECONDS` - Health probe frequency (default: 2)
- `PING_THREAD_TIMEOUT` - Ping timeout (default: 5)
- `PING_HOST` - Ping target host (default: "www.google.com")

### Attack Parameters
- `DURATION` - Attack duration in seconds (default: 0)
- `PACKET_SIZE` - Attack packet size (default: 1024)
- `DELAY` - Attack delay between packets (default: 0.001)

### Data Generation Parameters
- `MU_ANOMALIES` - Anomaly generation rate (default: 157)
- `MU_NORMAL` - Normal data generation rate (default: 115)
- `ALPHA` - Alpha parameter (default: 0.2)
- `BETA` - Beta parameter (default: 1.9)
- `TIME_EMULATION` - Enable time emulation (default: false)

### Probe Metrics
- `PROBE_METRICS` - Comma-separated list of metrics (default: "RTT,INBOUND,OUTBOUND,CPU,MEM")

## YAML Configuration Files

Each producer has a YAML configuration file that can override environment variables and provide additional settings.

### Example Configuration Structure

```yaml
# Configuration for producer-angela
vehicle:
  name: angela
  flotta: ETR700

data_generation:
  mu_anomalies: 157
  mu_normal: 115
  alpha: 0.2
  beta: 1.9
  time_emulation: false
  
  # Anomaly classes (0-18)
  anomaly_classes: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]
  
  # Diagnostics classes (0-14)
  diagnostics_classes: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]

# Health probe configuration
probe:
  frequency_seconds: 2
  timeout: 5
  host: "www.google.com"
  metrics: [RTT, INBOUND, OUTBOUND, CPU, MEM]

# Attack simulation configuration
attack:
  target_ip: "172.18.0.4"
  target_port: 80
  duration: 0  # 0 means continuous until stopped
  packet_size: 1024
  delay: 0.001
  bot_port: 5002

# System configuration
system:
  mode: "OF"  # OF or SW
  logging_level: "INFO"
  manager_port: 5000
```

## Docker Compose Integration

The docker-compose.yml file mounts configuration files and sets environment variables:

```yaml
producer-angela:
  build:
    context: ./producer
    dockerfile: Dockerfile
  environment:
    - VEHICLE_NAME=angela
    - KAFKA_BROKER=kafka:9092
    # ... other environment variables
  volumes:
    - ./config/producers/angela.yaml:/app/config.yaml:ro
  command: ["python", "produce.py"]
```

## Health Checks

Each producer exposes a health check endpoint at `http://localhost:5000/health`:

```json
{
  "status": "healthy",
  "vehicle": "angela",
  "running": true,
  "under_attack": false,
  "records_produced": 1234
}
```

## Security Benefits

1. **No Command Injection** - No shell command execution
2. **Input Validation** - All parameters validated before use
3. **Type Safety** - Proper data types maintained
4. **Access Control** - Configuration files are read-only in containers

## Adding New Producers

To add a new producer:

1. Create a new YAML configuration file in `config/producers/`
2. Add a new service to `docker-compose.yml`
3. Set the `VEHICLE_NAME` environment variable
4. Mount the configuration file as a volume

Example:
```yaml
producer-charlie:
  build:
    context: ./producer
    dockerfile: Dockerfile
  environment:
    - VEHICLE_NAME=charlie
    # ... other environment variables
  volumes:
    - ./config/producers/charlie.yaml:/app/config.yaml:ro
  command: ["python", "produce.py"]
```

## Troubleshooting

### Configuration Validation Errors
- Check that all required environment variables are set
- Verify YAML syntax in configuration files
- Ensure numeric values are within valid ranges

### Health Check Failures
- Verify the producer is running: `docker logs producer-angela`
- Check configuration loading: Look for configuration errors in logs
- Ensure the health endpoint is accessible: `curl http://localhost:5000/health`

### Environment Variable Issues
- Use the `env.example` file as a template
- Ensure variables are properly quoted in docker-compose.yml
- Check for typos in variable names
