#!/usr/bin/env python3
"""
Test script to validate producer configuration loading
"""
import os
import sys
import yaml

# Add producer directory to path
sys.path.insert(0, 'producer')

def test_config_loading():
    """Test that configuration loading works correctly"""
    print("Testing producer configuration loading...")
    
    # Test 1: Environment variable loading
    print("\n1. Testing environment variable loading...")
    os.environ['VEHICLE_NAME'] = 'test_vehicle'
    os.environ['KAFKA_BROKER'] = 'test-kafka:9092'
    os.environ['LOGGING_LEVEL'] = 'DEBUG'
    
    try:
        from producer.produce import load_config_from_environment
        env_config = load_config_from_environment()
        print(f"Environment config loaded: {env_config['vehicle_name']}")
        print(f"   Kafka broker: {env_config['kafka_broker']}")
        print(f"   Logging level: {env_config['logging_level']}")
    except Exception as e:
        print(f"Environment config failed: {e}")
        return False
    
    # Test 2: YAML file loading
    print("\n2. Testing YAML file loading...")
    try:
        from producer.produce import load_config_from_file
        file_config = load_config_from_file('config/producers/angela.yaml')
        print(f"YAML config loaded: {len(file_config)} parameters")
        if 'mu_anomalies' in file_config:
            print(f"   mu_anomalies: {file_config['mu_anomalies']}")
    except Exception as e:
        print(f"YAML config failed: {e}")
        return False
    
    # Test 3: Configuration merging
    print("\n3. Testing configuration merging...")
    try:
        from producer.produce import merge_configs
        merged_config = merge_configs(env_config, file_config)
        print(f"Configs merged: {len(merged_config)} total parameters")
        print(f"   Vehicle name (env priority): {merged_config['vehicle_name']}")
        print(f"   mu_anomalies (file): {merged_config.get('mu_anomalies', 'not set')}")
    except Exception as e:
        print(f"Config merging failed: {e}")
        return False
    
    # Test 4: Configuration validation
    print("\n4. Testing configuration validation...")
    try:
        from producer.produce import validate_config
        validate_config(merged_config)
        print("Configuration validation passed")
    except Exception as e:
        print(f"Configuration validation failed: {e}")
        return False
    
    print("\nAll tests passed!")
    return True

if __name__ == "__main__":
    success = test_config_loading()
    sys.exit(0 if success else 1)
