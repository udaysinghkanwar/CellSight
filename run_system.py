# run_system.py
import subprocess
import time
import os
import signal
import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global flag to signal shutdown
shutdown_flag = False

def run_clickhouse_setup():
    logger.info("Setting up ClickHouse database...")
    subprocess.run(["python", "clickhouse_setup.py"])

def run_kafka_producer():
    logger.info("Starting Kafka producer...")
    producer_process = subprocess.Popen(["python", "kafka_producer.py"])
    return producer_process

def run_kafka_consumer():
    logger.info("Starting Kafka consumer...")
    consumer_process = subprocess.Popen(["python", "kafka_consumer.py"])
    return consumer_process

def run_api_server():
    logger.info("Starting API server...")
    api_process = subprocess.Popen(["python", "api.py"])
    return api_process

def signal_handler(sig, frame):
    global shutdown_flag
    logger.info("Shutdown signal received, stopping processes...")
    shutdown_flag = True

def main():
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    """logger.info("Starting docker containers")
        subprocess.run(["docker-compose", "up", "-d"])
    """
    
    # First, ensure the infrastructure is running
    logger.info("Checking if Docker containers are running...")
    subprocess.run(["docker-compose", "ps"])
    
    # Set up ClickHouse
    run_clickhouse_setup()
    time.sleep(2)
    
    # Start processes
    processes = []
    
    # Start consumer first to ensure it's ready to receive messages
    consumer_process = run_kafka_consumer()
    processes.append(consumer_process)
    time.sleep(3)
    
    # Start producer
    producer_process = run_kafka_producer()
    processes.append(producer_process)
    
    # Start API server
    api_process = run_api_server()
    processes.append(api_process)
    
    logger.info("All processes started!")
    logger.info("Access the API docs at http://localhost:8000/docs")
    logger.info("Press Ctrl+C to stop all processes")
    
    try:
        # Keep main thread alive
        while not shutdown_flag:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
    finally:
        # Terminate all processes
        for process in processes:
            process.terminate()
            process.wait()
        
        logger.info("All processes stopped")

if __name__ == "__main__":
    main()