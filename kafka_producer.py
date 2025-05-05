# kafka_producer.py
from kafka import KafkaProducer
import json
import time
import logging
from data_generator import generate_battery_cell_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def get_partition(key, all_partitions, available):
    """Simple partitioning by production line"""
    if key == b'line-a':
        return 0
    elif key == b'line-b':
        return 1
    else:
        return 2

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=json_serializer,
            partitioner=get_partition
        )
        return producer
    except Exception as e:
        logger.error(f"Error creating Kafka producer: {e}")
        return None

def produce_data(producer, topic_name="battery-metrics-v2", count=100, delay=0.05):
    """Generate and send battery manufacturing data to Kafka"""
    if not producer:
        return
    
    try:
        for _ in range(count):
            data = generate_battery_cell_data()
            key = data['production_line'].encode('utf-8')
            
            # Send data to Kafka
            future = producer.send(topic_name, key=key, value=data)
            producer.flush()
            
            # Get metadata about the record
            record_metadata = future.get(timeout=10)
            logger.info(f"Sent record: Topic={record_metadata.topic}, "
                         f"Partition={record_metadata.partition}, "
                         f"Offset={record_metadata.offset}")
            
            time.sleep(delay)
    except Exception as e:
        logger.error(f"Error producing data: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    producer = create_producer()
    if producer:
        produce_data(producer, count=1000, delay=0.01)  # Generate 1000 records