# kafka_consumer.py
from kafka import KafkaConsumer
import json
from clickhouse_driver import Client
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_message(msg, clickhouse_client):
    """Process a message from Kafka and store in ClickHouse"""
    try:
        # Parse the JSON data
        data = msg.value
        
        # Extract the fields we want to store
        record = (
            data['timestamp'],
            data['cell_id'],
            data['production_line'],
            data['measurements']['temperature'],
            data['measurements']['voltage'],
            data['measurements']['internal_resistance'],
            data['measurements']['capacity'], 
            data['measurements']['thickness'],
            data['measurements']['weight'],
            data['process_parameters']['anode_coating_thickness'],
            data['process_parameters']['cathode_coating_thickness'],
            data['process_parameters']['electrolyte_volume'],
            data['process_parameters']['pressing_force'],
            data['quality_checks']['visual_inspection'],
            data['quality_checks']['leakage_test'],
            data['quality_checks']['dimension_check'],
            data['station'],
            data['operator_id'],
            data['batch_id']
        )
        
        # Insert into ClickHouse
        clickhouse_client.execute(
            '''
            INSERT INTO battery_manufacturing.cell_metrics VALUES
            ''', 
            [record]
        )
        
        logger.info(f"Stored data for cell {data['cell_id']}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def run_consumer():
    # Create ClickHouse client
    clickhouse_client = Client(host='localhost')
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        'battery-metrics',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='battery-analytics-group'
    )
    
    # Process messages
    logger.info("Starting consumer, waiting for messages...")
    try:
        for message in consumer:
            process_message(message, clickhouse_client)
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()