# clickhouse_setup.py
from clickhouse_driver import Client

def setup_clickhouse():
    # Connect to ClickHouse
    client = Client(
        host='localhost',
        user='default',
        password=''  # Empty password to match ClickHouse configuration
    )
    
    # Create database if not exists
    client.execute('CREATE DATABASE IF NOT EXISTS battery_manufacturing')
    
    # Create table for battery cell data
    client.execute('''
    CREATE TABLE IF NOT EXISTS battery_manufacturing.cell_metrics (
        timestamp DateTime,
        cell_id String,
        production_line String,
        temperature Float32,
        voltage Float32,
        internal_resistance Float32,
        capacity Int32,
        thickness Float32,
        weight Float32,
        anode_thickness Float32,
        cathode_thickness Float32,
        electrolyte_volume Float32,
        pressing_force Int32,
        visual_inspection String,
        leakage_test String,
        dimension_check String,
        station String,
        operator_id String,
        batch_id String
    ) ENGINE = MergeTree()
    ORDER BY (timestamp, production_line, cell_id)
    PARTITION BY toYYYYMM(timestamp)
    ''')
    
    print("ClickHouse database and table created successfully!")

if __name__ == "__main__":
    setup_clickhouse()