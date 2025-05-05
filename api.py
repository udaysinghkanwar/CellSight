# api.py
from fastapi import FastAPI, Query
from clickhouse_driver import Client
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Optional

app = FastAPI(title="Battery Manufacturing Analytics API")

# Create ClickHouse client
client = Client(host='localhost')

@app.get("/metrics/summary")
async def get_summary_metrics(
    hours: int = Query(1, description="Hours of data to include")
):
    """Get summary metrics for the last N hours"""
    cutoff_time = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
    
    # Get aggregate metrics
    query = f"""
    SELECT 
        production_line,
        count() as cell_count,
        avg(temperature) as avg_temperature,
        avg(voltage) as avg_voltage,
        avg(capacity) as avg_capacity,
        countIf(visual_inspection = 'fail') as visual_fails,
        countIf(leakage_test = 'fail') as leakage_fails,
        countIf(dimension_check = 'fail') as dimension_fails
    FROM battery_manufacturing.cell_metrics
    WHERE timestamp >= '{cutoff_time}'
    GROUP BY production_line
    """
    
    result = client.execute(query)
    
    # Convert to dictionaries for JSON response
    columns = ['production_line', 'cell_count', 'avg_temperature', 'avg_voltage', 
               'avg_capacity', 'visual_fails', 'leakage_fails', 'dimension_fails']
    
    return [dict(zip(columns, row)) for row in result]

@app.get("/metrics/anomalies")
async def get_anomalies(
    hours: int = Query(1, description="Hours of data to check")
):
    """Detect anomalies in manufacturing data"""
    cutoff_time = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
    
    # Find cells with high temperature
    temp_query = f"""
    SELECT 
        timestamp, cell_id, production_line, temperature, station
    FROM battery_manufacturing.cell_metrics
    WHERE timestamp >= '{cutoff_time}' AND temperature > 38.0
    ORDER BY temperature DESC
    LIMIT 100
    """
    
    temp_anomalies = client.execute(temp_query)
    
    # Find cells with capacity issues
    capacity_query = f"""
    SELECT 
        timestamp, cell_id, production_line, capacity, station
    FROM battery_manufacturing.cell_metrics
    WHERE timestamp >= '{cutoff_time}' 
    AND (capacity < 4850 OR capacity > 5150)
    ORDER BY ABS(capacity - 5000) DESC
    LIMIT 100
    """
    
    capacity_anomalies = client.execute(capacity_query)
    
    return {
        "temperature_anomalies": [
            {"timestamp": ts, "cell_id": cid, "line": line, "temperature": temp, "station": st}
            for ts, cid, line, temp, st in temp_anomalies
        ],
        "capacity_anomalies": [
            {"timestamp": ts, "cell_id": cid, "line": line, "capacity": cap, "station": st}
            for ts, cid, line, cap, st in capacity_anomalies
        ]
    }

@app.get("/metrics/production-trend")
async def get_production_trend(
    hours: int = Query(24, description="Hours of data to include"),
    interval_minutes: int = Query(60, description="Group by interval in minutes")
):
    """Get production trend data grouped by time interval"""
    cutoff_time = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
    
    query = f"""
    SELECT 
        toStartOfInterval(timestamp, INTERVAL {interval_minutes} MINUTE) as time_bucket,
        production_line,
        count() as cell_count,
        countIf(visual_inspection = 'pass' AND leakage_test = 'pass' AND dimension_check = 'pass') as passed_cells
    FROM battery_manufacturing.cell_metrics
    WHERE timestamp >= '{cutoff_time}'
    GROUP BY time_bucket, production_line
    ORDER BY time_bucket, production_line
    """
    
    result = client.execute(query)
    
    # Format for JSON response
    columns = ['time_bucket', 'production_line', 'cell_count', 'passed_cells']
    return [dict(zip(columns, row)) for row in result]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)