# data_generator.py
import time
import random
import json
from datetime import datetime

def generate_battery_cell_data():
    
    """Generate simulated battery cell manufacturing data"""
    
    cell_id = f"CELL-{random.randint(10000, 99999)}"
    
    data = {
        "timestamp": datetime.now().isoformat(),
        "cell_id": cell_id,
        "production_line": random.choice(["line-a", "line-b", "line-c"]),
        "measurements": {
            "temperature": round(random.uniform(20.0, 35.0), 2),  # Celsius
            "voltage": round(random.uniform(3.6, 4.2), 3),  # Volts
            "internal_resistance": round(random.uniform(0.01, 0.05), 4),  # Ohms
            "capacity": round(random.uniform(4800, 5200), 0),  # mAh
            "thickness": round(random.uniform(6.8, 7.2), 2),  # mm
            "weight": round(random.uniform(45.0, 49.0), 2),  # grams
        },
        "process_parameters": {
            "anode_coating_thickness": round(random.uniform(95, 105), 1),  # microns
            "cathode_coating_thickness": round(random.uniform(140, 150), 1),  # microns
            "electrolyte_volume": round(random.uniform(4.8, 5.2), 2),  # mL
            "pressing_force": round(random.uniform(980, 1020), 0),  # kN
        },
        "quality_checks": {
            "visual_inspection": random.choice(["pass", "pass", "pass", "fail"]),
            "leakage_test": random.choice(["pass", "pass", "pass", "fail"]),
            "dimension_check": random.choice(["pass", "pass", "pass", "fail"]),
        },
        "station": random.choice(["coating", "assembly", "filling", "testing"]),
        "operator_id": f"OP-{random.randint(100, 999)}",
        "batch_id": f"BTC-{random.randint(1000, 9999)}"
    }
    
    # Add some anomalies occasionally
    if random.random() < 0.05:  # 5% chance of temperature anomaly
        data["measurements"]["temperature"] = round(random.uniform(40.0, 50.0), 2)
    
    return data

if __name__ == "__main__":
    # Generate sample data
    for _ in range(10):
        data = generate_battery_cell_data()
        print(json.dumps(data, indent=2))
        time.sleep(0.5)