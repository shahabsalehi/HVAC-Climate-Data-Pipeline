"""
HVAC and Climate Data Generator

Generates synthetic HVAC sensor data and climate data for testing and development.
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict


class HVACDataGenerator:
    """Generator for synthetic HVAC sensor data"""
    
    def __init__(self, base_path: str = "data/raw"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def generate_sensor_reading(
        self,
        sensor_id: str,
        timestamp: datetime,
        base_temp: float = 22.0,
        base_humidity: float = 45.0,
    ) -> Dict:
        """
        Generate a single HVAC sensor reading with realistic patterns
        """
        return {
            "sensor_id": sensor_id,
            "timestamp": timestamp.isoformat(),
            "temperature_celsius": round(base_temp + random.uniform(-2, 2), 2),
            "humidity_percent": round(base_humidity + random.uniform(-5, 5), 2),
            "pressure_kpa": round(101.3 + random.uniform(-0.5, 0.5), 2),
            "co2_ppm": random.randint(400, 1000),
            "hvac_mode": random.choice(["heating", "cooling", "fan", "auto"]),
            "setpoint_celsius": round(base_temp + random.uniform(-1, 1), 1),
        }
    
    def generate_batch(
        self,
        sensor_ids: List[str],
        start_time: datetime,
        hours: int = 24,
        interval_minutes: int = 15,
    ) -> List[Dict]:
        """
        Generate a batch of sensor readings for multiple sensors over time
        """
        readings = []
        current_time = start_time
        end_time = start_time + timedelta(hours=hours)
        
        while current_time < end_time:
            for sensor_id in sensor_ids:
                reading = self.generate_sensor_reading(sensor_id, current_time)
                readings.append(reading)
            current_time += timedelta(minutes=interval_minutes)
        
        return readings
    
    def save_to_json(self, data: List[Dict], filename: str):
        """Save generated data to JSON file"""
        output_path = self.base_path / filename
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Saved {len(data)} records to {output_path}")


class ClimateDataGenerator:
    """Generator for synthetic climate data"""
    
    def __init__(self, base_path: str = "data/raw"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def generate_weather_reading(
        self,
        location: str,
        timestamp: datetime,
        base_temp: float = 15.0,
    ) -> Dict:
        """
        Generate a single weather reading with realistic patterns
        """
        return {
            "location": location,
            "timestamp": timestamp.isoformat(),
            "outdoor_temp_celsius": round(base_temp + random.uniform(-5, 5), 2),
            "outdoor_humidity_percent": round(60 + random.uniform(-20, 20), 2),
            "wind_speed_kmh": round(random.uniform(0, 30), 2),
            "wind_direction_degrees": random.randint(0, 359),
            "solar_radiation_wm2": max(0, round(random.uniform(0, 1000), 2)),
            "precipitation_mm": round(max(0, random.gauss(0, 2)), 2),
        }
    
    def generate_batch(
        self,
        locations: List[str],
        start_time: datetime,
        hours: int = 24,
        interval_minutes: int = 60,
    ) -> List[Dict]:
        """
        Generate a batch of weather readings for multiple locations over time
        """
        readings = []
        current_time = start_time
        end_time = start_time + timedelta(hours=hours)
        
        while current_time < end_time:
            for location in locations:
                reading = self.generate_weather_reading(location, current_time)
                readings.append(reading)
            current_time += timedelta(minutes=interval_minutes)
        
        return readings
    
    def save_to_json(self, data: List[Dict], filename: str):
        """Save generated data to JSON file"""
        output_path = self.base_path / filename
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Saved {len(data)} records to {output_path}")


def main():
    """
    Main function to generate sample HVAC and climate data
    """
    print("Generating HVAC and Climate Data...")
    
    # Generate HVAC sensor data
    hvac_generator = HVACDataGenerator()
    sensor_ids = ["SENSOR_001", "SENSOR_002", "SENSOR_003"]
    start_time = datetime.now() - timedelta(days=1)
    
    hvac_data = hvac_generator.generate_batch(
        sensor_ids=sensor_ids,
        start_time=start_time,
        hours=24,
        interval_minutes=15,
    )
    hvac_generator.save_to_json(hvac_data, "hvac_data_sample.json")
    
    # Generate climate data
    climate_generator = ClimateDataGenerator()
    locations = ["Building_A", "Building_B", "Building_C"]
    
    climate_data = climate_generator.generate_batch(
        locations=locations,
        start_time=start_time,
        hours=24,
        interval_minutes=60,
    )
    climate_generator.save_to_json(climate_data, "climate_data_sample.json")
    
    print("Data generation complete!")


if __name__ == "__main__":
    main()
