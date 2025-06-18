#!/usr/bin/env python3
"""
Validate exported JSON against schema for frontend consumption.

Usage:
    python scripts/validate_json.py [json_path]
    
If no path given, validates artifacts/json/hvac_climate_telemetry.json (canonical)
"""

import json
import sys
from datetime import datetime
from pathlib import Path


def validate_iso8601(value: str) -> bool:
    """Check if string is valid ISO 8601 timestamp."""
    try:
        if "+" in value or value.endswith("Z"):
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            datetime.fromisoformat(value)
        return True
    except ValueError:
        return False


def validate_hvac_climate_telemetry(data: dict) -> list[str]:
    """Validate canonical hvac_climate_telemetry.json schema."""
    errors = []
    
    # Required top-level fields
    required_top = ["pipeline", "generated_at", "site", "current_conditions"]
    for field in required_top:
        if field not in data:
            errors.append(f"Missing required field: {field}")
    
    # Validate generated_at timestamp
    if "generated_at" in data:
        if not validate_iso8601(data["generated_at"]):
            errors.append("Field 'generated_at' is not a valid ISO 8601 timestamp")
    
    # Validate site object
    if "site" in data:
        site = data["site"]
        site_fields = ["name", "location", "building_type"]
        for field in site_fields:
            if field not in site:
                errors.append(f"Missing site.{field}")
    
    # Validate current_conditions object
    if "current_conditions" in data:
        conditions = data["current_conditions"]
        condition_fields = ["indoor_temp_c", "outdoor_temp_c"]
        for field in condition_fields:
            if field not in conditions:
                errors.append(f"Missing current_conditions.{field}")
        
        # Range checks
        if "indoor_temp_c" in conditions:
            temp = conditions["indoor_temp_c"]
            if not isinstance(temp, (int, float)) or temp < -50 or temp > 60:
                errors.append(f"current_conditions.indoor_temp_c out of range: {temp}")
        
        if "outdoor_temp_c" in conditions:
            temp = conditions["outdoor_temp_c"]
            if not isinstance(temp, (int, float)) or temp < -50 or temp > 60:
                errors.append(f"current_conditions.outdoor_temp_c out of range: {temp}")
    
    # Validate hourly_data array if present
    if "hourly_data" in data:
        if not isinstance(data["hourly_data"], list):
            errors.append("hourly_data must be an array")
        elif len(data["hourly_data"]) > 0:
            sample = data["hourly_data"][0]
            if "hour" not in sample:
                errors.append("hourly_data items must have 'hour' field")
    
    return errors


def main():
    json_path = sys.argv[1] if len(sys.argv) > 1 else "artifacts/json/hvac_climate_telemetry.json"
    path = Path(json_path)
    
    if not path.exists():
        print(f"✗ File not found: {path}")
        sys.exit(1)
    
    try:
        with open(path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON: {e}")
        sys.exit(1)
    
    errors = validate_hvac_climate_telemetry(data)
    
    if errors:
        print(f"✗ Validation failed for {path}:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)
    else:
        print(f"✓ Validation passed: {path}")
        print(f"  pipeline: {data.get('pipeline', 'N/A')}")
        print(f"  generated_at: {data.get('generated_at', 'N/A')}")
        if "site" in data:
            print(f"  site: {data['site'].get('name', 'N/A')}")
        if "current_conditions" in data:
            cc = data["current_conditions"]
            print(f"  indoor_temp_c: {cc.get('indoor_temp_c', 'N/A')}°C")
            print(f"  outdoor_temp_c: {cc.get('outdoor_temp_c', 'N/A')}°C")
        if "hourly_data" in data:
            print(f"  hourly_data: {len(data['hourly_data'])} records")
        sys.exit(0)


if __name__ == "__main__":
    main()
