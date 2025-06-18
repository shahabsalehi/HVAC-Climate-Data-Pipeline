# scripts/generate_indoor_data.py
import argparse
import json
from datetime import datetime, timedelta
import random
from pathlib import Path

ROOMS = ["office_1", "office_2", "meeting_1", "lab_1"]
SENSORS_PER_ROOM = ["temp", "humidity", "co2", "voc"]

def simulate_value(sensor_type, hour, base_offset):
    # crude daily pattern + noise
    if sensor_type == "temp":
        # 21-25 C typical indoor, cooler at night
        baseline = 23 + 2 * (1 if 8 <= hour <= 18 else -1)
        return round(baseline + base_offset + random.uniform(-0.8, 0.8), 2)
    if sensor_type == "humidity":
        baseline = 40 + 5 * (1 if 6 <= hour <= 12 else -1)
        return round(baseline + base_offset + random.uniform(-4, 4), 1)
    if sensor_type == "co2":
        baseline = 450 + (300 if 9 <= hour <= 17 else 0)  # occupied hours
        return int(baseline + base_offset * 20 + random.uniform(-50, 50))
    if sensor_type == "voc":
        baseline = 150 + (80 if 9 <= hour <= 17 else 0)
        return int(max(50, baseline + base_offset * 10 + random.uniform(-20, 20)))
    raise ValueError(f"Unknown sensor_type: '{sensor_type}'. Expected one of: temp, humidity, co2, voc")

def main(start_date, days, out_dir, freq_minutes=5):
    if freq_minutes <= 0:
        raise ValueError(f"freq_minutes must be positive, got {freq_minutes}")
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    start_dt = datetime.fromisoformat(start_date)
    end_dt = start_dt + timedelta(days=days)

    current = start_dt
    events = []
    event_id = 0
    while current < end_dt:
        hour = current.hour
        for room in ROOMS:
            base_offset = random.uniform(-1, 1)
            for sensor in SENSORS_PER_ROOM:
                event_id += 1
                events.append(
                    {
                        "event_id": event_id,
                        "ts_utc": current.isoformat(),
                        "building_id": "building_A",
                        "room_id": room,
                        "sensor_type": sensor,
                        "value": simulate_value(sensor, hour, base_offset),
                        "unit": {
                            "temp": "C",
                            "humidity": "%",
                            "co2": "ppm",
                            "voc": "ppb",
                        }[sensor],
                    }
                )
        current += timedelta(minutes=freq_minutes)

    outfile = out_path / f"indoor_events_{start_date}_d{days}.jsonl"
    with outfile.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"Wrote {len(events)} events to {outfile}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--out-dir", default="data/raw/indoor")
    args = parser.parse_args()
    main(args.start_date, args.days, args.out_dir)
