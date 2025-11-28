# scripts/generate_outdoor_data.py
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
import random

def main(start_date, days, out_dir, lat=59.4370, lon=24.7536):
    """
    Simple synthetic outdoor weather generator.
    In a real deployment, replace with an Open-Meteo or NOAA API call.
    Default coordinates: Tallinn, Estonia (59.4370°N, 24.7536°E)
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    start = datetime.fromisoformat(start_date)
    end = start + timedelta(days=days)

    current = start
    rows = []
    while current < end:
        hour = current.hour
        # Synthetic Baltic spring weather (Tallinn-like conditions)
        base_temp = 12 + 5 * (1 if 11 <= hour <= 16 else -1)
        temp_c = round(base_temp + random.uniform(-3, 3), 2)
        rel_humidity = round(55 + random.uniform(-15, 15), 1)
        wind_speed = round(max(0, random.gauss(4, 1.5)), 1)

        rows.append(
            {
                "ts_utc": current.isoformat(),
                "latitude": lat,
                "longitude": lon,
                "temp_c": temp_c,
                "rel_humidity_pct": rel_humidity,
                "wind_speed_ms": wind_speed,
                "source": "synthetic_generator_v1",
            }
        )
        current += timedelta(hours=1)

    outfile = out_path / f"outdoor_weather_{start_date}_d{days}.jsonl"
    with outfile.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"Wrote {len(rows)} rows to {outfile}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--out-dir", default="data/raw/outdoor")
    args = parser.parse_args()
    main(args.start_date, args.days, args.out_dir)
