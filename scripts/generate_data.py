import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_hvac_climate_data(num_records=1000):
    start_date = datetime(2023, 1, 1)
    data = []
    for i in range(num_records):
        timestamp = start_date + timedelta(hours=i)
        outdoor_temp = 10 + 15 * np.sin(2 * np.pi * i / 8760) + random.uniform(-5, 5)
        indoor_temp = 21 + random.uniform(-2, 2)
        humidity = 45 + random.uniform(-10, 10)
        hvac_power = max(0, (abs(outdoor_temp - indoor_temp) * 100) + random.uniform(-50, 50))
        data.append({
            "timestamp": timestamp.isoformat(),
            "outdoor_temp_c": round(outdoor_temp, 2),
            "indoor_temp_c": round(indoor_temp, 2),
            "humidity_pct": round(humidity, 2),
            "hvac_power_kw": round(hvac_power / 1000, 3)
        })
    return pd.DataFrame(data)

if __name__ == "__main__":
    df = generate_hvac_climate_data()
    df.to_csv("data/raw/hvac_climate.csv", index=False)
    print(f"Generated {len(df)} records")
