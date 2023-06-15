import pandas as pd
import duckdb

def ingest_raw_data(filepath):
    df = pd.read_csv(filepath)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def transform_to_silver(df):
    df = df.dropna()
    df = df[(df['indoor_temp_c'] > 10) & (df['indoor_temp_c'] < 35)]
    df = df[(df['humidity_pct'] > 20) & (df['humidity_pct'] < 80)]
    return df

def load_to_duckdb(df, db_path, table_name):
    conn = duckdb.connect(db_path)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    conn.close()

if __name__ == "__main__":
    df = ingest_raw_data("data/raw/hvac_climate.csv")
    df = transform_to_silver(df)
    load_to_duckdb(df, "data/hvac.duckdb", "hvac_silver")
    print(f"Processed {len(df)} records")
