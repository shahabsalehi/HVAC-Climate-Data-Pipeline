"""
Data Ingestion and Transformation Module

Handles data ingestion from various sources and transformation through
the medallion architecture (bronze -> silver -> gold).
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import glob

# Configurable thresholds for comfort flags
OVERCOOLING_INDOOR_THRESHOLD = 21  # Celsius
OVERCOOLING_OUTDOOR_THRESHOLD = 25  # Celsius
STALE_AIR_CO2_THRESHOLD = 1000  # ppm


def load_raw_indoor(path_pattern: str) -> pd.DataFrame:
    """
    Load raw indoor JSONL files into a DataFrame
    
    Args:
        path_pattern: Glob pattern for indoor JSONL files
    
    Returns:
        DataFrame with indoor sensor events
    """
    print(f"Loading raw indoor data from: {path_pattern}")
    files = sorted(glob.glob(path_pattern))
    
    if not files:
        print(f"Warning: No files found matching {path_pattern}")
        return pd.DataFrame()
    
    all_records = []
    for file in files:
        with open(file, 'r') as f:
            for line in f:
                try:
                    all_records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping malformed JSON line in {file}: {e}")
                    continue
    
    df = pd.DataFrame(all_records)
    print(f"Loaded {len(df)} indoor sensor events from {len(files)} file(s)")
    return df


def load_raw_outdoor(path_pattern: str) -> pd.DataFrame:
    """
    Load raw outdoor JSONL files into a DataFrame
    
    Args:
        path_pattern: Glob pattern for outdoor JSONL files
    
    Returns:
        DataFrame with outdoor weather events
    """
    print(f"Loading raw outdoor data from: {path_pattern}")
    files = sorted(glob.glob(path_pattern))
    
    if not files:
        print(f"Warning: No files found matching {path_pattern}")
        return pd.DataFrame()
    
    all_records = []
    for file in files:
        with open(file, 'r') as f:
            for line in f:
                try:
                    all_records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping malformed JSON line in {file}: {e}")
                    continue
    
    df = pd.DataFrame(all_records)
    print(f"Loaded {len(df)} outdoor weather events from {len(files)} file(s)")
    return df


def transform_to_bronze(df: pd.DataFrame, data_type: str = "indoor") -> pd.DataFrame:
    """
    Transform raw data to bronze layer with type casting and basic cleaning
    
    Args:
        df: Raw DataFrame
        data_type: Type of data ("indoor" or "outdoor")
    
    Returns:
        Bronze-layer DataFrame with proper types
    """
    print(f"Transforming {data_type} data to bronze layer...")
    
    if df.empty:
        return df
    
    bronze_df = df.copy()
    
    # Parse timestamps
    bronze_df['ts_utc'] = pd.to_datetime(bronze_df['ts_utc'])
    
    # Add metadata
    bronze_df['_ingestion_timestamp'] = datetime.now()
    
    # Type-specific transformations
    if data_type == "indoor":
        # Ensure proper types for indoor data
        bronze_df['event_id'] = bronze_df['event_id'].astype(int)
        bronze_df['value'] = bronze_df['value'].astype(float)
    elif data_type == "outdoor":
        # Ensure proper types for outdoor data
        bronze_df['temp_c'] = bronze_df['temp_c'].astype(float)
        bronze_df['rel_humidity_pct'] = bronze_df['rel_humidity_pct'].astype(float)
        bronze_df['wind_speed_ms'] = bronze_df['wind_speed_ms'].astype(float)
    
    print(f"Bronze transformation complete: {len(bronze_df)} records")
    return bronze_df


def join_and_compute_comfort(indoor_df: pd.DataFrame, outdoor_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join indoor and outdoor data, resample, and compute comfort metrics
    
    Args:
        indoor_df: Bronze indoor DataFrame
        outdoor_df: Bronze outdoor DataFrame
    
    Returns:
        Silver-layer comfort facts DataFrame
    """
    print("Joining indoor and outdoor data and computing comfort metrics...")
    
    if indoor_df.empty or outdoor_df.empty:
        print("Warning: Empty input data")
        return pd.DataFrame()
    
    # Pivot indoor data to get one row per timestamp + room with all sensor values
    indoor_pivot = indoor_df.pivot_table(
        index=['ts_utc', 'building_id', 'room_id'],
        columns='sensor_type',
        values='value',
        aggfunc='mean'
    ).reset_index()
    
    # Rename columns for clarity
    indoor_pivot.columns.name = None
    indoor_pivot = indoor_pivot.rename(columns={
        'temp': 'indoor_temp_c',
        'humidity': 'indoor_rel_humidity_pct',
        'co2': 'indoor_co2_ppm',
        'voc': 'indoor_voc_ppb'
    })
    
    # Set timestamp as index for resampling
    indoor_pivot['ts_utc'] = pd.to_datetime(indoor_pivot['ts_utc'])
    outdoor_df['ts_utc'] = pd.to_datetime(outdoor_df['ts_utc'])
    
    # Resample outdoor data to 5-minute resolution (forward fill for up to 15 minutes)
    outdoor_resampled = outdoor_df.set_index('ts_utc').resample('5min').asfreq()
    # Track which rows are actual, forward-filled, or missing
    outdoor_resampled['data_quality'] = 'actual'
    mask_missing = outdoor_resampled['temp_c'].isna() | outdoor_resampled['rel_humidity_pct'].isna()
    outdoor_resampled.loc[mask_missing, 'data_quality'] = 'missing'
    outdoor_resampled = outdoor_resampled.ffill(limit=3)
    # After ffill, mark forward-filled rows
    mask_ffilled = (outdoor_resampled['data_quality'] == 'missing') & (
        outdoor_resampled['temp_c'].notna() | outdoor_resampled['rel_humidity_pct'].notna()
    )
    outdoor_resampled.loc[mask_ffilled, 'data_quality'] = 'ffilled'
    outdoor_resampled = outdoor_resampled.reset_index()
    # Log warning if any rows remain missing after ffill
    if (outdoor_resampled['data_quality'] == 'missing').any():
        print("Warning: Some outdoor data is missing for >15 minutes and will propagate as NaN.")
    
    # Merge indoor with outdoor data, including data_quality
    comfort_df = pd.merge_asof(
        indoor_pivot.sort_values('ts_utc'),
        outdoor_resampled[['ts_utc', 'temp_c', 'rel_humidity_pct', 'data_quality']].sort_values('ts_utc'),
        on='ts_utc',
        direction='nearest',
        tolerance=pd.Timedelta('15min')
    )
    
    # Rename outdoor columns
    comfort_df = comfort_df.rename(columns={
        'temp_c': 'outdoor_temp_c',
        'rel_humidity_pct': 'outdoor_rel_humidity_pct'
    })
    
    # Compute flags
    # Overcooled: indoor_temp_c < OVERCOOLING_INDOOR_THRESHOLD AND outdoor_temp_c > OVERCOOLING_OUTDOOR_THRESHOLD
    comfort_df['overcooled_flag'] = (
        (comfort_df['indoor_temp_c'] < OVERCOOLING_INDOOR_THRESHOLD) & 
        (comfort_df['outdoor_temp_c'] > OVERCOOLING_OUTDOOR_THRESHOLD)
    ).fillna(False)
    
    # Stale air: indoor_co2_ppm > STALE_AIR_CO2_THRESHOLD
    comfort_df['stale_air_flag'] = (
        comfort_df['indoor_co2_ppm'] > STALE_AIR_CO2_THRESHOLD
    ).fillna(False)
    
    # Add date column for partitioning
    comfort_df['date'] = comfort_df['ts_utc'].dt.date
    
    print(f"Comfort facts computed: {len(comfort_df)} records")
    return comfort_df


def compute_daily_metrics(comfort_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily aggregated metrics for gold layer
    
    Args:
        comfort_df: Silver-layer comfort facts DataFrame
    
    Returns:
        Gold-layer daily metrics DataFrame
    """
    print("Computing daily metrics for gold layer...")
    
    if comfort_df.empty:
        return pd.DataFrame()
    
    # Group by date, building, and room
    daily_metrics = comfort_df.groupby(['date', 'building_id', 'room_id']).agg(
        n_readings=('ts_utc', 'count'),
        n_overcooled=('overcooled_flag', 'sum'),
        n_stale_air=('stale_air_flag', 'sum'),
        avg_indoor_temp=('indoor_temp_c', 'mean'),
        avg_indoor_humidity=('indoor_rel_humidity_pct', 'mean'),
        avg_indoor_co2=('indoor_co2_ppm', 'mean'),
        avg_outdoor_temp=('outdoor_temp_c', 'mean')
    ).reset_index()
    
    # Calculate percentages
    daily_metrics['pct_time_overcooled'] = (
        daily_metrics['n_overcooled'] / daily_metrics['n_readings'] * 100
    ).round(2)
    
    daily_metrics['pct_time_stale_air'] = (
        daily_metrics['n_stale_air'] / daily_metrics['n_readings'] * 100
    ).round(2)
    
    print(f"Daily metrics computed: {len(daily_metrics)} records")
    return daily_metrics


def write_partitioned_parquet(df: pd.DataFrame, base_path: str, layer: str, 
                               table_name: str, partition_cols: List[str] = None):
    """
    Write DataFrame as partitioned Parquet files
    
    Args:
        df: DataFrame to write
        base_path: Base data directory
        layer: Data layer (bronze/silver/gold)
        table_name: Table name
        partition_cols: Columns to partition by
    """
    if df.empty:
        print(f"Warning: Empty DataFrame, skipping write to {layer}/{table_name}")
        return
    
    # Create directory structure
    output_path = Path(base_path) / layer / table_name
    output_path.mkdir(parents=True, exist_ok=True)
    
    if partition_cols:
        # Write partitioned by date components
        if 'date' in df.columns and 'date' in partition_cols:
            # Ensure date is datetime for dt accessor
            df['date'] = pd.to_datetime(df['date'])
            
            # Add year, month, day columns for partitioning
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day'] = df['date'].dt.day
            
            # Group by partition columns and write
            for (year, month, day), group in df.groupby(['year', 'month', 'day']):
                partition_path = output_path / f"year={year}" / f"month={month:02d}" / f"day={day:02d}"
                partition_path.mkdir(parents=True, exist_ok=True)
                
                # Drop partition columns from data
                group_data = group.drop(columns=['year', 'month', 'day'])
                
                filename = partition_path / f"part-{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
                group_data.to_parquet(filename, index=False)
                print(f"Wrote {len(group)} records to {filename}")
        else:
            # Simple partition by specified columns
            df.to_parquet(output_path, partition_cols=partition_cols, index=False)
            print(f"Wrote {len(df)} records to {output_path}")
    else:
        # No partitioning, write single file
        filename = output_path / f"{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
        df.to_parquet(filename, index=False)
        print(f"Wrote {len(df)} records to {filename}")


def run_pipeline_for_day(target_date: str, base_path: str = "data"):
    """
    Run the complete pipeline for a specific day
    
    Args:
        target_date: Date in YYYY-MM-DD format
        base_path: Base data directory path
    """
    print(f"\n{'='*60}")
    print(f"Running HVAC Climate Pipeline for {target_date}")
    print(f"{'='*60}\n")
    
    # 1. Load raw data
    indoor_pattern = f"{base_path}/raw/indoor/*{target_date}*.jsonl"
    outdoor_pattern = f"{base_path}/raw/outdoor/*{target_date}*.jsonl"
    
    raw_indoor = load_raw_indoor(indoor_pattern)
    raw_outdoor = load_raw_outdoor(outdoor_pattern)
    
    if raw_indoor.empty or raw_outdoor.empty:
        print("Error: Missing required data files")
        return
    
    # 2. Transform to bronze
    bronze_indoor = transform_to_bronze(raw_indoor, "indoor")
    bronze_outdoor = transform_to_bronze(raw_outdoor, "outdoor")
    
    # Write bronze layer
    write_partitioned_parquet(bronze_indoor, base_path, "bronze", "indoor_events", 
                             partition_cols=['date'] if 'date' in bronze_indoor.columns else None)
    write_partitioned_parquet(bronze_outdoor, base_path, "bronze", "outdoor_weather",
                             partition_cols=['date'] if 'date' in bronze_outdoor.columns else None)
    
    # 3. Transform to silver (comfort facts)
    comfort_facts = join_and_compute_comfort(bronze_indoor, bronze_outdoor)
    
    if not comfort_facts.empty:
        write_partitioned_parquet(comfort_facts, base_path, "silver", "hvac_comfort_facts",
                                 partition_cols=['date'])
        
        # 4. Transform to gold (daily metrics)
        daily_metrics = compute_daily_metrics(comfort_facts)
        
        if not daily_metrics.empty:
            write_partitioned_parquet(daily_metrics, base_path, "gold", "daily_comfort_metrics",
                                     partition_cols=['date'])
    
    print(f"\n{'='*60}")
    print("Pipeline execution complete!")
    print(f"{'='*60}\n")


def main():
    """
    Main function to run the ingestion and transformation pipeline
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="HVAC Climate Data Pipeline")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--base-path", default="data", help="Base data directory")
    
    args = parser.parse_args()
    
    run_pipeline_for_day(args.date, args.base_path)


if __name__ == "__main__":
    main()
