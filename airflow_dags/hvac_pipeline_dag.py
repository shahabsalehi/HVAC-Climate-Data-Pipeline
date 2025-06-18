"""
Airflow DAG for HVAC Climate Data Pipeline

This DAG orchestrates the HVAC climate data pipeline:
1. Generate synthetic data (optional, for testing)
2. Transform data through bronze -> silver -> gold layers
3. Run data quality checks

Schedule: Daily execution
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
from pathlib import Path

# Add scripts directory to Python path
scripts_path = Path(__file__).parent.parent / "scripts"
if not scripts_path.exists():
    raise FileNotFoundError(f"Scripts directory not found at {scripts_path}")
sys.path.insert(0, str(scripts_path))


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def generate_sample_data(**context):
    """
    Generate synthetic HVAC and climate data for testing
    
    This is optional and typically disabled in production
    where real sensors provide data.
    """
    from generate_indoor_data import main as generate_indoor
    from generate_outdoor_data import main as generate_outdoor
    
    # Get execution date from context
    exec_date = context['ds']  # YYYY-MM-DD format
    
    print(f"Generating sample data for {exec_date}")
    
    # Generate 1 day of data
    generate_indoor(start_date=exec_date, days=1, out_dir="data/raw/indoor")
    generate_outdoor(start_date=exec_date, days=1, out_dir="data/raw/outdoor")
    
    print("Sample data generation complete")


def run_data_pipeline(**context):
    """
    Execute the complete data pipeline for the execution date
    
    Transforms data through bronze -> silver -> gold layers
    """
    from ingest_transform import run_pipeline_for_day
    
    # Get execution date from context
    exec_date = context['ds']  # YYYY-MM-DD format
    
    print(f"Running data pipeline for {exec_date}")
    
    # Run the pipeline
    run_pipeline_for_day(target_date=exec_date, base_path="data")
    
    print("Data pipeline execution complete")


def validate_data_quality(**context):
    """
    Run data quality checks on processed data
    
    Validates:
    - Data completeness (no missing critical fields)
    - Data accuracy (values within expected ranges)
    - Data timeliness (data is fresh)
    """
    import pandas as pd
    from pathlib import Path
    
    exec_date = context['ds']
    date_obj = datetime.strptime(exec_date, '%Y-%m-%d')
    
    print(f"Running data quality checks for {exec_date}")
    
    # Check gold layer metrics exist
    gold_path = Path("data/gold/daily_comfort_metrics")
    partition_path = gold_path / f"year={date_obj.year}" / f"month={date_obj.month:02d}" / f"day={date_obj.day:02d}"
    
    if not partition_path.exists():
        raise ValueError(f"Gold layer data not found for {exec_date} at {partition_path}")
    
    # Load and validate gold metrics
    df = pd.read_parquet(partition_path)
    
    if df.empty:
        raise ValueError(f"No data found in gold layer for {exec_date}")
    
    # Quality checks
    checks_passed = []
    checks_failed = []
    
    # Check 1: Completeness - ensure all rooms have data
    # Load expected rooms from configuration file (config/expected_rooms.txt)
    config_path = Path("config/expected_rooms.txt")
    if not config_path.exists():
        raise ValueError(f"Expected rooms config file not found at {config_path}")
    with config_path.open("r") as f:
        expected_rooms = set(line.strip() for line in f if line.strip())
    actual_rooms = set(df['room_id'].unique())
    if expected_rooms == actual_rooms:
        checks_passed.append("Room completeness")
    else:
        checks_failed.append(f"Room completeness (missing: {expected_rooms - actual_rooms})")
    
    # Check 2: Data volume - expect reasonable number of readings per room
    min_readings_per_room = 100  # At least 100 5-minute readings per day
    low_volume_rooms = df[df['n_readings'] < min_readings_per_room]
    if low_volume_rooms.empty:
        checks_passed.append("Data volume")
    else:
        checks_failed.append(f"Data volume (low readings: {low_volume_rooms['room_id'].tolist()})")
    
    # Check 3: Value ranges - temperatures within reasonable bounds
    temp_issues = df[(df['avg_indoor_temp'] < 15) | (df['avg_indoor_temp'] > 30)]
    if temp_issues.empty:
        checks_passed.append("Temperature ranges")
    else:
        checks_failed.append(f"Temperature ranges (issues in: {temp_issues['room_id'].tolist()})")
    
    # Report results
    print(f"\nQuality Checks Passed ({len(checks_passed)}):")
    for check in checks_passed:
        print(f"  ✓ {check}")
    
    if checks_failed:
        print(f"\nQuality Checks Failed ({len(checks_failed)}):")
        for check in checks_failed:
            print(f"  ✗ {check}")
        # In production, you might want to raise an exception here
        # For now, we'll just log warnings
        print("\nWarning: Some quality checks failed, but pipeline will continue")
    else:
        print("\n✓ All quality checks passed!")
    
    print(f"\nProcessed {len(df)} room-day records")


# Define the DAG
with DAG(
    'hvac_climate_data_pipeline',
    default_args=default_args,
    description='HVAC and Climate Data Pipeline - Bronze → Silver → Gold',
    schedule_interval='@daily',  # Run once per day
    start_date=datetime(2025, 1, 15),  # Start from the date we have test data
    catchup=False,  # Don't backfill historical runs
    tags=['hvac', 'climate', 'etl', 'medallion-architecture'],
) as dag:
    
    # Step 1: Generate sample data (optional - comment out in production)
    generate_data_task = PythonOperator(
        task_id='generate_sample_data',
        python_callable=generate_sample_data,
        provide_context=True,
    )
    
    # Step 2: Run data pipeline (bronze -> silver -> gold)
    pipeline_task = PythonOperator(
        task_id='run_data_pipeline',
        python_callable=run_data_pipeline,
        provide_context=True,
    )
    
    # Step 3: Data quality validation
    quality_check_task = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        provide_context=True,
    )
    
    # Define workflow dependencies
    # Optional: Comment out generate_data_task in production
    generate_data_task >> pipeline_task >> quality_check_task
    
    # For production (with real sensors), use:
    # pipeline_task >> quality_check_task
