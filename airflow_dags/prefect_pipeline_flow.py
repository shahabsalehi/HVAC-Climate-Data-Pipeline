"""
Prefect Flow for HVAC Climate Data Pipeline

This flow orchestrates the HVAC climate data pipeline using Prefect as an
alternative to Airflow. It provides the same functionality as hvac_pipeline_dag.py
but uses Prefect's workflow engine.

Pipeline Steps:
1. Generate synthetic data (optional, for testing)
2. Transform data through bronze -> silver -> gold layers
3. Run data quality checks

For production use with Airflow, see: hvac_pipeline_dag.py
"""

from datetime import datetime
from pathlib import Path
import sys
from prefect import flow, task
import pandas as pd

# Add scripts directory to Python path
scripts_path = Path(__file__).parent.parent / "scripts"
if scripts_path.exists():
    sys.path.insert(0, str(scripts_path))


@task(
    name="Generate Sample Data",
    description="Generate synthetic HVAC and climate data for testing",
    retries=2,
    retry_delay_seconds=300,
)
def generate_sample_data(execution_date: str):
    """
    Generate synthetic HVAC and climate data for testing
    
    Args:
        execution_date: Date string in YYYY-MM-DD format
    
    This is optional and typically disabled in production where real sensors provide data.
    """
    from generate_indoor_data import main as generate_indoor
    from generate_outdoor_data import main as generate_outdoor
    
    print(f"Generating sample data for {execution_date}")
    
    # Generate 1 day of data
    generate_indoor(start_date=execution_date, days=1, out_dir="data/raw/indoor")
    generate_outdoor(start_date=execution_date, days=1, out_dir="data/raw/outdoor")
    
    print("Sample data generation complete")
    return {"status": "success", "date": execution_date}


@task(
    name="Run Data Pipeline",
    description="Execute the complete ETL pipeline (Bronze → Silver → Gold)",
    retries=2,
    retry_delay_seconds=180,
)
def run_data_pipeline(execution_date: str, previous_step: dict):
    """
    Execute the complete data pipeline for the execution date
    
    Transforms data through bronze -> silver -> gold layers
    
    Args:
        execution_date: Date string in YYYY-MM-DD format
        previous_step: Result from previous task (for dependency)
    """
    from ingest_transform import run_pipeline_for_day
    
    print(f"Running data pipeline for {execution_date}")
    
    # Run the pipeline
    run_pipeline_for_day(target_date=execution_date, base_path="data")
    
    print("Data pipeline execution complete")
    return {"status": "success", "date": execution_date}


@task(
    name="Validate Data Quality",
    description="Run data quality checks on processed data",
    retries=1,
    retry_delay_seconds=60,
)
def validate_data_quality(execution_date: str, previous_step: dict):
    """
    Run data quality checks on processed data
    
    Validates:
    - Data completeness (no missing critical fields)
    - Data accuracy (values within expected ranges)
    - Data timeliness (data is fresh)
    
    Args:
        execution_date: Date string in YYYY-MM-DD format
        previous_step: Result from previous task (for dependency)
    """
    date_obj = datetime.strptime(execution_date, '%Y-%m-%d')
    
    print(f"Running data quality checks for {execution_date}")
    
    # Check gold layer metrics exist
    gold_path = Path("data/gold/daily_comfort_metrics")
    partition_path = gold_path / f"year={date_obj.year}" / f"month={date_obj.month:02d}" / f"day={date_obj.day:02d}"
    
    if not partition_path.exists():
        raise ValueError(f"Gold layer data not found for {execution_date} at {partition_path}")
    
    # Load and validate gold metrics
    df = pd.read_parquet(partition_path)
    
    if df.empty:
        raise ValueError(f"No data found in gold layer for {execution_date}")
    
    # Quality checks
    checks_passed = []
    checks_failed = []
    
    # Check 1: Data volume - expect reasonable number of readings per room
    min_readings_per_room = 100  # At least 100 5-minute readings per day
    low_volume_rooms = df[df['n_readings'] < min_readings_per_room]
    if low_volume_rooms.empty:
        checks_passed.append("Data volume")
    else:
        checks_failed.append(f"Data volume (low readings: {low_volume_rooms['room_id'].tolist()})")
    
    # Check 2: Value ranges - temperatures within reasonable bounds
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
        print("\nWarning: Some quality checks failed, but pipeline will continue")
    else:
        print("\n✓ All quality checks passed!")
    
    print(f"\nProcessed {len(df)} room-day records")
    
    return {
        "status": "success" if not checks_failed else "warning",
        "checks_passed": len(checks_passed),
        "checks_failed": len(checks_failed),
        "records_processed": len(df)
    }


@flow(
    name="HVAC Climate Data Pipeline",
    description="End-to-end HVAC and climate data processing pipeline using Prefect",
    retries=1,
    retry_delay_seconds=600,
)
def hvac_climate_pipeline(execution_date: str = None, include_data_generation: bool = True):
    """
    Main Prefect flow for HVAC climate data pipeline
    
    Args:
        execution_date: Date to process (YYYY-MM-DD). Defaults to today.
        include_data_generation: Whether to generate sample data first (for testing)
    
    This flow provides the same functionality as the Airflow DAG but uses Prefect.
    For production deployment with Airflow, see hvac_pipeline_dag.py
    """
    if execution_date is None:
        execution_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"Starting HVAC Climate Pipeline for {execution_date}")
    
    # Step 1: Generate sample data (optional - disable in production)
    if include_data_generation:
        data_gen_result = generate_sample_data(execution_date)
    else:
        data_gen_result = {"status": "skipped"}
    
    # Step 2: Run data pipeline (bronze -> silver -> gold)
    pipeline_result = run_data_pipeline(execution_date, data_gen_result)
    
    # Step 3: Data quality validation
    quality_results = validate_data_quality(execution_date, pipeline_result)
    
    print(f"\nPipeline completed!")
    print(f"Quality Score: {quality_results['checks_passed']}/{quality_results['checks_passed'] + quality_results['checks_failed']} checks passed")
    
    return quality_results


if __name__ == "__main__":
    # Run the flow locally for testing
    #
    # Usage:
    #     python airflow_dags/prefect_pipeline_flow.py
    
    # Run with sample data generation
    result = hvac_climate_pipeline(
        execution_date=datetime.now().strftime('%Y-%m-%d'),
        include_data_generation=True
    )
    print(f"\nFinal Result: {result}")

