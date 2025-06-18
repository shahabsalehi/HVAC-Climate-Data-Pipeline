#!/usr/bin/env python3
"""
Unified Sample Data Generator

Generates realistic sample HVAC and climate data for testing and development.
This is a convenience wrapper that generates both indoor sensor data and outdoor weather data.

Usage:
    python scripts/generate_sample_data.py
    python scripts/generate_sample_data.py --days 7
    python scripts/generate_sample_data.py --start-date 2025-01-15 --days 7
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Import the individual generators
try:
    from generate_indoor_data import main as generate_indoor
    from generate_outdoor_data import main as generate_outdoor
except ImportError:
    # If running from parent directory
    sys.path.insert(0, str(Path(__file__).parent))
    from generate_indoor_data import main as generate_indoor
    from generate_outdoor_data import main as generate_outdoor


def main():
    """Generate sample HVAC indoor and outdoor data with a single command"""
    parser = argparse.ArgumentParser(
        description='Generate sample HVAC and climate data for testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 2 days of data starting from today
  python scripts/generate_sample_data.py

  # Generate 7 days of data
  python scripts/generate_sample_data.py --days 7

  # Generate data for a specific date range
  python scripts/generate_sample_data.py --start-date 2025-01-15 --days 7
        """
    )
    
    parser.add_argument(
        '--start-date',
        default=datetime.now().strftime('%Y-%m-%d'),
        help='Start date in YYYY-MM-DD format (default: today)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=2,
        help='Number of days of data to generate (default: 2)'
    )
    parser.add_argument(
        '--indoor-dir',
        default='data/raw/indoor',
        help='Output directory for indoor data (default: data/raw/indoor)'
    )
    parser.add_argument(
        '--outdoor-dir',
        default='data/raw/outdoor',
        help='Output directory for outdoor data (default: data/raw/outdoor)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("HVAC Climate Data Pipeline - Sample Data Generator")
    print("=" * 60)
    print(f"\nStart Date: {args.start_date}")
    print(f"Duration: {args.days} day(s)")
    print(f"Indoor data: {args.indoor_dir}")
    print(f"Outdoor data: {args.outdoor_dir}")
    print()
    
    # Generate indoor sensor data
    print("Generating indoor sensor data...")
    print("-" * 60)
    try:
        generate_indoor(
            start_date=args.start_date,
            days=args.days,
            out_dir=args.indoor_dir
        )
        print("✓ Indoor sensor data generated successfully")
    except Exception as e:
        print(f"✗ Error generating indoor data: {e}")
        return 1
    
    print()
    
    # Generate outdoor weather data
    print("Generating outdoor weather data...")
    print("-" * 60)
    try:
        generate_outdoor(
            start_date=args.start_date,
            days=args.days,
            out_dir=args.outdoor_dir
        )
        print("✓ Outdoor weather data generated successfully")
    except Exception as e:
        print(f"✗ Error generating outdoor data: {e}")
        return 1
    
    print()
    print("=" * 60)
    print("Sample data generation complete!")
    print("=" * 60)
    print()
    print("Next steps:")
    print(f"  1. Run the pipeline: python scripts/ingest_transform.py --date {args.start_date}")
    print("  2. Start the API: uvicorn api.main:app --reload")
    print("  3. View API docs: http://localhost:8000/docs")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
