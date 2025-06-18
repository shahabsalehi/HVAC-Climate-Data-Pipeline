"""
Tests for HVAC Climate Data Pipeline

Comprehensive test suite covering:
- Data generation
- Data transformation (bronze, silver, gold)
- API endpoints
- Data quality checks
"""

import pytest
import pandas as pd
import json
from datetime import date
from pathlib import Path
import tempfile
import shutil
import sys

# Import modules to test
from scripts.generate_indoor_data import simulate_value, main as generate_indoor_main
from scripts.generate_outdoor_data import main as generate_outdoor_main
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from generate_sample_data import main as generate_sample_main
from scripts.ingest_transform import (
    load_raw_indoor,
    load_raw_outdoor,
    transform_to_bronze,
    join_and_compute_comfort,
    compute_daily_metrics,
)


# Test fixtures
@pytest.fixture
def temp_data_dir():
    """Create a temporary data directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_indoor_jsonl(temp_data_dir):
    """Create sample indoor JSONL file"""
    data_dir = temp_data_dir / "raw" / "indoor"
    data_dir.mkdir(parents=True)
    
    # Create sample data
    events = [
        {
            "event_id": 1,
            "ts_utc": "2025-01-15T00:00:00",
            "building_id": "building_A",
            "room_id": "office_1",
            "sensor_type": "temp",
            "value": 22.5,
            "unit": "C"
        },
        {
            "event_id": 2,
            "ts_utc": "2025-01-15T00:00:00",
            "building_id": "building_A",
            "room_id": "office_1",
            "sensor_type": "humidity",
            "value": 45.0,
            "unit": "%"
        },
        {
            "event_id": 3,
            "ts_utc": "2025-01-15T00:00:00",
            "building_id": "building_A",
            "room_id": "office_1",
            "sensor_type": "co2",
            "value": 800,
            "unit": "ppm"
        },
        {
            "event_id": 4,
            "ts_utc": "2025-01-15T00:00:00",
            "building_id": "building_A",
            "room_id": "office_1",
            "sensor_type": "voc",
            "value": 150,
            "unit": "ppb"
        }
    ]
    
    file_path = data_dir / "test_indoor.jsonl"
    with open(file_path, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')
    
    return file_path


@pytest.fixture
def sample_outdoor_jsonl(temp_data_dir):
    """Create sample outdoor JSONL file"""
    data_dir = temp_data_dir / "raw" / "outdoor"
    data_dir.mkdir(parents=True)
    
    # Create sample data
    records = [
        {
            "ts_utc": "2025-01-15T00:00:00",
            "latitude": 59.3293,
            "longitude": 18.0686,
            "temp_c": 10.0,
            "rel_humidity_pct": 60.0,
            "wind_speed_ms": 3.5,
            "source": "test"
        }
    ]
    
    file_path = data_dir / "test_outdoor.jsonl"
    with open(file_path, 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')
    
    return file_path


# Tests for data generators
class TestDataGenerators:
    """Tests for data generation scripts"""
    
    def test_simulate_temp_value(self):
        """Test temperature simulation"""
        value = simulate_value("temp", hour=12, base_offset=0)
        assert 18 <= value <= 30, f"Temperature {value} out of reasonable range"
    
    def test_simulate_humidity_value(self):
        """Test humidity simulation"""
        value = simulate_value("humidity", hour=12, base_offset=0)
        assert 20 <= value <= 70, f"Humidity {value} out of reasonable range"
    
    def test_simulate_co2_value(self):
        """Test CO2 simulation"""
        value = simulate_value("co2", hour=12, base_offset=0)
        assert 300 <= value <= 1500, f"CO2 {value} out of reasonable range"
    
    def test_simulate_voc_value(self):
        """Test VOC simulation"""
        value = simulate_value("voc", hour=12, base_offset=0)
        assert 50 <= value <= 500, f"VOC {value} out of reasonable range"
    
    def test_generate_indoor_data(self, temp_data_dir):
        """Test indoor data generation"""
        out_dir = temp_data_dir / "raw" / "indoor"
        generate_indoor_main(
            start_date="2025-01-15",
            days=1,
            out_dir=str(out_dir),
            freq_minutes=5
        )
        
        # Check file was created
        files = list(out_dir.glob("*.jsonl"))
        assert len(files) == 1, "Expected one JSONL file"
        
        # Check file has data
        with open(files[0], 'r') as f:
            lines = f.readlines()
            assert len(lines) > 0, "File should have data"
            
            # Parse first line
            first_event = json.loads(lines[0])
            assert 'event_id' in first_event
            assert 'ts_utc' in first_event
            assert 'sensor_type' in first_event
    
    def test_generate_outdoor_data(self, temp_data_dir):
        """Test outdoor data generation"""
        out_dir = temp_data_dir / "raw" / "outdoor"
        generate_outdoor_main(
            start_date="2025-01-15",
            days=1,
            out_dir=str(out_dir)
        )
        
        # Check file was created
        files = list(out_dir.glob("*.jsonl"))
        assert len(files) == 1, "Expected one JSONL file"
        
        # Check file has data
        with open(files[0], 'r') as f:
            lines = f.readlines()
            assert len(lines) == 24, "Expected 24 hourly readings"
    
    def test_generate_sample_data_unified(self, temp_data_dir, monkeypatch):
        """Test unified sample data generation script"""
        
        # Mock sys.argv to pass arguments to the script
        test_args = [
            'generate_sample_data.py',
            '--start-date', '2025-01-15',
            '--days', '1',
            '--indoor-dir', str(temp_data_dir / "indoor"),
            '--outdoor-dir', str(temp_data_dir / "outdoor")
        ]
        monkeypatch.setattr(sys, 'argv', test_args)
        
        # Run the unified generator
        result = generate_sample_main()
        
        # Check it succeeded
        assert result == 0, "Script should return 0 on success"
        
        # Check indoor files created
        indoor_files = list((temp_data_dir / "indoor").glob("*.jsonl"))
        assert len(indoor_files) == 1, "Expected one indoor JSONL file"
        
        # Check outdoor files created
        outdoor_files = list((temp_data_dir / "outdoor").glob("*.jsonl"))
        assert len(outdoor_files) == 1, "Expected one outdoor JSONL file"
        
        # Verify indoor data has content
        with open(indoor_files[0], 'r') as f:
            indoor_lines = f.readlines()
            assert len(indoor_lines) > 0, "Indoor file should have data"
        
        # Verify outdoor data has content
        with open(outdoor_files[0], 'r') as f:
            outdoor_lines = f.readlines()
            assert len(outdoor_lines) == 24, "Outdoor file should have 24 hourly readings"


# Tests for data ingestion
class TestDataIngestion:
    """Tests for data ingestion module"""
    
    def test_load_raw_indoor(self, sample_indoor_jsonl):
        """Test loading indoor JSONL data"""
        pattern = str(sample_indoor_jsonl.parent / "*.jsonl")
        df = load_raw_indoor(pattern)
        
        assert not df.empty, "DataFrame should not be empty"
        assert len(df) == 4, "Expected 4 sensor events"
        assert 'event_id' in df.columns
        assert 'sensor_type' in df.columns
        assert 'value' in df.columns
    
    def test_load_raw_outdoor(self, sample_outdoor_jsonl):
        """Test loading outdoor JSONL data"""
        pattern = str(sample_outdoor_jsonl.parent / "*.jsonl")
        df = load_raw_outdoor(pattern)
        
        assert not df.empty, "DataFrame should not be empty"
        assert len(df) == 1, "Expected 1 weather record"
        assert 'temp_c' in df.columns
        assert 'rel_humidity_pct' in df.columns


# Tests for data transformation
class TestBronzeTransformation:
    """Tests for bronze layer transformation"""
    
    def test_transform_to_bronze_indoor(self, sample_indoor_jsonl):
        """Test bronze transformation for indoor data"""
        pattern = str(sample_indoor_jsonl.parent / "*.jsonl")
        raw_df = load_raw_indoor(pattern)
        bronze_df = transform_to_bronze(raw_df, "indoor")
        
        assert not bronze_df.empty
        assert '_ingestion_timestamp' in bronze_df.columns
        assert bronze_df['ts_utc'].dtype == 'datetime64[ns]'
    
    def test_transform_to_bronze_outdoor(self, sample_outdoor_jsonl):
        """Test bronze transformation for outdoor data"""
        pattern = str(sample_outdoor_jsonl.parent / "*.jsonl")
        raw_df = load_raw_outdoor(pattern)
        bronze_df = transform_to_bronze(raw_df, "outdoor")
        
        assert not bronze_df.empty
        assert '_ingestion_timestamp' in bronze_df.columns
        assert bronze_df['ts_utc'].dtype == 'datetime64[ns]'


class TestSilverTransformation:
    """Tests for silver layer transformation"""
    
    def test_join_and_compute_comfort(self, sample_indoor_jsonl, sample_outdoor_jsonl):
        """Test silver layer transformation"""
        # Load and transform to bronze
        indoor_pattern = str(sample_indoor_jsonl.parent / "*.jsonl")
        outdoor_pattern = str(sample_outdoor_jsonl.parent / "*.jsonl")
        
        indoor_df = load_raw_indoor(indoor_pattern)
        outdoor_df = load_raw_outdoor(outdoor_pattern)
        
        bronze_indoor = transform_to_bronze(indoor_df, "indoor")
        bronze_outdoor = transform_to_bronze(outdoor_df, "outdoor")
        
        # Transform to silver
        comfort_df = join_and_compute_comfort(bronze_indoor, bronze_outdoor)
        
        assert not comfort_df.empty, "Comfort DataFrame should not be empty"
        assert 'indoor_temp_c' in comfort_df.columns
        assert 'outdoor_temp_c' in comfort_df.columns
        assert 'overcooled_flag' in comfort_df.columns
        assert 'stale_air_flag' in comfort_df.columns
        assert 'date' in comfort_df.columns
    
    def test_overcooled_flag_logic(self):
        """Test overcooled flag computation"""
        # Create test data that should trigger overcooled flag
        test_data = pd.DataFrame({
            'indoor_temp_c': [20.0, 22.0, 19.0],
            'outdoor_temp_c': [26.0, 24.0, 27.0]
        })
        
        overcooled = (test_data['indoor_temp_c'] < 21) & (test_data['outdoor_temp_c'] > 25)
        
        assert overcooled.iloc[0] == True, "Should be overcooled: indoor=20, outdoor=26"
        assert overcooled.iloc[1] == False, "Should not be overcooled: indoor=22"
        assert overcooled.iloc[2] == True, "Should be overcooled: indoor=19, outdoor=27"
    
    def test_stale_air_flag_logic(self):
        """Test stale air flag computation"""
        test_data = pd.DataFrame({
            'indoor_co2_ppm': [800, 1100, 1500]
        })
        
        stale_air = test_data['indoor_co2_ppm'] > 1000
        
        assert stale_air.iloc[0] == False, "Should not be stale: co2=800"
        assert stale_air.iloc[1] == True, "Should be stale: co2=1100"
        assert stale_air.iloc[2] == True, "Should be stale: co2=1500"


class TestGoldTransformation:
    """Tests for gold layer transformation"""
    
    def test_compute_daily_metrics(self):
        """Test gold layer daily metrics computation"""
        # Create sample silver data
        test_data = pd.DataFrame({
            'ts_utc': pd.date_range('2025-01-15', periods=10, freq='5min'),
            'date': [date(2025, 1, 15)] * 10,
            'building_id': ['building_A'] * 10,
            'room_id': ['office_1'] * 10,
            'indoor_temp_c': [22.0] * 10,
            'indoor_rel_humidity_pct': [45.0] * 10,
            'indoor_co2_ppm': [800.0] * 10,
            'outdoor_temp_c': [10.0] * 10,
            'overcooled_flag': [False] * 8 + [True] * 2,
            'stale_air_flag': [False] * 9 + [True] * 1
        })
        
        gold_df = compute_daily_metrics(test_data)
        
        assert not gold_df.empty
        assert len(gold_df) == 1, "Should have one row for one room-day"
        assert gold_df['n_readings'].iloc[0] == 10
        assert gold_df['n_overcooled'].iloc[0] == 2
        assert gold_df['n_stale_air'].iloc[0] == 1
        assert gold_df['pct_time_overcooled'].iloc[0] == pytest.approx(20.0)
        assert gold_df['pct_time_stale_air'].iloc[0] == pytest.approx(10.0)


# Tests for API
class TestAPI:
    """Tests for FastAPI endpoints"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)
    
    def test_root_endpoint(self, client):
        """Test root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "status" in data
    
    def test_health_endpoint(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
    
    def test_rooms_endpoint(self, client):
        """Test rooms list endpoint"""
        response = client.get("/rooms")
        assert response.status_code in [200, 500]  # 500 if no data exists yet


# Integration tests
class TestIntegration:
    """Integration tests for end-to-end pipeline"""
    
    def test_end_to_end_pipeline(self, temp_data_dir):
        """Test complete pipeline execution"""
        # Generate data
        indoor_dir = temp_data_dir / "raw" / "indoor"
        outdoor_dir = temp_data_dir / "raw" / "outdoor"
        
        generate_indoor_main(
            start_date="2025-01-15",
            days=1,
            out_dir=str(indoor_dir),
            freq_minutes=5
        )
        
        generate_outdoor_main(
            start_date="2025-01-15",
            days=1,
            out_dir=str(outdoor_dir)
        )
        
        # Load raw data
        indoor_pattern = str(indoor_dir / "*.jsonl")
        outdoor_pattern = str(outdoor_dir / "*.jsonl")
        
        raw_indoor = load_raw_indoor(indoor_pattern)
        raw_outdoor = load_raw_outdoor(outdoor_pattern)
        
        # Transform to bronze
        bronze_indoor = transform_to_bronze(raw_indoor, "indoor")
        bronze_outdoor = transform_to_bronze(raw_outdoor, "outdoor")
        
        # Transform to silver
        comfort_facts = join_and_compute_comfort(bronze_indoor, bronze_outdoor)
        
        # Transform to gold
        daily_metrics = compute_daily_metrics(comfort_facts)
        
        # Verify results
        assert not daily_metrics.empty, "Daily metrics should not be empty"
        assert 'pct_time_overcooled' in daily_metrics.columns
        assert 'pct_time_stale_air' in daily_metrics.columns
        
        # Check we have metrics for all rooms
        expected_rooms = {"office_1", "office_2", "meeting_1", "lab_1"}
        actual_rooms = set(daily_metrics['room_id'].unique())
        assert expected_rooms == actual_rooms, f"Expected rooms {expected_rooms}, got {actual_rooms}"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])
