"""
FastAPI Application for HVAC Climate Data Pipeline

This API provides endpoints for:
- Querying processed HVAC and climate comfort metrics
- Accessing overcooling and air quality statistics
- Viewing daily summaries per room
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from datetime import datetime, date, timedelta
from typing import Optional, List
from pydantic import BaseModel
import pandas as pd
from pathlib import Path
import glob


# Initialize FastAPI app
app = FastAPI(
    title="HVAC Climate Data Pipeline API",
    description="API for accessing HVAC comfort metrics and air quality data",
    version="1.0.0",
)


# Data paths
DATA_ROOT = Path("data")
GOLD_METRICS_PATH = DATA_ROOT / "gold" / "daily_comfort_metrics"
SILVER_FACTS_PATH = DATA_ROOT / "silver" / "hvac_comfort_facts"


# Pydantic models for response
class ComfortSummary(BaseModel):
    """Model for daily comfort summary per room"""
    date: str
    building_id: str
    room_id: str
    n_readings: int
    n_overcooled: int
    n_stale_air: int
    pct_time_overcooled: float
    pct_time_stale_air: float
    avg_indoor_temp: float
    avg_indoor_humidity: float
    avg_indoor_co2: float
    avg_outdoor_temp: float


class OvercoolingMetric(BaseModel):
    """Model for overcooling time-series data"""
    date: str
    room_id: str
    pct_time_overcooled: float
    n_overcooled: int
    n_readings: int


def load_gold_metrics(start_date: date, end_date: date, room_id: Optional[str] = None) -> pd.DataFrame:
    """
    Load gold layer metrics for a date range
    
    Args:
        start_date: Start date
        end_date: End date (inclusive)
        room_id: Optional room filter
    
    Returns:
        DataFrame with gold metrics
    """
    all_data = []
    current = start_date
    
    while current <= end_date:
        # Build partition path
        partition_path = GOLD_METRICS_PATH / f"year={current.year}" / f"month={current.month:02d}" / f"day={current.day:02d}"
        
        if partition_path.exists():
            try:
                df = pd.read_parquet(partition_path)
                all_data.append(df)
            except Exception as e:
                print(f"Warning: Error reading {partition_path}: {e}")
        
        current += timedelta(days=1)
    
    if not all_data:
        return pd.DataFrame()
    
    result = pd.concat(all_data, ignore_index=True)
    
    # Filter by room if specified
    if room_id:
        result = result[result['room_id'] == room_id]
    
    return result


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "HVAC Climate Data Pipeline API",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "health": "/health",
            "comfort_summary": "/comfort/summary",
            "comfort_overcooling": "/comfort/overcooling",
            "rooms": "/rooms",
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "data_available": GOLD_METRICS_PATH.exists(),
    }


@app.get("/rooms")
async def list_rooms():
    """
    List all available rooms
    
    Returns list of unique room IDs found in the data
    """
    try:
        # Find all parquet files in gold layer
        parquet_files = sorted(glob.glob(str(GOLD_METRICS_PATH / "**/*.parquet"), recursive=True))
        
        if not parquet_files:
            return {"rooms": [], "message": "No data available"}
        
        # Load a sample to get room list
        df = pd.read_parquet(parquet_files[0])
        rooms = sorted(df['room_id'].unique().tolist())
        
        return {
            "rooms": rooms,
            "count": len(rooms)
        }
    except Exception as e:
        # Log the full error for debugging (in production, use proper logging)
        print(f"Error loading room data: {str(e)}")
        # Return generic message to client
        raise HTTPException(status_code=500, detail="Error loading room data")


@app.get("/comfort/overcooling", response_model=List[OvercoolingMetric])
async def get_overcooling_metrics(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    room_id: Optional[str] = Query(None, description="Filter by room ID")
):
    """
    Get overcooling metrics time-series
    
    Returns percentage of time overcooled per day, optionally filtered by room.
    
    Overcooled is defined as: indoor_temp < 21°C AND outdoor_temp > 25°C
    """
    try:
        # Parse dates
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")
        
        # Validate date range length
        if (end - start).days > 365:
            raise HTTPException(status_code=400, detail="Date range cannot exceed 365 days")
        
        # Load data
        df = load_gold_metrics(start, end, room_id)
        
        if df.empty:
            return []
        
        # Convert date column to string for response
        df['date'] = df['date'].astype(str)
        
        # Select relevant columns
        result = df[['date', 'room_id', 'pct_time_overcooled', 'n_overcooled', 'n_readings']]
        
        # Convert to list of dicts
        return result.to_dict('records')
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        # Log the full error for debugging (in production, use proper logging)
        print(f"Error processing overcooling request: {str(e)}")
        raise HTTPException(status_code=500, detail="Error processing request")


@app.get("/comfort/summary", response_model=List[ComfortSummary])
async def get_comfort_summary(
    date: str = Query(..., description="Date (YYYY-MM-DD)"),
    room_id: Optional[str] = Query(None, description="Filter by room ID")
):
    """
    Get daily comfort summary for all rooms (or a specific room)
    
    Returns comprehensive comfort metrics including:
    - Reading counts
    - Overcooling incidents
    - Stale air incidents
    - Average temperatures and conditions
    """
    try:
        # Parse date
        target_date = datetime.strptime(date, '%Y-%m-%d').date()
        
        # Load data for single day
        df = load_gold_metrics(target_date, target_date, room_id)
        
        if df.empty:
            return []
        
        # Convert date column to string
        df['date'] = df['date'].astype(str)
        
        # Round numeric columns for cleaner output
        numeric_cols = ['avg_indoor_temp', 'avg_indoor_humidity', 'avg_indoor_co2', 'avg_outdoor_temp']
        for col in numeric_cols:
            df[col] = df[col].round(2)
        
        # Convert to list of dicts
        return df.to_dict('records')
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        # Log the full error for debugging (in production, use proper logging)
        print(f"Error processing summary request: {str(e)}")
        raise HTTPException(status_code=500, detail="Error processing request")


@app.get("/comfort/stale-air")
async def get_stale_air_metrics(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    room_id: Optional[str] = Query(None, description="Filter by room ID")
):
    """
    Get stale air metrics time-series
    
    Returns percentage of time with stale air per day, optionally filtered by room.
    
    Stale air is defined as: indoor_co2 > 1000 ppm
    """
    try:
        # Parse dates
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        if start > end:
            raise HTTPException(status_code=400, detail="start_date must be before or equal to end_date")
        
        # Load data
        df = load_gold_metrics(start, end, room_id)
        
        if df.empty:
            return []
        
        # Convert date column to string
        df['date'] = df['date'].astype(str)
        
        # Select relevant columns
        result = df[['date', 'room_id', 'pct_time_stale_air', 'n_stale_air', 'n_readings', 'avg_indoor_co2']]
        
        # Round CO2 for cleaner output
        result['avg_indoor_co2'] = result['avg_indoor_co2'].round(0)
        
        # Convert to list of dicts
        return result.to_dict('records')
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        # Log the full error for debugging (in production, use proper logging)
        print(f"Error processing stale-air request: {str(e)}")
        raise HTTPException(status_code=500, detail="Error processing request")


if __name__ == "__main__":
    import uvicorn
    # Run the API server
    uvicorn.run(app, host="0.0.0.0", port=8000)
