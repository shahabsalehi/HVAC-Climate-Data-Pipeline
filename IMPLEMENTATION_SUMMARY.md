# HVAC Climate Data Pipeline - Implementation Summary

## Overview
A medallion-style HVAC climate pipeline that generates synthetic indoor and outdoor data, transforms it through bronze, silver, and gold layers, and serves results through a small FastAPI service. Airflow orchestration, an exploration notebook, and integration tests are included to keep the workflow reproducible.

## Components
- **Data generation**: `scripts/generate_indoor_data.py` (indoor sensors: temperature, humidity, CO2, VOC across four rooms, 5-minute cadence) and `scripts/generate_outdoor_data.py` (hourly weather with temperature, humidity, wind speed).
- **ETL pipeline**: `scripts/ingest_transform.py` handles ingestion, bronze casting, temporal joins, 5-minute resampling, comfort flags, and daily aggregations. Output is partitioned Parquet by year/month/day.
- **Medallion layout**: `data/bronze/` (typed raw), `data/silver/hvac_comfort_facts/` (joined and enriched), `data/gold/daily_comfort_metrics/` (daily KPIs per room, including overcooling and stale-air percentages).
- **Orchestration**: `airflow_dags/hvac_pipeline_dag.py` runs optional data generation, ETL, and validation with daily scheduling and basic retries.
- **API**: `api/main.py` exposes health, room listing, and comfort summaries/overcooling/air-quality endpoints backed by the gold layer.
- **Visualization**: `notebooks/exploration.ipynb` for comparing indoor/outdoor temps, overcooling highlights, air quality, and per-room metrics.
- **Documentation**: `README.md` plus `diagrams/architecture.mmd` for the end-to-end flow and schemas.
- **Tests**: `tests/test_pipeline.py` covers generators, transformations, and API responses.

## How to Run
1. Generate data:
   ```bash
   python scripts/generate_indoor_data.py --start-date 2025-01-15 --days 2
   python scripts/generate_outdoor_data.py --start-date 2025-01-15 --days 2
   ```
2. Run the pipeline:
   ```bash
   python scripts/ingest_transform.py --date 2025-01-15
   ```
3. Start the API:
   ```bash
   uvicorn api.main:app --reload
   ```
4. Explore data:
   ```bash
   jupyter notebook notebooks/exploration.ipynb
   ```
5. Run tests:
   ```bash
   pytest tests/ -v
   ```

## Metrics and Status
- Three data layers with partitioned Parquet outputs.
- Four REST endpoints in the FastAPI service.
- Plotly notebook for manual QA of comfort and air-quality trends.
- Integration tests validate generation, transformations, and API responses.

## Future Enhancements
- Real-time streaming with Kafka.
- Predictive models for maintenance.
- Cloud deployment automation and monitoring dashboards.
