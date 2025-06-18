.PHONY: help install install-dev test lint format clean generate-data run-api export-json validate-json venv

# Virtual environment
VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

help:  ## Show this help message
	@echo "HVAC Climate Data Pipeline - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

venv: $(VENV)/bin/activate  ## Create virtual environment and install dependencies

$(VENV)/bin/activate: requirements.txt
	python -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	touch $(VENV)/bin/activate

install: venv  ## Install production dependencies (via venv)

install-dev: venv  ## Install development dependencies
	$(PIP) install -r requirements-dev.txt

test: venv  ## Run tests with pytest
	$(PYTHON) -m pytest tests/ -v

test-cov: venv  ## Run tests with coverage report
	$(PYTHON) -m pytest tests/ -v --cov=. --cov-report=html --cov-report=term

lint: venv  ## Run linters (flake8, mypy)
	$(VENV)/bin/flake8 airflow_dags/ api/ scripts/ tests/
	$(VENV)/bin/mypy airflow_dags/ api/ scripts/ tests/ || true

format: venv  ## Format code with black and isort
	$(VENV)/bin/black airflow_dags/ api/ scripts/ tests/
	$(VENV)/bin/isort airflow_dags/ api/ scripts/ tests/

format-check: venv  ## Check code formatting without making changes
	$(VENV)/bin/black --check airflow_dags/ api/ scripts/ tests/
	$(VENV)/bin/isort --check-only airflow_dags/ api/ scripts/ tests/

clean:  ## Clean up generated files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache htmlcov .coverage
	@echo "Cleaned up generated files"

generate-data: venv  ## Generate sample HVAC and climate data (2 days by default)
	$(PYTHON) scripts/generate_sample_data.py

sample-data: venv  ## Generate sample HVAC and climate data (alias for generate-data)
	$(PYTHON) scripts/generate_sample_data.py

run-api: venv  ## Run the FastAPI server
	$(VENV)/bin/uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

run-api-prod: venv  ## Run the FastAPI server in production mode
	$(VENV)/bin/uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4

jupyter: venv  ## Start Jupyter Lab
	$(VENV)/bin/jupyter lab notebooks/

setup:  ## Run setup script to install dependencies
	./setup.sh

quick-start: venv  ## Quick start: generate data, run tests (uses venv)
	@echo "Running quick start setup..."
	@echo "Generating sample data..."
	$(PYTHON) scripts/generate_sample_data.py
	@echo "Running tests..."
	$(PYTHON) -m pytest tests/ -v
	@echo ""
	@echo "Quick start complete! Next: make run-api"

all: clean format lint test  ## Run all checks
	@echo "All checks completed!"

export-json: venv  ## Export gold-layer data to canonical JSON for frontend
	@echo "Exporting climate telemetry to canonical JSON..."
	@mkdir -p artifacts/json
	$(PYTHON) scripts/export_json.py
	@test -f artifacts/json/hvac_climate_telemetry.json || (echo "ERROR: hvac_climate_telemetry.json not created" && exit 1)
	@echo "Done! Output: artifacts/json/hvac_climate_telemetry.json"

validate-json: venv  ## Validate exported JSON against schema
	@echo "Validating JSON schema..."
	$(PYTHON) scripts/validate_json.py
