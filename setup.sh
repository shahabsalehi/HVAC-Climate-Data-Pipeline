#!/bin/bash

# HVAC Climate Data Pipeline - Setup Script
# This script sets up the development environment

set -e

echo "=================================="
echo "HVAC Climate Data Pipeline Setup"
echo "=================================="
echo ""

# Check Python version
echo "Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python version: $python_version"

# Check if Python 3.9+ is available
if ! python3 -c 'import sys; assert sys.version_info >= (3, 9)' 2>/dev/null; then
    echo "Error: Python 3.9 or higher is required"
    exit 1
fi

# Create virtual environment
if [ ! -d "venv" ]; then
    echo ""
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "Virtual environment created!"
else
    echo ""
    echo "Virtual environment already exists."
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo ""
echo "Installing dependencies from requirements.txt..."
echo "(This may take a few minutes...)"
pip install -r requirements.txt

echo ""
echo "=================================="
echo "Setup Complete!"
echo "=================================="
echo ""
echo "To activate the virtual environment, run:"
echo "  source venv/bin/activate"
echo ""
echo "To deactivate, run:"
echo "  deactivate"
echo ""
echo "Next steps:"
echo "  1. Generate sample data: python scripts/generate_sample_data.py"
echo "  2. Run the pipeline: python scripts/ingest_transform.py --date 2025-01-15"
echo "  3. Start the API: uvicorn api.main:app --reload"
echo "  4. Run tests: pytest tests/ -v"
echo ""
echo "For more commands, run: make help"
echo ""
