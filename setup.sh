#!/bin/bash

echo "=============================================="
echo " Setting up E-Commerce Data Pipeline Environment"
echo "=============================================="

# Exit immediately if a command fails
set -e

# Check Python
if ! command -v python3 &> /dev/null
then
    echo "Python3 not found. Please install Python 3.8+"
    exit 1
fi

# Create virtual environment
echo "Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Load environment variables
if [ -f ".env" ]; then
  echo "Loading environment variables from .env"
  export $(grep -v '^#' .env | xargs)
else
  echo "WARNING: .env file not found. Using system environment variables."
fi

echo "=============================================="
echo " Environment setup completed successfully"
echo "=============================================="

