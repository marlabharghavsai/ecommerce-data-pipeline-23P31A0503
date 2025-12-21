#!/bin/bash

echo "Setting up E-Commerce Data Pipeline environment..."

python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

echo "Setup completed successfully."
