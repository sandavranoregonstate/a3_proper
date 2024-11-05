#!/bin/bash

# Move up one directory
cd ..

# Check if requirements file exists
if [ ! -f requirements.txt ]; then
    echo "requirements.txt file not found!"
    exit 1
fi

# Create a virtual environment in the 'venv' directory
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install requirements from requirements.txt
pip install -r requirements.txt

echo "Virtual environment setup complete and requirements installed. The environment is now activated."
