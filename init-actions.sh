#!/bin/bash

# This script will run on all nodes in the cluster
set -e # Exit immediately if a command fails

echo "INFO: Installing Python dependencies from requirements.txt"
pip install --upgrade pip
pip install -r requirements.txt