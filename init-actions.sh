#!/bin/bash
set -e # Exit immediately if a command fails

echo "INFO: Copying requirements.txt from GCS..."
# This command downloads the requirements file to the local directory
gsutil cp gs://bl-dataproc-resources/resources/requirements.txt .

echo "INFO: Installing Python dependencies from requirements.txt..."
pip install --upgrade pip
pip install -r requirements.txt

echo "INFO: Successfully installed dependencies."