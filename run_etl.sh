#!/bin/bash

# Stop on any error
set -e

echo "🔹 Starting ETL process..."

echo "🔸 Step 1: Extract"
python extract.py

echo "🔸 Step 2: Transform"
python transform.py

echo "🔸 Step 3: Load"
python load.py

echo "✅ ETL process completed successfully!"
