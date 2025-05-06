FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy only requirements.txt
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all local files to the container
COPY . .
RUN chmod +x run_etl.sh

# Run your ETL chain script
CMD ["bash", "run_etl.sh"]
