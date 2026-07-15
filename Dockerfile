# Dockerfile
# Lightweight container running the Python ETL pipeline.
# Connects to the `postgres` service defined in docker-compose.yml.

FROM python:3.12-slim

WORKDIR /app

# Install dependencies first for better layer caching
COPY etl/requirements.txt ./etl/requirements.txt
RUN pip install --no-cache-dir -r etl/requirements.txt

# Copy application code and data
COPY etl/ ./etl/
COPY data/ ./data/

# Default: run the full pipeline (extract -> transform -> load)
CMD ["python", "etl/etl_pipeline.py"]
