FROM python:3.10-slim
WORKDIR /app
# I'm using 'tenacity' for retries to make the system robust
RUN pip install --no-cache-dir minio tenacity
COPY ingest.py .
CMD ["python", "ingest.py"]