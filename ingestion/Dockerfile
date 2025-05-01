FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests \
    azure-storage-file-datalake \
    jsonschema

COPY ingest_github.py .

CMD ["python", "ingest_github.py"]