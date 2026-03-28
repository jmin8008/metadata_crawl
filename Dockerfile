FROM python:3.11-slim AS base

WORKDIR /app

# System deps for lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev libxslt1-dev gcc && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
ENV MC_DOWNLOAD_DIR=/app/data/downloads
ENV MC_CACHE_DIR=/app/data/cache
ENV MC_LOG_DIR=/app/data/logs

VOLUME ["/app/data"]

ENTRYPOINT ["python", "-m", "src.pipeline"]
