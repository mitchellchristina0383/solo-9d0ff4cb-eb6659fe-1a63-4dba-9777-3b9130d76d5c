FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd --system --uid 10001 cache \
    && mkdir -p /data \
    && chown cache:cache /data
USER cache

ENV CACHE_DB_PATH=/data/cache.db \
    CACHE_STORAGE_DIR=/data/storage \
    TOKEN_TTL_SECONDS=300 \
    MAX_UPLOAD_BYTES=1048576 \
    ORPHAN_GRACE_SECONDS=60 \
    SWEEP_INTERVAL_SECONDS=30

VOLUME /data
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=4)" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
