# POLYMARKET BTC-5 - Render free web service
# Listens on $PORT (Render sets this; default 10000). Ephemeral disk: SQLite is restored
# from / backed up to the Hugging Face bucket when HF_TOKEN is set.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1 \
    HF_HUB_DISABLE_PROGRESS_BARS=1 \
    BTC5_FEED_SOURCE=cloud \
    BTC5_FEED_ROLE=primary \
    BTC5_DB_BACKUP=1 \
    BTC5_BACKUP_EVERY=600 \
    BTC5_INPROCESS_PUSH=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && mkdir -p /app/data

COPY backup.py pusher.py supervisor.py verify_resolutions.py pytest.ini render_start.py ./
COPY btc5/ btc5/
COPY static/ static/

# Render routes traffic to $PORT (commonly 10000). The process binds that port at runtime.
EXPOSE 10000
CMD ["python", "render_start.py"]
