#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

VENV_DIR="venv"
LOG_DIR="logs"
CELERY_LOG_FILE="${LOG_DIR}/celery_worker.log"
CELERY_BEAT_LOG_FILE="${LOG_DIR}/celery_beat.log"
UVICORN_LOG_FILE="${LOG_DIR}/uvicorn.log"
PROJECT_PORT="8001"
DB_FILE="hoarder_processor.db"
REDIS_SERVICE_NAME="redis"

mkdir -p "$LOG_DIR"
> "$CELERY_LOG_FILE"
> "$CELERY_BEAT_LOG_FILE"
> "$UVICORN_LOG_FILE"

if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"
pip install -r requirements.txt --quiet

if [ -f "docker-compose.yml" ]; then
    docker compose down --remove-orphans
    docker compose up -d
fi

retries=15
while ! docker compose exec ${REDIS_SERVICE_NAME} redis-cli set startup_check 1 > /dev/null 2>&1; do
    retries=$((retries - 1))
    if [ $retries -le 0 ]; then
        docker compose logs ${REDIS_SERVICE_NAME}
        exit 1
    fi
    sleep 2
done

sleep 3

python3 scripts/init_db.py

if [ ! -f "$DB_FILE" ]; then
    exit 1
fi

pkill -9 -f "celery -A celery_app" || true
pkill -9 -f "uvicorn app.main:app --host 0.0.0.0 --port 8001" || true
sleep 2

PYTHONPATH=. celery -A celery_app worker --loglevel=INFO -n processor_worker@%h --without-mingle > "$CELERY_LOG_FILE" 2>&1 &
CELERY_PID=$!

PYTHONPATH=. celery -A celery_app beat --loglevel=INFO > "$CELERY_BEAT_LOG_FILE" 2>&1 &
CELERY_BEAT_PID=$!

uvicorn app.main:app --host 0.0.0.0 --port ${PROJECT_PORT} > "$UVICORN_LOG_FILE" 2>&1 &
UVICORN_PID=$!
sleep 3

if ! ps -p $CELERY_PID > /dev/null; then
    echo "FATAL: Celery worker failed to start. Check logs in ${CELERY_LOG_FILE}" >&2
    exit 1
fi
if ! ps -p $CELERY_BEAT_PID > /dev/null; then
    echo "FATAL: Celery beat failed to start. Check logs in ${CELERY_BEAT_LOG_FILE}" >&2
    exit 1
fi
if ! ps -p $UVICORN_PID > /dev/null; then
    echo "FATAL: Uvicorn server failed to start. Check logs in ${UVICORN_LOG_FILE}" >&2
    exit 1
fi

trap "echo '...Stopping services'; kill -9 $CELERY_PID $UVICORN_PID $CELERY_BEAT_PID; docker compose down; exit 0" INT
tail -f --pid=$CELERY_PID --pid=$UVICORN_PID --pid=$CELERY_BEAT_PID "$CELERY_LOG_FILE" "$CELERY_BEAT_LOG_FILE" "$UVICORN_LOG_FILE"
