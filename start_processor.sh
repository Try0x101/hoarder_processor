#!/bin/bash
set -euo pipefail

VENV_DIR="venv"
LOG_DIR="logs"
CELERY_LOG_FILE="${LOG_DIR}/celery_worker.log"
UVICORN_LOG_FILE="${LOG_DIR}/uvicorn.log"
PROJECT_PORT="8001"
DB_FILE="hoarder_processor.db"

mkdir -p "$LOG_DIR"
> "$CELERY_LOG_FILE"
> "$UVICORN_LOG_FILE"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"
pip install -r requirements.txt --quiet

if ! command -v redis-cli &> /dev/null || ! redis-cli ping > /dev/null 2>&1; then
    echo "Error: Redis is not running or not reachable." >&2
    exit 1
fi

echo "Initializing database..."
python3 scripts/init_db.py

if [ ! -f "$DB_FILE" ]; then
    echo "Error: Database file '$DB_FILE' was not created. Halting." >&2
    exit 1
fi
echo "Database is ready."

echo "Stopping any stale processes..."
PORT_PID=$(lsof -ti :${PROJECT_PORT} || true)
if [ -n "$PORT_PID" ]; then
    kill -9 "$PORT_PID"
fi

pkill -9 -f "celery -A celery_app" || true
sleep 2

echo "Starting Celery worker in background..."
PYTHONPATH=. celery -A celery_app worker --loglevel=INFO -n processor_worker@%h > "$CELERY_LOG_FILE" 2>&1 &
CELERY_PID=$!

echo "Starting Uvicorn server in background..."
uvicorn app.main:app --host 0.0.0.0 --port ${PROJECT_PORT} > "$UVICORN_LOG_FILE" 2>&1 &
UVICORN_PID=$!
sleep 3

if ! ps -p $CELERY_PID > /dev/null; then
    echo "Error: Celery worker failed to start. Check logs in ${CELERY_LOG_FILE}" >&2
    exit 1
fi
if ! ps -p $UVICORN_PID > /dev/null; then
    echo "Error: Uvicorn server failed to start. Check logs in ${UVICORN_LOG_FILE}" >&2
    exit 1
fi

echo "Services started successfully. Tailing logs..."
trap "echo '...Stopping services'; kill -9 $CELERY_PID $UVICORN_PID; exit 0" INT
tail -f --pid=$CELERY_PID --pid=$UVICORN_PID "$CELERY_LOG_FILE" "$UVICORN_LOG_FILE"
