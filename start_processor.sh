#!/bin/bash
set -euo pipefail

VENV_DIR="venv"
LOG_DIR="logs"
CELERY_LOG_FILE="${LOG_DIR}/celery_worker.log"
UVICORN_LOG_FILE="${LOG_DIR}/uvicorn.log"
PROJECT_PORT="8001"

mkdir -p "$LOG_DIR"
> "$CELERY_LOG_FILE"
> "$UVICORN_LOG_FILE"

if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"
pip install -r requirements.txt --quiet

if ! command -v redis-cli &> /dev/null; then
    exit 1
fi
if ! redis-cli ping > /dev/null 2>&1; then
    exit 1
fi
python3 scripts/init_db.py > /dev/null 2>&1

PORT_PID=$(lsof -ti :${PROJECT_PORT} || true)
if [ -n "$PORT_PID" ]; then
    kill -9 "$PORT_PID"
fi

pkill -9 -f "celery -A celery_app" || true
sleep 2

PYTHONPATH=. celery -A celery_app worker --loglevel=INFO -n processor_worker@%h > "$CELERY_LOG_FILE" 2>&1 &
CELERY_PID=$!

uvicorn app.main:app --host 0.0.0.0 --port ${PROJECT_PORT} > "$UVICORN_LOG_FILE" 2>&1 &
UVICORN_PID=$!
sleep 3

if ! ps -p $CELERY_PID > /dev/null; then
    exit 1
fi
if ! ps -p $UVICORN_PID > /dev/null; then
    exit 1
fi

trap "exit 0" INT
tail -f --pid=$CELERY_PID --pid=$UVICORN_PID "$CELERY_LOG_FILE" "$UVICORN_LOG_FILE"
