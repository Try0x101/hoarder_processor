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

if ! command -v redis-cli &> /dev/null; then
    echo "FATAL: redis-cli is not installed or not in PATH. Please install redis-tools (e.g., sudo apt-get install redis-tools)." >&2
    exit 1
fi

retries=15
echo "Waiting for Redis Sentinel to elect a master..."
while [ -z "$(redis-cli -p 26379 sentinel get-master-addr-by-name mymaster 2>/dev/null)" ]; do
    retries=$((retries - 1))
    if [ $retries -le 0 ]; then
        echo "FATAL: Redis master not elected by Sentinel."
        docker compose logs
        exit 1
    fi
    sleep 2
done
echo "Redis master is available."

sleep 3

echo "Initializing database..."
python3 scripts/init_db.py
echo "Database initialization complete."

if [ ! -f "$DB_FILE" ]; then
    echo "FATAL: Database file '$DB_FILE' not found after initialization." >&2
    exit 1
fi

pkill -9 -f "celery -A celery_app" || true
pkill -9 -f "uvicorn app.main:app --host 127.0.0.1 --port 8001" || true
sleep 2

export GOOGLE_CLIENT_ID="9386374739-9tcsvhkan37q3hqq22dvh5e5op7d752g.apps.googleusercontent.com"
export GOOGLE_CLIENT_SECRET="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"
export SESSION_SECRET_KEY="659c4efed7e605c944c3166acbb6978b35a9eb6a67845a40ac2ab7e73fd2f8ae"
export ALLOWED_USER_EMAIL="try0x101@gmail.com"
export PYTHONUNBUFFERED=1

PYTHONPATH=. celery -A celery_app worker --loglevel=INFO -n processor_worker@%h --without-mingle > "$CELERY_LOG_FILE" 2>&1 &
CELERY_PID=$!

PYTHONPATH=. celery -A celery_app beat --loglevel=INFO > "$CELERY_BEAT_LOG_FILE" 2>&1 &
CELERY_BEAT_PID=$!

uvicorn app.main:app --host 127.0.0.1 --port ${PROJECT_PORT} > "$UVICORN_LOG_FILE" 2>&1 &
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