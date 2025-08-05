from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_process_init
import sqlite3
from app.geojson_processor import settings as geojson_settings
from app.database import (
    REDIS_SENTINEL_HOSTS, REDIS_MASTER_NAME,
    CELERY_DB_BROKER, CELERY_DB_BACKEND, DB_PATH
)
from app.utils import OUI_VENDOR_MAP

@worker_process_init.connect
def on_worker_init(**kwargs):
    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        cur = con.cursor()
        cur.execute("SELECT oui, vendor FROM oui_vendors")
        rows = cur.fetchall()
        OUI_VENDOR_MAP.update({row[0]: row[1] for row in rows})
        con.close()
    except Exception:
        # In production, this error would be caught by monitoring.
        # For now, we allow it to fail silently if the DB is not ready.
        pass

celery_app = Celery(
    'hoarder_processor',
    include=['app.tasks', 'app.geojson_processor.tasks']
)

celery_app.conf.broker_url = ";".join([f"sentinel://{host}:{port}" for host, port in REDIS_SENTINEL_HOSTS])
celery_app.conf.result_backend = celery_app.conf.broker_url

celery_app.conf.broker_transport_options = {
    'master_name': REDIS_MASTER_NAME,
    'db': CELERY_DB_BROKER
}
celery_app.conf.result_backend_transport_options = {
    'master_name': REDIS_MASTER_NAME,
    'db': CELERY_DB_BACKEND
}

celery_app.conf.update(
    task_track_started=True,
    broker_connection_retry_on_startup=True,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    worker_pool_restarts=True,
    worker_max_tasks_per_child=1000,
    worker_prefetch_multiplier=1,
    beat_schedule_filename="/opt/hoarder_processor/logs/celerybeat-schedule",
    beat_schedule={
        'cleanup-db-every-6-hours': {
            'task': 'processor.cleanup_db',
            'schedule': crontab(minute=15, hour='*/6'),
        },
        'run-geojson-processor': {
            'task': 'geojson_processor.run',
            'schedule': geojson_settings.TASK_SCHEDULE_SECONDS,
        },
        'monitor-system-every-15-seconds': {
            'task': 'processor.monitor_system',
            'schedule': 15.0,
        },
    }
)
