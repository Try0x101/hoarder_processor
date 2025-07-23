from celery import Celery
from celery.schedules import crontab
from app.geojson_processor import settings as geojson_settings

celery_app = Celery(
    'hoarder_processor',
    broker="redis://localhost:6380/2",
    backend="redis://localhost:6380/3",
    include=['app.tasks', 'app.geojson_processor.tasks']
)

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
