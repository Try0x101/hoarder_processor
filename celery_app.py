from celery import Celery

celery_app = Celery(
    'hoarder_processor',
    # Use different DB numbers to isolate from hoarder_ingest
    broker="redis://localhost:6380/2",
    backend="redis://localhost:6380/3",
    include=['app.tasks']
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
    worker_prefetch_multiplier=1
)
