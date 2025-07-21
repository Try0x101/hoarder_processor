import asyncio
from celery_app import celery_app
from .processor import run_processor_once

@celery_app.task(name="geojson_processor.run")
def run_geojson_processor_task():
    try:
        asyncio.run(run_processor_once())
    except Exception as e:
        print(f"Error running GeoJSON processor task: {e}")
