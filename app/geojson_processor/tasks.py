import asyncio
import redis.asyncio as redis
from celery_app import celery_app
from .processor import run_processor_once
from app.database import REDIS_SENTINEL_HOSTS, REDIS_MASTER_NAME, REDIS_DB_POSITION

GEOJSON_LOCK_KEY = "lock:geojson_processor"
GEOJSON_LOCK_TIMEOUT = 3600  # 1 hour

async def _run_with_lock():
    redis_client = None
    lock_acquired = False
    try:
        sentinel = redis.Sentinel(REDIS_SENTINEL_HOSTS, socket_timeout=0.5, decode_responses=True)
        redis_client = sentinel.master_for(REDIS_MASTER_NAME, db=REDIS_DB_POSITION)
        
        lock_acquired = await redis_client.set(GEOJSON_LOCK_KEY, "1", ex=GEOJSON_LOCK_TIMEOUT, nx=True)
        
        if not lock_acquired:
            print("GEOJSON: Run skipped, another process already holds the lock.")
            return

        await run_processor_once()

    except redis.RedisError as e:
        print(f"GEOJSON: Redis error during task execution: {e}")
    except Exception as e:
        print(f"Error running GeoJSON processor task: {e}")
    finally:
        if lock_acquired and redis_client:
            await redis_client.delete(GEOJSON_LOCK_KEY)
        if redis_client:
            await redis_client.close()

@celery_app.task(name="geojson_processor.run")
def run_geojson_processor_task():
    asyncio.run(_run_with_lock())