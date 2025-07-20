import asyncio
import aiosqlite
import redis.asyncio as redis
import os
from typing import List, Dict, Any
from celery_app import celery_app
from app.database import get_latest_state_for_device, save_stateful_data, DB_PATH
from app.utils import deep_merge, cleanup_empty
from app.weather import get_weather_enrichment
from app.transforms import transform_payload

MAX_DB_SIZE_BYTES = 10 * 1024 * 1024 * 1024
TARGET_DB_SIZE_BYTES = 9 * 1024 * 1024 * 1024

def prepare_flat_data(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **record.get("payload", {}),
        "device_id": record.get("device_id"),
        "original_ingest_id": record.get("id"),
        "request_id": record.get("request_id"),
        "calculated_event_timestamp": record.get("calculated_event_timestamp"),
        "received_at": record.get("received_at"),
        "warnings": record.get("warnings"),
        "client_ip": record.get("request_headers", {}).get("client_ip"),
        "request_headers": record.get("request_headers", {})
    }

async def _process_and_store_statefully(records: List[Dict[str, Any]]):
    records_to_save = []
    sorted_records = sorted(records, key=lambda r: r.get('calculated_event_timestamp', ''))
    redis_client = None
    task_weather_cache = {}
    
    try:
        redis_client = redis.from_url("redis://localhost:6380/4", decode_responses=True, socket_timeout=2)
        async with aiosqlite.connect(DB_PATH) as db:
            for record in sorted_records:
                device_id = record.get("device_id")
                if not device_id: continue

                flat_data = prepare_flat_data(record)
                
                base_state, last_known_ts = await get_latest_state_for_device(db, device_id)
                current_record_ts = flat_data.get("calculated_event_timestamp")
                if current_record_ts and last_known_ts and current_record_ts <= last_known_ts:
                    continue

                cache_key = None
                lat, lon = flat_data.get('y'), flat_data.get('x')
                if lat and lon:
                    try: cache_key = f"{round(float(lat), 2)}:{round(float(lon), 2)}"
                    except (ValueError, TypeError): pass

                if cache_key and cache_key in task_weather_cache:
                    flat_data.update(task_weather_cache[cache_key])
                else:
                    weather_keys_before = {k for k in flat_data if 'temp' in k or 'wind' in k or 'marine' in k}
                    flat_data = await get_weather_enrichment(redis_client, device_id, flat_data)
                    weather_keys_after = {k for k in flat_data if 'temp' in k or 'wind' in k or 'marine' in k}
                    
                    if cache_key and len(weather_keys_after) > len(weather_keys_before):
                        added_keys = weather_keys_after - weather_keys_before
                        task_weather_cache[cache_key] = {k: flat_data[k] for k in added_keys}

                structured_payload = transform_payload(flat_data)
                final_payload = cleanup_empty(structured_payload)

                new_diagnostics = final_payload.pop("diagnostics", {})
                
                base_state = base_state or {}
                base_state.pop("diagnostics", None)
                
                merged_state = deep_merge(final_payload, base_state)
                merged_state["diagnostics"] = new_diagnostics
                
                records_to_save.append({
                    "original_ingest_id": flat_data.get("original_ingest_id"),
                    "device_id": device_id,
                    "enriched_payload": merged_state,
                    "calculated_event_timestamp": current_record_ts,
                })

        if records_to_save:
            await save_stateful_data(records_to_save)

    except Exception as e:
        print(f"Error in stateful processing task: {e}")
    finally:
        if redis_client:
            await redis_client.close()

@celery_app.task(name="processor.process_and_store_data")
def process_and_store_data(records: List[Dict[str, Any]]):
    if not records:
        return
    asyncio.run(_process_and_store_statefully(records))

async def _async_cleanup_db():
    try:
        total_size = sum(os.path.getsize(DB_PATH + s) for s in ["", "-wal", "-shm"] if os.path.exists(DB_PATH + s))
        if total_size < MAX_DB_SIZE_BYTES:
            return

        async with aiosqlite.connect(DB_PATH, timeout=120) as db:
            while total_size > TARGET_DB_SIZE_BYTES:
                cursor = await db.execute(
                    "DELETE FROM enriched_telemetry WHERE id IN (SELECT id FROM enriched_telemetry ORDER BY calculated_event_timestamp ASC, id ASC LIMIT 1000)"
                )
                if cursor.rowcount == 0:
                    break
                await db.commit()
                total_size = sum(os.path.getsize(DB_PATH + s) for s in ["", "-wal", "-shm"] if os.path.exists(DB_PATH + s))
            
            await db.execute("VACUUM")
            await db.commit()
    except Exception as e:
        print(f"Error during DB cleanup: {e}")

@celery_app.task(name="processor.cleanup_db")
def cleanup_db():
    asyncio.run(_async_cleanup_db())
