import asyncio
import aiosqlite
import datetime
import orjson
import redis.asyncio as redis
from typing import List, Dict, Any
from celery_app import celery_app
from app.processing import enrich_record, fetch_weather_data
from app.database import get_latest_state_for_device, save_stateful_data, DB_PATH, get_device_weather_state, save_device_weather_state
from app.utils import deep_merge, calculate_distance_km

async def _process_and_store_statefully(records: List[Dict[str, Any]]):
    records_to_save = []
    sorted_records = sorted(records, key=lambda r: r.get('calculated_event_timestamp', ''))
    redis_client = None
    
    try:
        redis_client = redis.from_url("redis://localhost:6380/4", decode_responses=True, socket_timeout=2)
        async with aiosqlite.connect(DB_PATH) as db:
            for record in sorted_records:
                device_id = record.get("device_id")
                if not device_id: continue

                partial_enrichment = await enrich_record(record)
                if not partial_enrichment: continue
                
                new_data = partial_enrichment["enriched_payload"]
                
                try:
                    lat_str = new_data.get("location", {}).get("latitude")
                    lon_str = new_data.get("location", {}).get("longitude")
                    if lat_str and lon_str:
                        current_lat, current_lon = float(lat_str), float(lon_str)
                        last_weather = await get_device_weather_state(redis_client, device_id)
                        
                        update = False
                        if not last_weather:
                            update = True
                        else:
                            time_diff = datetime.datetime.now(datetime.timezone.utc).timestamp() - last_weather.get("ts", 0)
                            dist = calculate_distance_km(current_lat, current_lon, last_weather.get("lat"), last_weather.get("lon"))
                            if time_diff > 3600 or dist > 1.0:
                                update = True
                        
                        if update:
                            weather = await fetch_weather_data(current_lat, current_lon)
                            if weather:
                                new_data = deep_merge(weather, new_data)
                                await save_device_weather_state(redis_client, device_id, current_lat, current_lon)
                except (ValueError, TypeError):
                    pass
                
                new_diagnostics = new_data.pop("diagnostics", {})
                
                base_state = await get_latest_state_for_device(db, device_id) or {}
                base_state.pop("diagnostics", None)
                
                merged_state = deep_merge(new_data, base_state)
                
                merged_state["diagnostics"] = new_diagnostics
                
                final_record = {
                    "original_ingest_id": partial_enrichment["original_ingest_id"],
                    "device_id": device_id,
                    "enriched_payload": merged_state,
                    "calculated_event_timestamp": partial_enrichment["calculated_event_timestamp"],
                }
                records_to_save.append(final_record)

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
