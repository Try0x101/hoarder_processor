import asyncio
import aiosqlite
import redis.asyncio as redis
import os
import copy
import datetime
import orjson
from typing import List, Dict, Any
from celery_app import celery_app
from app.database import get_latest_state_for_device, save_stateful_data, DB_PATH
from app.utils import deep_merge, cleanup_empty, merge_and_update_freshness, reconstruct_from_freshness
from app.weather import get_weather_enrichment
from app.transforms import transform_payload, format_bssid

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

                request_size_bytes = len(orjson.dumps(record))
                flat_data = prepare_flat_data(record)
                
                base_freshness_payload, last_known_ts = await get_latest_state_for_device(db, device_id)
                current_record_ts = flat_data.get("calculated_event_timestamp")

                if not current_record_ts:
                    continue

                if current_record_ts and last_known_ts and current_record_ts <= last_known_ts:
                    continue

                simple_base_state = reconstruct_from_freshness(base_freshness_payload) if base_freshness_payload else {}
                old_weather_diag = None
                if simple_base_state:
                    old_weather_diag = simple_base_state.get("diagnostics", {}).get("weather")
                    if 't' not in flat_data:
                        previous_type = simple_base_state.get("network", {}).get("type")
                        if previous_type: flat_data['t'] = previous_type
                
                raw_bssid = flat_data.get('b')
                if 'b' not in flat_data and simple_base_state:
                    flat_data['b'] = simple_base_state.get("network", {}).get("wifi_bssid")
                elif format_bssid(raw_bssid) is None:
                    if simple_base_state and 'network' in simple_base_state and 'wifi_bssid' in simple_base_state['network']:
                        del simple_base_state['network']['wifi_bssid']

                flat_data = await get_weather_enrichment(redis_client, device_id, flat_data)

                if 'weather_fetch_ts' not in flat_data and old_weather_diag:
                    loc_str = old_weather_diag.get('weather_fetch_location')
                    if loc_str:
                        try:
                            lat_s, lon_s = loc_str.split(',')
                            flat_data['weather_fetch_lat'] = float(lat_s.strip())
                            flat_data['weather_fetch_lon'] = float(lon_s.strip())
                        except (ValueError, KeyError, IndexError): pass

                    ts_str = old_weather_diag.get('weather_request_timestamp_utc')
                    if ts_str:
                        try:
                            dt_obj = datetime.datetime.strptime(ts_str, '%d.%m.%Y %H:%M:%S UTC')
                            flat_data['weather_fetch_ts'] = dt_obj.isoformat()
                        except (ValueError, TypeError): pass

                structured_payload = transform_payload(flat_data)
                new_simple_payload = cleanup_empty(structured_payload)
                
                simple_base_state_for_history = copy.deepcopy(simple_base_state)
                simple_base_state_for_history.pop("diagnostics", None)
                historical_payload = deep_merge(new_simple_payload, simple_base_state_for_history)

                latest_simple_payload_for_freshness = copy.deepcopy(new_simple_payload)
                if old_weather_diag and not latest_simple_payload_for_freshness.get("diagnostics", {}).get("weather"):
                    latest_simple_payload_for_freshness.setdefault("diagnostics", {}).setdefault("weather", old_weather_diag)

                latest_freshness_payload = merge_and_update_freshness(
                    base_freshness_payload or {},
                    latest_simple_payload_for_freshness,
                    current_record_ts
                )
                
                records_to_save.append({
                    "original_ingest_id": flat_data.get("original_ingest_id"),
                    "device_id": device_id,
                    "historical_payload": historical_payload,
                    "latest_payload": latest_freshness_payload,
                    "calculated_event_timestamp": current_record_ts,
                    "request_size_bytes": request_size_bytes
                })

        if records_to_save:
            await save_stateful_data(records_to_save)

    except Exception as e:
        print(f"Error in stateful processing task: {e}")
        raise
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
