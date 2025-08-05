import asyncio
import aiosqlite
import redis.asyncio as redis
import os
import copy
import datetime
import orjson
import psutil
import time
import redis as sync_redis
from typing import List, Dict, Any
from celery_app import celery_app
from app.database import (
    get_latest_state_for_device, save_stateful_data, DB_PATH,
    REDIS_SENTINEL_HOSTS, REDIS_MASTER_NAME,
    REDIS_DB_POSITION, REDIS_DB_METRICS, REDIS_DB_IP_INTEL,
    get_device_batch_ts, save_device_batch_ts, delete_device_batch_ts,
    ensure_db_initialized
)
from app.utils import cleanup_empty, reconstruct_from_freshness, update_freshness_from_full_state
from app.weather import get_weather_enrichment
from app.transforms import transform_payload
from app.ip_intelligence import get_ip_intelligence

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
    start_time = time.monotonic()
    records_to_save = []
    
    records_by_device: Dict[str, List[Dict[str, Any]]] = {}
    for r in records:
        device_id = r.get("device_id")
        if device_id:
            records_by_device.setdefault(device_id, []).append(r)

    position_redis_client = None
    ip_intel_redis_client = None
    sentinel = redis.Sentinel(REDIS_SENTINEL_HOSTS, socket_timeout=0.5, decode_responses=True)
    
    try:
        position_redis_client = sentinel.master_for(REDIS_MASTER_NAME, db=REDIS_DB_POSITION)
        ip_intel_redis_client = sentinel.master_for(REDIS_MASTER_NAME, db=REDIS_DB_IP_INTEL)
        
        async with aiosqlite.connect(DB_PATH) as db:
            await ensure_db_initialized(db)
            for device_id, device_records in records_by_device.items():
                sorted_records = sorted(device_records, key=lambda r: r.get('calculated_event_timestamp', ''))
                
                base_ts = await get_device_batch_ts(position_redis_client, device_id)
                current_freshness_payload, _ = await get_latest_state_for_device(db, device_id)

                for record in sorted_records:
                    payload = record.get("payload", {})
                    event_timestamp_dt = None
                    
                    if 'ts' in payload and payload['ts'] is not None:
                        base_ts = int(payload['ts'])
                        await save_device_batch_ts(position_redis_client, device_id, base_ts)
                        event_timestamp_dt = datetime.datetime.fromtimestamp(base_ts, tz=datetime.timezone.utc)
                    elif 'to' in payload and base_ts is not None and payload['to'] is not None:
                        event_timestamp_dt = datetime.datetime.fromtimestamp(base_ts + int(payload['to']), tz=datetime.timezone.utc)
                    elif record.get('received_at'):
                        try:
                            event_timestamp_dt = datetime.datetime.fromisoformat(record['received_at'].replace(" ", "T").replace("Z", "+00:00"))
                            base_ts = None
                            await delete_device_batch_ts(position_redis_client, device_id)
                        except (ValueError, TypeError):
                            continue
                    else:
                        continue
                    
                    record['calculated_event_timestamp'] = event_timestamp_dt.isoformat(sep=' ', timespec='seconds')
                    
                    request_size_bytes = len(orjson.dumps(record))
                    flat_data = prepare_flat_data(record)
                    
                    current_record_ts = flat_data.get("calculated_event_timestamp")
                    if not current_record_ts: continue

                    simple_base_state = reconstruct_from_freshness(current_freshness_payload) if current_freshness_payload else {}
                    
                    ip_intel_data = await get_ip_intelligence(ip_intel_redis_client, flat_data.get("client_ip"))
                    flat_data = await get_weather_enrichment(position_redis_client, device_id, flat_data)

                    new_full_simple_state = transform_payload(flat_data, simple_base_state, ip_intel_data)
                    
                    new_freshness_payload = update_freshness_from_full_state(
                        current_freshness_payload or {},
                        new_full_simple_state,
                        current_record_ts
                    )
                    
                    records_to_save.append({
                        "original_ingest_id": flat_data.get("original_ingest_id"),
                        "device_id": device_id,
                        "historical_payload": new_full_simple_state,
                        "latest_payload": new_freshness_payload,
                        "calculated_event_timestamp": current_record_ts,
                        "request_size_bytes": request_size_bytes
                    })
                    
                    current_freshness_payload = new_freshness_payload

        if records_to_save:
            await save_stateful_data(records_to_save)

        duration = time.monotonic() - start_time
        num_records = len(records_to_save)
        if num_records > 0:
            stats_redis_client = None
            try:
                stats_redis_client = sentinel.master_for(REDIS_MASTER_NAME, db=REDIS_DB_METRICS, decode_responses=False)
                data_point = orjson.dumps({
                    "ts": time.time(), "duration": duration, "count": num_records
                })
                await stats_redis_client.lpush("processing_stats", data_point)
                await stats_redis_client.ltrim("processing_stats", 0, 1999)
            except redis.RedisError:
                pass
            finally:
                if stats_redis_client:
                    await stats_redis_client.close()

    except Exception as e:
        print(f"Error in stateful processing task: {e}")
        raise
    finally:
        if position_redis_client:
            await position_redis_client.close()
        if ip_intel_redis_client:
            await ip_intel_redis_client.close()

@celery_app.task(name="processor.process_and_store_data")
def process_and_store_data(records: List[Dict[str, Any]]):
    if not records:
        return
    asyncio.run(_process_and_store_statefully(records))

async def _async_cleanup_db():
    try:
        if not os.path.exists(DB_PATH):
            return
        total_size = sum(os.path.getsize(DB_PATH + s) for s in ["", "-wal", "-shm"] if os.path.exists(DB_PATH + s))
        if total_size < MAX_DB_SIZE_BYTES:
            return

        async with aiosqlite.connect(DB_PATH, timeout=120) as db:
            await ensure_db_initialized(db)
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

def _get_app_processes():
    pids = []
    try:
        for proc in psutil.process_iter(['pid', 'cmdline']):
            if not proc.info['cmdline']:
                continue
            cmd = " ".join(proc.info['cmdline'])
            if 'uvicorn app.main:app' in cmd or \
               'celery -A celery_app worker' in cmd or \
               'celery -A celery_app beat' in cmd:
                pids.append(proc.pid)
    except psutil.Error:
        pass
    return [psutil.Process(p) for p in set(pids) if psutil.pid_exists(p)]

@celery_app.task(name="processor.monitor_system")
def monitor_system():
    redis_client = None
    try:
        processes = _get_app_processes()
        if not processes:
            return

        for p in processes:
            p.cpu_percent()
        time.sleep(0.5)

        total_cpu = sum(p.cpu_percent() for p in processes)
        total_mem = sum(p.memory_info().rss for p in processes)

        sentinel = sync_redis.Sentinel(REDIS_SENTINEL_HOSTS, socket_timeout=0.5)
        redis_client = sentinel.master_for(REDIS_MASTER_NAME, db=REDIS_DB_METRICS)
        data_point = orjson.dumps({
            "ts": time.time(), "cpu_percent": total_cpu, "mem_rss_bytes": total_mem
        })
        
        pipe = redis_client.pipeline()
        pipe.lpush("system_stats", data_point)
        pipe.ltrim("system_stats", 0, 399)
        pipe.execute()
    except (psutil.Error, sync_redis.RedisError):
        pass
    except Exception:
        pass
    finally:
        if redis_client:
            redis_client.close()