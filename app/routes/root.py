from fastapi import APIRouter, Request
from fastapi.routing import APIRoute
from collections import defaultdict
import aiosqlite
import os
import orjson
import math
import asyncio
import datetime
import time
from typing import Optional
import redis.asyncio as redis
from app.utils import format_utc_timestamp, reconstruct_from_freshness
from .data_access import query_recent_devices, build_base_url
from app.database import DB_PATH, REDIS_SENTINEL_HOSTS, REDIS_MASTER_NAME, REDIS_DB_METRICS

router = APIRouter()
MAX_DB_SIZE_BYTES = 10 * 1024 * 1024 * 1024
APP_START_TIME = time.time()

def format_last_seen_ago(seconds: Optional[float]) -> Optional[str]:
    if seconds is None or seconds < 0:
        return None
    if seconds < 2:
        return "1 second ago"
    if seconds < 60:
        return f"{round(seconds)} seconds ago"
    if seconds < 3600:
        val = round(seconds / 60)
        return f"{val} minute{'s' if val != 1 else ''} ago"
    if seconds < 86400:
        val = round(seconds / 3600)
        return f"{val} hour{'s' if val != 1 else ''} ago"
    if seconds < 86400 * 7:
        val = round(seconds / 86400)
        return f"{val} day{'s' if val != 1 else ''} ago"
    if seconds < 86400 * 30.44:
        val = round(seconds / (86400 * 7))
        return f"{val} week{'s' if val != 1 else ''} ago"
    if seconds < 86400 * 365.25:
        val = round(seconds / (86400 * 30.44))
        return f"{val} month{'s' if val != 1 else ''} ago"
    
    years = round(seconds / (86400 * 365.25), 1)
    if years == int(years):
        val = int(years)
        return f"{val} year{'s' if val != 1 else ''} ago"
    return f"{years} years ago"

def check_process_log_status(log_file_path: str, threshold_seconds: int) -> str:
    if not os.path.exists(log_file_path):
        return "not_found"
    try:
        last_mod_time = os.path.getmtime(log_file_path)
        if (time.time() - last_mod_time) > threshold_seconds:
            return "stale"
        return "active"
    except OSError:
        return "error"

async def _get_historical_metrics():
    redis_client = None
    try:
        sentinel = redis.Sentinel(REDIS_SENTINEL_HOSTS, socket_timeout=0.5, decode_responses=True)
        redis_client = sentinel.master_for(REDIS_MASTER_NAME, db=REDIS_DB_METRICS)
        
        system_stats_raw, processing_stats_raw = await asyncio.gather(
            redis_client.lrange("system_stats", 0, -1),
            redis_client.lrange("processing_stats", 0, -1)
        )
        now = time.time()
        
        sys_metrics_1m, sys_metrics_1h = [], []
        for item in system_stats_raw:
            try:
                data = orjson.loads(item)
                age = now - data['ts']
                if age <= 3600:
                    sys_metrics_1h.append(data)
                    if age <= 60: sys_metrics_1m.append(data)
            except (orjson.JSONDecodeError, KeyError): continue

        cpu_1m = sum(d['cpu_percent'] for d in sys_metrics_1m) / len(sys_metrics_1m) if sys_metrics_1m else None
        mem_1m = sum(d['mem_rss_bytes'] for d in sys_metrics_1m) / len(sys_metrics_1m) if sys_metrics_1m else None
        cpu_1h = sum(d['cpu_percent'] for d in sys_metrics_1h) / len(sys_metrics_1h) if sys_metrics_1h else None
        mem_1h = sum(d['mem_rss_bytes'] for d in sys_metrics_1h) / len(sys_metrics_1h) if sys_metrics_1h else None

        proc_metrics_1m, proc_metrics_1h = [], []
        for item in processing_stats_raw:
            try:
                data = orjson.loads(item)
                age = now - data['ts']
                if age <= 3600:
                    proc_metrics_1h.append(data)
                    if age <= 60: proc_metrics_1m.append(data)
            except (orjson.JSONDecodeError, KeyError): continue
                
        total_duration_1m = sum(d['duration'] for d in proc_metrics_1m)
        total_count_1m = sum(d['count'] for d in proc_metrics_1m)
        avg_time_1m_ms = (total_duration_1m / total_count_1m * 1000) if total_count_1m > 0 else None
        
        total_duration_1h = sum(d['duration'] for d in proc_metrics_1h)
        total_count_1h = sum(d['count'] for d in proc_metrics_1h)
        avg_time_1h_ms = (total_duration_1h / total_count_1h * 1000) if total_count_1h > 0 else None

        return {
            "cpu_usage_average_last_1_minute_perc": round(cpu_1m, 2) if cpu_1m is not None else None,
            "cpu_usage_average_last_1_hour_perc": round(cpu_1h, 2) if cpu_1h is not None else None,
            "memory_usage_average_last_1_minute_mb": int(round(mem_1m / 1024**2)) if mem_1m is not None else None,
            "memory_usage_average_last_1_hour_mb": int(round(mem_1h / 1024**2)) if mem_1h is not None else None,
            "record_processing_time_average_last_1_minute_ms": int(round(avg_time_1m_ms)) if avg_time_1m_ms is not None else None,
            "record_processing_time_average_last_1_hour_ms": int(round(avg_time_1h_ms)) if avg_time_1h_ms is not None else None,
        }
    except redis.RedisError: return {}
    finally:
        if redis_client: await redis_client.close()

@router.get("/", tags=["Root"])
async def root(request: Request):
    base_url = build_base_url(request)
    
    endpoint_groups = defaultdict(list)
    for route in request.app.routes:
        if isinstance(route, APIRoute) and route.tags and route.include_in_schema:
            group = route.tags[0]
            endpoint_groups[group].append({
                "path": f"{base_url}{route.path}",
                "name": route.name,
                "methods": sorted(list(route.methods)),
            })

    devices_raw = await query_recent_devices(limit=10)
    recent_devices = []
    now_for_ago = datetime.datetime.now(datetime.timezone.utc)
    for device in devices_raw:
        device_id = device.get('device_id')
        freshness_payload = orjson.loads(device['enriched_payload'])
        payload = reconstruct_from_freshness(freshness_payload)
        
        last_seen_ts_utc = None
        last_seen_ago = None
        ts_str = device.get('last_updated_ts')
        if ts_str:
            try:
                naive_dt = datetime.datetime.fromisoformat(ts_str.replace(" ", "T"))
                aware_dt = naive_dt.replace(tzinfo=datetime.timezone.utc)
                last_seen_ts_utc = int(aware_dt.timestamp())
                seconds_ago = (now_for_ago - aware_dt).total_seconds()
                last_seen_ago = format_last_seen_ago(seconds_ago)
            except (ValueError, TypeError):
                pass

        traffic_stats = {
            "average_total_traffic_per_day_in_kb": None,
            "average_total_traffic_per_week_in_mb": None,
            "average_total_traffic_per_month_in_mb": None
        }
        
        total_bytes = device.get('total_bytes') or 0
        first_seen_ts = device.get('first_seen_ts')
        
        if total_bytes == 0:
            traffic_stats = {
                "average_total_traffic_per_day_in_kb": 0,
                "average_total_traffic_per_week_in_mb": 0,
                "average_total_traffic_per_month_in_mb": 0
            }
        elif first_seen_ts:
            try:
                naive_dt = datetime.datetime.fromisoformat(first_seen_ts.replace(" ", "T"))
                first_seen_dt = naive_dt.replace(tzinfo=datetime.timezone.utc)
                days_active = max((now_for_ago - first_seen_dt).total_seconds() / 86400.0, 1.0/24.0)
                avg_bytes_per_day = total_bytes / days_active
                traffic_stats["average_total_traffic_per_day_in_kb"] = int(round(avg_bytes_per_day / 1024))
                traffic_stats["average_total_traffic_per_week_in_mb"] = int(round((avg_bytes_per_day * 7) / (1024 * 1024)))
                traffic_stats["average_total_traffic_per_month_in_mb"] = int(round((avg_bytes_per_day * 30.44) / (1024 * 1024)))
            except (ValueError, TypeError, ZeroDivisionError) as e:
                print(f"ERROR calculating traffic for device {device_id}: {e}. First seen TS: '{first_seen_ts}'")
                pass
        
        device_diagnostics = {
            "last_seen_timestamp_utc": last_seen_ts_utc,
            "total_records": device['total_records'],
            "traffic": traffic_stats
        }

        recent_devices.append({
            "device_id": device_id,
            "device_name": payload.get("identity", {}).get("device_name"),
            "client_ip": payload.get("network", {}).get("source_ip"),
            "last_seen_ago": last_seen_ago,
            "diagnostics": device_diagnostics,
            'links': {
                'latest': f"{base_url}/data/latest/{device['device_id']}",
                'history': f"{base_url}/data/history?device_id={device_id}&limit=50"
            }
        })
    
    db_stats = {}
    system_health = {}
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    try:
        async with aiosqlite.connect(f"file:{DB_PATH}?mode=ro", uri=True) as db:
            db.row_factory = aiosqlite.Row
            total_devices_cur = await db.execute("SELECT COUNT(DISTINCT device_id) as c FROM enriched_telemetry")
            total_records_cur = await db.execute("SELECT COUNT(*) as c FROM enriched_telemetry")
            time_range_cur = await db.execute("SELECT MIN(calculated_event_timestamp) as oldest, MAX(calculated_event_timestamp) as newest FROM enriched_telemetry")
            
            last_hour_ts_str = (now_dt - datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
            last_24h_ts_str = (now_dt - datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
            records_last_hour_cur = await db.execute("SELECT COUNT(*) as c FROM enriched_telemetry WHERE processed_at > ?", (last_hour_ts_str,))
            active_devices_cur = await db.execute("SELECT COUNT(DISTINCT device_id) as c FROM enriched_telemetry WHERE calculated_event_timestamp > ?", (last_24h_ts_str,))
            
            total_devices = await total_devices_cur.fetchone()
            total_records = await total_records_cur.fetchone()
            time_range = await time_range_cur.fetchone()
            records_last_hour = await records_last_hour_cur.fetchone()
            active_devices = await active_devices_cur.fetchone()
        
        db_files_info = []
        total_db_size = 0
        main_db_path = DB_PATH
        if os.path.exists(main_db_path):
            size_bytes = os.path.getsize(main_db_path)
            total_db_size += size_bytes
            db_files_info.append({
                "file": os.path.basename(main_db_path),
                "size_mb": int(round(size_bytes / (1024*1024))),
                "path": main_db_path
            })
        for suffix in ["-wal", "-shm"]:
            filepath = DB_PATH + suffix
            if os.path.exists(filepath):
                total_db_size += os.path.getsize(filepath)
        
        storage_estimation = {}
        if time_range and time_range['oldest'] and time_range['newest'] and total_records and total_records['c'] > 1000:
            try:
                oldest_dt_naive = datetime.datetime.fromisoformat(time_range['oldest'].replace(" ", "T"))
                oldest_dt = oldest_dt_naive.replace(tzinfo=datetime.timezone.utc)
                newest_dt_naive = datetime.datetime.fromisoformat(time_range['newest'].replace(" ", "T"))
                newest_dt = newest_dt_naive.replace(tzinfo=datetime.timezone.utc)
                
                days_of_data = (newest_dt - oldest_dt).total_seconds() / 86400.0
                if days_of_data > 0.0001:
                    rate_bytes_day = total_db_size / days_of_data
                    remaining_bytes = MAX_DB_SIZE_BYTES - total_db_size
                    storage_estimation["database_retention_in_days"] = int(round(days_of_data))
                    storage_estimation["storage_rate_per_day_in_mb"] = int(round(rate_bytes_day / (1024*1024)))
                    if rate_bytes_day > 0 and remaining_bytes > 0:
                        days_left = remaining_bytes / rate_bytes_day
                        storage_estimation["estimated_time_untill_full_in_days"] = int(round(days_left))
            except (ValueError, TypeError, ZeroDivisionError) as e:
                print(f"ERROR calculating storage estimation: {e}")
                pass

        db_stats = {
            "total_processed_records": total_records['c'] if total_records else 0,
            "total_unique_devices": total_devices['c'] if total_devices else 0,
            "oldest_record": format_utc_timestamp(time_range['oldest']) if time_range else None,
            "newest_record": format_utc_timestamp(time_range['newest']) if time_range else None,
            "database_files": db_files_info,
            "total_database_size_in_mb": int(round(total_db_size / (1024 * 1024))),
            "database_size_limit_in_mb": int(MAX_DB_SIZE_BYTES / (1024 * 1024)),
            "storage_estimation": storage_estimation
        }

        time_since_last_record_seconds = None
        if time_range and time_range['newest']:
            try:
                newest_dt_naive = datetime.datetime.fromisoformat(time_range['newest'].replace(" ", "T"))
                newest_dt = newest_dt_naive.replace(tzinfo=datetime.timezone.utc)
                time_since_last_record_seconds = int((now_dt - newest_dt).total_seconds())
            except (ValueError, TypeError):
                pass
        
        LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "logs"))
        CELERY_LOG_FILE = os.path.join(LOG_DIR, "celery_worker.log")
        CELERY_BEAT_LOG_FILE = os.path.join(LOG_DIR, "celery_beat.log")
        
        uptime_seconds = time.time() - APP_START_TIME
        celery_worker_status = check_process_log_status(CELERY_LOG_FILE, 180)
        celery_beat_status = check_process_log_status(CELERY_BEAT_LOG_FILE, 90)
        
        health_checks = [
            celery_worker_status == 'active',
            celery_beat_status == 'active',
            db_stats.get("error") is None,
            time_since_last_record_seconds is not None and time_since_last_record_seconds < 1800
        ]
        uptime_percentage = round((sum(health_checks) / len(health_checks)) * 100, 2)

        system_health = {
            "uptime": {
                "time_since_restart_utc": int(APP_START_TIME),
                "time_since_restart": format_last_seen_ago(uptime_seconds),
                "uptime_percentage": uptime_percentage
            },
            "activity": {
                "time_since_last_record_seconds": time_since_last_record_seconds,
                "records_processed_last_hour": records_last_hour['c'] if records_last_hour else 0,
                "active_devices_last_24h": active_devices['c'] if active_devices else 0
            },
            "celery_status": {
                "worker": celery_worker_status,
                "beat": celery_beat_status
            },
            "performance": await _get_historical_metrics()
        }

    except aiosqlite.Error as e:
        db_stats = {"error": f"Could not query database statistics: {e}"}
        system_health = {"error": f"Could not query system health: {e}"}

    return {
        "request": {"self_url": f"{base_url}/"},
        "server": "Hoarder Processor Server",
        "system_health": system_health,
        "diagnostics": {
            "database_stats": db_stats
        },
        "recently_processed_devices": recent_devices,
        "api_endpoints": dict(sorted(endpoint_groups.items()))
    }