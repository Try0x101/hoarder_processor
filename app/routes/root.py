from fastapi import APIRouter, Request
from fastapi.routing import APIRoute
from collections import defaultdict
import aiosqlite
import os
import orjson
import math
import datetime
from app.utils import format_utc_timestamp
from .data_access import query_recent_devices, build_base_url, DB_PATH

router = APIRouter()
MAX_DB_SIZE_BYTES = 10 * 1024 * 1024 * 1024

def format_db_size(size_bytes: int) -> str:
    if not isinstance(size_bytes, int) or size_bytes < 0:
        return "N/A"
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    if size_bytes < 1024:
        return f"{size_bytes} B"
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p)
    return f"{s} {size_name[i]}"

def format_retention_period(days: float) -> str:
    if days <= 0:
        return "Less than a minute"
    seconds = days * 86400
    if seconds < 3600:
        val = round(seconds / 60)
        return f"{val} minute{'s' if val != 1 else ''}"
    if seconds < 86400:
        val = round(seconds / 3600)
        return f"{val} hour{'s' if val != 1 else ''}"
    if seconds < 86400 * 7:
        val = round(seconds / 86400)
        return f"{val} day{'s' if val != 1 else ''}"
    if seconds < 86400 * 30:
        val = round(seconds / (86400 * 7))
        return f"{val} week{'s' if val != 1 else ''}"
    if seconds < 86400 * 365:
        val = round(seconds / (86400 * 30.44))
        return f"{val} month{'s' if val != 1 else ''}"
    years = round(seconds / (86400 * 365.25), 1)
    if years == int(years):
        return f"{int(years)} year{'s' if int(years) != 1 else ''}"
    return f"{years} years"

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
    for device in devices_raw:
        device_id = device.get('device_id')
        payload = orjson.loads(device['enriched_payload'])
        
        traffic_stats = {
            "average_total_traffic_per_day": "N/A",
            "average_total_traffic_per_week": "N/A",
            "average_total_traffic_per_month": "N/A"
        }
        
        total_bytes = device.get('total_bytes') or 0
        first_seen_ts = device.get('first_seen_ts')
        
        if total_bytes == 0:
            traffic_stats = {
                "average_total_traffic_per_day": "0 B",
                "average_total_traffic_per_week": "0 B",
                "average_total_traffic_per_month": "0 B"
            }
        elif first_seen_ts:
            try:
                naive_dt = datetime.datetime.fromisoformat(first_seen_ts.replace(" ", "T"))
                first_seen_dt = naive_dt.replace(tzinfo=datetime.timezone.utc)
                now_dt = datetime.datetime.now(datetime.timezone.utc)
                days_active = max((now_dt - first_seen_dt).total_seconds() / 86400.0, 1.0/24.0)
                avg_bytes_per_day = total_bytes / days_active
                traffic_stats["average_total_traffic_per_day"] = f"{format_db_size(int(avg_bytes_per_day))}"
                traffic_stats["average_total_traffic_per_week"] = f"{format_db_size(int(avg_bytes_per_day * 7))}"
                traffic_stats["average_total_traffic_per_month"] = f"{format_db_size(int(avg_bytes_per_day * 30.44))}"
            except (ValueError, TypeError, ZeroDivisionError) as e:
                print(f"ERROR calculating traffic for device {device_id}: {e}. First seen TS: '{first_seen_ts}'")
                pass

        recent_devices.append({
            "device_id": device_id,
            "device_name": payload.get("identity", {}).get("device_name"),
            "client_ip": payload.get("network", {}).get("source_ip"),
            "last_seen": format_utc_timestamp(device['last_updated_ts']),
            "total_records": device['total_records'],
            "traffic": traffic_stats,
            'links': {
                'latest': f"{base_url}/data/latest/{device['device_id']}",
                'history': f"{base_url}/data/history?device_id={device_id}&limit=50"
            }
        })
    
    db_stats = {}
    try:
        async with aiosqlite.connect(f"file:{DB_PATH}?mode=ro", uri=True) as db:
            db.row_factory = aiosqlite.Row
            total_devices_cur = await db.execute("SELECT COUNT(DISTINCT device_id) as c FROM enriched_telemetry")
            total_records_cur = await db.execute("SELECT COUNT(*) as c FROM enriched_telemetry")
            time_range_cur = await db.execute("SELECT MIN(calculated_event_timestamp) as oldest, MAX(calculated_event_timestamp) as newest FROM enriched_telemetry")
            total_devices = await total_devices_cur.fetchone()
            total_records = await total_records_cur.fetchone()
            time_range = await time_range_cur.fetchone()
        
        db_files_info = []
        total_db_size = 0
        for suffix in ["", "-wal", "-shm"]:
            filepath = DB_PATH + suffix
            if os.path.exists(filepath):
                size_bytes = os.path.getsize(filepath)
                total_db_size += size_bytes
                db_files_info.append({
                    "file": os.path.basename(filepath),
                    "size": format_db_size(size_bytes),
                    "path": filepath
                })
        
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
                    storage_estimation["database_retention"] = format_retention_period(days_of_data)
                    storage_estimation["storage_rate_per_day"] = format_db_size(int(rate_bytes_day))
                    if rate_bytes_day > 0 and remaining_bytes > 0:
                        days_left = remaining_bytes / rate_bytes_day
                        est_time = f"{days_left / 30:.1f} months" if days_left > 60 else f"{days_left:.1f} days"
                        storage_estimation["estimated_time_until_full"] = est_time
            except (ValueError, TypeError, ZeroDivisionError) as e:
                print(f"ERROR calculating storage estimation: {e}")
                pass

        db_stats = {
            "total_processed_records": total_records['c'] if total_records else 0,
            "total_unique_devices": total_devices['c'] if total_devices else 0,
            "oldest_record_timestamp_utc": format_utc_timestamp(time_range['oldest']) if time_range else None,
            "newest_record_timestamp_utc": format_utc_timestamp(time_range['newest']) if time_range else None,
            "database_files": db_files_info,
            "total_database_size": format_db_size(total_db_size),
            "database_size_limit": format_db_size(MAX_DB_SIZE_BYTES),
            "storage_estimation": storage_estimation
        }
    except aiosqlite.Error as e:
        db_stats = {"error": f"Could not query database statistics: {e}"}

    return {
        "request": {"self_url": f"{base_url}/"},
        "server": "Hoarder Processor Server",
        "status": "online",
        "diagnostics": {
            "database_stats": db_stats,
            "webhook_status": "Receiving data from ingest server",
            "broker_status": "Configured and active (see worker logs for status)"
        },
        "recently_processed_devices": recent_devices,
        "api_endpoints": dict(sorted(endpoint_groups.items()))
    }