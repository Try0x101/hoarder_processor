from fastapi import APIRouter, Request
from fastapi.routing import APIRoute
from collections import defaultdict
import aiosqlite
import os
import orjson
import math
from app.utils import format_utc_timestamp
from .data_access import query_recent_devices, build_base_url, DB_PATH

router = APIRouter()

def format_db_size(size_bytes: int) -> str:
    if not isinstance(size_bytes, int) or size_bytes < 0:
        return "N/A"
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

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
        payload = orjson.loads(device['enriched_payload'])
        recent_devices.append({
            "device_id": device['device_id'],
            "device_name": payload.get("identity", {}).get("device_name"),
            "client_ip": payload.get("network", {}).get("source_ip"),
            "last_seen": format_utc_timestamp(device['last_updated_ts']),
            "total_records": device['total_records'],
            'links': {
                'latest': f"{base_url}/data/latest/{device['device_id']}",
                'history': f"{base_url}/data/history?device_id={device['device_id']}&limit=50"
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
        for suffix in ["", "-wal", "-shm"]:
            filepath = DB_PATH + suffix
            if os.path.exists(filepath):
                size_bytes = os.path.getsize(filepath)
                db_files_info.append({
                    "file": os.path.basename(filepath),
                    "size": format_db_size(size_bytes),
                    "path": filepath
                })

        db_stats = {
            "total_processed_records": total_records['c'] if total_records else 0,
            "total_unique_devices": total_devices['c'] if total_devices else 0,
            "oldest_record_timestamp_utc": format_utc_timestamp(time_range['oldest']) if time_range else None,
            "newest_record_timestamp_utc": format_utc_timestamp(time_range['newest']) if time_range else None,
            "database_files": db_files_info
        }
    except aiosqlite.Error as e:
        db_stats = {"error": f"Could not query database statistics: {e}"}

    return {
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
