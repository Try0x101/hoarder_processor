import aiosqlite
import os
import orjson
from fastapi import APIRouter, Request, HTTPException, Query
from typing import Optional, List, Dict, Any
from urllib.parse import quote_plus
from app.utils import diff_states, format_utc_timestamp, cleanup_empty, parse_freshness_payload, reconstruct_from_freshness

DB_FILE = "hoarder_processor.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", DB_FILE)

router = APIRouter(prefix="/data", tags=["Data Access"])

def build_base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"

async def query_recent_devices(limit: int = 10) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            query = """
                SELECT
                    les.device_id,
                    les.last_updated_ts,
                    les.enriched_payload,
                    COALESCE((SELECT COUNT(*) FROM enriched_telemetry et WHERE et.device_id = les.device_id), 0) as total_records,
                    COALESCE((SELECT SUM(et.request_size_bytes) FROM enriched_telemetry et WHERE et.device_id = les.device_id), 0) as total_bytes,
                    (SELECT MIN(et.calculated_event_timestamp) FROM enriched_telemetry et WHERE et.device_id = les.device_id) as first_seen_ts
                FROM latest_enriched_state les
                ORDER BY les.last_updated_ts DESC
                LIMIT ?
            """
            cursor = await db.execute(query, (limit,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        print(f"ERROR QUERYING RECENT DEVICES: {e}")
        return []

async def get_enriched_history(device_id: Optional[str], limit: int, cursor_ts: Optional[str], cursor_id: Optional[int]) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            query_parts = ["SELECT id, original_ingest_id, enriched_payload, calculated_event_timestamp FROM enriched_telemetry"]
            params = []
            conditions = []
            if device_id:
                conditions.append("device_id = ?")
                params.append(device_id)
            if cursor_ts and cursor_id is not None:
                conditions.append("(calculated_event_timestamp, id) < (?, ?)")
                params.extend([cursor_ts, cursor_id])
            if conditions:
                query_parts.append("WHERE " + " AND ".join(conditions))
            query_parts.append("ORDER BY calculated_event_timestamp DESC, id DESC LIMIT ?")
            params.append(limit + 1)
            
            cursor = await db.execute(" ".join(query_parts), tuple(params))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception:
        return []

@router.get("/history")
async def get_device_history(
    request: Request,
    device_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    cursor: Optional[str] = Query(None)
):
    base_url = build_base_url(request)
    cursor_ts, cursor_id = None, None
    if cursor:
        try:
            parts = cursor.split(',')
            if len(parts) == 2:
                cursor_ts, cursor_id = parts[0], int(parts[1])
        except (ValueError, IndexError):
            raise HTTPException(status_code=400, detail="Invalid cursor format.")

    history_rows = await get_enriched_history(device_id, limit, cursor_ts, cursor_id)
    
    records_to_process = history_rows[:limit]
    data_with_deltas = []
    if records_to_process:
        for i, current_row in enumerate(records_to_process):
            current_payload = orjson.loads(current_row['enriched_payload'])
            
            previous_payload = None
            previous_row_index = i + 1
            if previous_row_index < len(history_rows):
                previous_payload = orjson.loads(history_rows[previous_row_index]['enriched_payload'])
            
            changes = diff_states(current_payload, previous_payload) if previous_payload else current_payload

            current_diagnostics = current_payload.get("diagnostics", {})
            event_diagnostics = {
                "ingest_request_id": current_diagnostics.get("ingest_request_id"),
                "timestamps": current_diagnostics.get("timestamps"),
            }
            
            if "diagnostics" in changes:
                changes["diagnostics"].pop("ingest_request_id", None)
                changes["diagnostics"].pop("timestamps", None)
                if not changes["diagnostics"]:
                    changes.pop("diagnostics", None)

            data_with_deltas.append({
                "id": current_row['id'],
                "original_ingest_id": current_row.get('original_ingest_id'),
                "changes": cleanup_empty(changes),
                "diagnostics": cleanup_empty(event_diagnostics)
            })

    base_params = [f"limit={limit}"]
    if device_id: base_params.append(f"device_id={device_id}")

    self_params = base_params[:]
    if cursor: self_params.append(f"cursor={quote_plus(cursor)}")
    self_url = f"{base_url}/data/history?{'&'.join(self_params)}"

    navigation = {"root": f"{base_url}/"}
    if device_id:
        navigation["latest"] = f"{base_url}/data/latest/{device_id}"
    if cursor:
        navigation["first_page"] = f"{base_url}/data/history?{'&'.join(base_params)}"

    next_cursor_obj = None
    if len(history_rows) > limit:
        last_record = history_rows[limit-1]
        next_ts, next_id = last_record['calculated_event_timestamp'], last_record['id']
        raw_cursor = f"{next_ts},{next_id}"
        navigation["next_page"] = f"{base_url}/data/history?{'&'.join(base_params + [f'cursor={quote_plus(raw_cursor)}'])}"
        next_cursor_obj = {
            "raw": raw_cursor,
            "timestamp": format_utc_timestamp(next_ts),
            "id": next_id
        }
    
    time_range = {}
    if records_to_process:
        start_ts = records_to_process[0]['calculated_event_timestamp']
        end_ts = records_to_process[-1]['calculated_event_timestamp']
        time_range = {"start": format_utc_timestamp(start_ts), "end": format_utc_timestamp(end_ts)}

    pagination = {
        "limit": limit,
        "records_returned": len(data_with_deltas),
        "next_cursor": next_cursor_obj,
        "time_range": time_range
    }

    return {
        "request": {"self_url": self_url},
        "navigation": navigation,
        "pagination": cleanup_empty(pagination),
        "data": data_with_deltas
    }

@router.get("/latest/{device_id}")
async def get_latest_device_data(request: Request, device_id: str):
    base_url = build_base_url(request)
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT enriched_payload FROM latest_enriched_state WHERE device_id = ?", (device_id,))
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"No state found for device '{device_id}'.")
            
            freshness_payload = orjson.loads(row['enriched_payload'])
            data_payload, freshness_info = parse_freshness_payload(freshness_payload)

            if "diagnostics" not in data_payload:
                data_payload["diagnostics"] = {}
            data_payload["diagnostics"]["data_freshness"] = cleanup_empty(freshness_info)
            
            return {
                "request": {"self_url": f"{base_url}/data/latest/{device_id}"},
                "navigation": {
                    "root": f"{base_url}/",
                    "history": f"{base_url}/data/history?device_id={device_id}&limit=50"
                },
                "data": data_payload
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve latest data: {e}")

@router.get("/devices")
async def get_devices_endpoint(request: Request, limit: int = 20):
    base_url = build_base_url(request)
    devices = await query_recent_devices(limit=limit)
    
    processed_devices = []
    for device in devices:
        freshness_payload = orjson.loads(device['enriched_payload'])
        payload = reconstruct_from_freshness(freshness_payload)
        processed_devices.append({
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

    self_url = f"{base_url}/data/devices?limit={limit}"
    return {
        "request": {"self_url": self_url},
        "navigation": {"root": f"{base_url}/"},
        "data": processed_devices
    }
