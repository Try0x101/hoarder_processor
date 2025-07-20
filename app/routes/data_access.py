import aiosqlite
import os
import orjson
from fastapi import APIRouter, Request, HTTPException, Query
from typing import Optional, List, Dict, Any
from urllib.parse import quote_plus
from app.utils import diff_states, format_utc_timestamp

DB_FILE = "hoarder_processor.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", DB_FILE)

router = APIRouter(prefix="/data", tags=["Data Access"])

def build_base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"

async def query_recent_devices(limit: int = 10) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT device_id, last_updated_ts FROM latest_enriched_state ORDER BY last_updated_ts DESC LIMIT ?", (limit,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception:
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
    
    data_with_deltas = []
    if history_rows:
        records_to_process = history_rows[:limit]
        
        for i, current_row in enumerate(records_to_process):
            current_payload = orjson.loads(current_row['enriched_payload'])
            previous_row_index = i + 1
            
            if previous_row_index < len(history_rows):
                previous_payload = orjson.loads(history_rows[previous_row_index]['enriched_payload'])
                changes = diff_states(current_payload, previous_payload)
            else:
                changes = current_payload

            diagnostics = current_payload.get("diagnostics", {})
            if "diagnostics" in changes:
                del changes["diagnostics"]

            data_with_deltas.append({
                "id": current_row['id'],
                "original_ingest_id": current_row.get('original_ingest_id'),
                "changes": changes,
                "diagnostics": diagnostics
            })

    next_cursor_str = None
    if len(history_rows) > limit:
        last_record_on_page = history_rows[limit-1]
        next_cursor_str = f"{last_record_on_page['calculated_event_timestamp']},{last_record_on_page['id']}"

    base_params = [f"limit={limit}"]
    if device_id: base_params.append(f"device_id={device_id}")
    self_params = base_params[:]
    if cursor: self_params.append(f"cursor={quote_plus(cursor)}")
    self_url = f"{base_url}/data/history?{'&'.join(self_params)}"
    navigation = {"self": self_url, "root": f"{base_url}/"}
    if next_cursor_str:
        navigation["next_page"] = f"{base_url}/data/history?{'&'.join(base_params + [f'cursor={quote_plus(next_cursor_str)}'])}"
    if cursor:
        navigation["first_page"] = f"{base_url}/data/history?{'&'.join(base_params)}"

    return {
        "navigation": navigation,
        "pagination": {"limit": limit, "records_returned": len(data_with_deltas), "next_cursor": next_cursor_str},
        "data": data_with_deltas
    }

@router.get("/latest/{device_id}")
async def get_latest_device_data(device_id: str):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT enriched_payload FROM latest_enriched_state WHERE device_id = ?", (device_id,))
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"No state found for device '{device_id}'.")
            return orjson.loads(row['enriched_payload'])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve latest data: {e}")

@router.get("/devices")
async def get_devices_endpoint(request: Request, limit: int = 20):
    base_url = build_base_url(request)
    devices = await query_recent_devices(limit=limit)
    for device in devices:
        device['last_updated_ts'] = format_utc_timestamp(device['last_updated_ts'])
        device['links'] = {
            'latest': f"{base_url}/data/latest/{device['device_id']}",
            'history': f"{base_url}/data/history?device_id={device['device_id']}"
        }
    return devices
