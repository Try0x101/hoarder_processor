import aiosqlite
import os
import orjson
from typing import List, Dict, Any, Optional

DB_FILE = "hoarder_processor.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", DB_FILE)

async def get_latest_state_for_device(conn: aiosqlite.Connection, device_id: str) -> Optional[Dict[str, Any]]:
    conn.row_factory = aiosqlite.Row
    cursor = await conn.execute("SELECT enriched_payload FROM latest_enriched_state WHERE device_id = ?", (device_id,))
    row = await cursor.fetchone()
    if row:
        return orjson.loads(row["enriched_payload"])
    return None

async def save_stateful_data(records: List[Dict[str, Any]]):
    if not records:
        return

    try:
        async with aiosqlite.connect(DB_PATH, timeout=30) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            
            for record in records:
                # Log the historical event
                await db.execute(
                    "INSERT OR IGNORE INTO enriched_telemetry (original_ingest_id, device_id, enriched_payload, calculated_event_timestamp) VALUES (?, ?, ?, ?)",
                    (
                        record.get("original_ingest_id"),
                        record.get("device_id"),
                        orjson.dumps(record.get("enriched_payload")).decode(),
                        record.get("calculated_event_timestamp")
                    )
                )
                # Update the latest state view
                await db.execute(
                    "INSERT INTO latest_enriched_state (device_id, enriched_payload, last_updated_ts) VALUES (?, ?, ?) ON CONFLICT(device_id) DO UPDATE SET enriched_payload=excluded.enriched_payload, last_updated_ts=excluded.last_updated_ts",
                    (
                        record.get("device_id"),
                        orjson.dumps(record.get("enriched_payload")).decode(),
                        record.get("calculated_event_timestamp")
                    )
                )
            await db.commit()
    except Exception as e:
        print(f"ERROR saving stateful data: {e}")
        raise
