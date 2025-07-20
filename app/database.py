import aiosqlite
import os
import orjson
import redis.asyncio as redis
import datetime
from typing import List, Dict, Any, Optional

DB_FILE = "hoarder_processor.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", DB_FILE)

async def get_device_weather_state(redis_client: redis.Redis, device_id: str) -> Optional[Dict[str, Any]]:
    try:
        state = await redis_client.hgetall(f"weather_state:{device_id}")
        if state and "lat" in state and "lon" in state and "ts" in state:
            return {
                "lat": float(state["lat"]),
                "lon": float(state["lon"]),
                "ts": float(state["ts"]),
            }
    except (redis.RedisError, ValueError, TypeError):
        return None
    return None

async def save_device_weather_state(redis_client: redis.Redis, device_id: str, lat: float, lon: float):
    try:
        key = f"weather_state:{device_id}"
        await redis_client.hset(key, mapping={
            "lat": lat,
            "lon": lon,
            "ts": datetime.datetime.now(datetime.timezone.utc).timestamp()
        })
        await redis_client.expire(key, 3600 * 24 * 7)
    except redis.RedisError:
        pass

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
                await db.execute(
                    "INSERT OR IGNORE INTO enriched_telemetry (original_ingest_id, device_id, enriched_payload, calculated_event_timestamp) VALUES (?, ?, ?, ?)",
                    (
                        record.get("original_ingest_id"),
                        record.get("device_id"),
                        orjson.dumps(record.get("enriched_payload")).decode(),
                        record.get("calculated_event_timestamp")
                    )
                )
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
