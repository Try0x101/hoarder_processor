import aiosqlite
import os
import orjson
import redis.asyncio as redis
import datetime
from typing import List, Dict, Any, Optional, Tuple

DB_FILE = "hoarder_processor.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", DB_FILE)

DEVICE_POSITION_KEY_PREFIX = "device:position"
DEVICE_POSITION_TTL_SECONDS = 30 * 24 * 3600

def _get_redis_position_key(device_id: str) -> str:
    return f"{DEVICE_POSITION_KEY_PREFIX}:{device_id}"

async def get_device_position(redis_client: redis.Redis, device_id: str) -> Optional[Dict[str, Any]]:
    if not redis_client:
        return None
    redis_key = _get_redis_position_key(device_id)
    try:
        pos_data = await redis_client.hgetall(redis_key)
        if not pos_data:
            return None

        typed_pos_data = {}
        for key, value in pos_data.items():
            if key in ['lat', 'lon']:
                try: typed_pos_data[key] = float(value)
                except (ValueError, TypeError): typed_pos_data[key] = None
            elif key == 'weather_update_count':
                try: typed_pos_data[key] = int(value)
                except (ValueError, TypeError): typed_pos_data[key] = 0
            else:
                typed_pos_data[key] = value
        return typed_pos_data
    except redis.RedisError:
        return None

async def save_device_position(redis_client: redis.Redis, device_id: str, position_data: Dict[str, Any]):
    if not redis_client:
        return
    redis_key = _get_redis_position_key(device_id)
    try:
        save_data = {k: v for k, v in position_data.items() if v is not None}
        if not save_data:
            return

        async with redis_client.pipeline(transaction=True) as pipe:
            await pipe.hset(redis_key, mapping=save_data)
            await pipe.expire(redis_key, DEVICE_POSITION_TTL_SECONDS)
            await pipe.execute()
    except redis.RedisError:
        pass

async def get_latest_state_for_device(conn: aiosqlite.Connection, device_id: str) -> Optional[Tuple[Dict[str, Any], str]]:
    conn.row_factory = aiosqlite.Row
    cursor = await conn.execute("SELECT enriched_payload, last_updated_ts FROM latest_enriched_state WHERE device_id = ?", (device_id,))
    row = await cursor.fetchone()
    if row and row["enriched_payload"] and row["last_updated_ts"]:
        try:
            return orjson.loads(row["enriched_payload"]), row["last_updated_ts"]
        except orjson.JSONDecodeError:
            return None, None
    return None, None

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
