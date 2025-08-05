import aiosqlite
import os
import orjson
import redis.asyncio as redis
import redis as sync_redis
import datetime
import sqlite3
import re
import urllib.request
from typing import List, Dict, Any, Optional, Tuple

DB_FILE = "hoarder_processor.db"
DB_PATH = "/opt/hoarder_processor/hoarder_processor.db"
OUI_URL = "https://standards-oui.ieee.org/oui/oui.txt"

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS enriched_telemetry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_ingest_id INTEGER NOT NULL,
    device_id TEXT NOT NULL,
    enriched_payload TEXT NOT NULL,
    calculated_event_timestamp TEXT NOT NULL,
    request_size_bytes INTEGER NOT NULL DEFAULT 0,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(original_ingest_id)
);
CREATE TABLE IF NOT EXISTS latest_enriched_state (
    device_id TEXT PRIMARY KEY,
    enriched_payload TEXT NOT NULL,
    last_updated_ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS oui_vendors (
    oui TEXT PRIMARY KEY,
    vendor TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_enriched_device_event_time ON enriched_telemetry (device_id, calculated_event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_enriched_event_time ON enriched_telemetry (calculated_event_timestamp DESC);
"""

def initialize_database_if_needed():
    try:
        con = sqlite3.connect(DB_PATH, timeout=30.0)
        cur = con.cursor()
        cur.executescript(DB_SCHEMA)
        con.commit()
        
        cur.execute("SELECT COUNT(*) FROM oui_vendors")
        if cur.fetchone()[0] > 0:
            con.close()
            return

        print("OUI table is empty. Populating now...")
        req = urllib.request.Request(
            OUI_URL,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.status != 200:
                return
            oui_data = response.read().decode('utf-8')

        oui_pattern = re.compile(r"^([0-9A-F]{2}-[0-9A-F]{2}-[0-9A-F]{2})\s+\(hex\)\s+(.*)$")
        vendors = [
            (match.group(1).replace("-", "").upper(), match.group(2).strip())
            for line in oui_data.splitlines()
            if (match := oui_pattern.match(line))
        ]
        
        if vendors:
            cur.execute("BEGIN")
            cur.executemany("INSERT OR REPLACE INTO oui_vendors (oui, vendor) VALUES (?, ?)", vendors)
            con.commit()
            print(f"Successfully populated OUI table with {len(vendors)} vendors.")
    except Exception as e:
        print(f"CRITICAL: An error occurred during database initialization: {e}")
    finally:
        if 'con' in locals() and con:
            con.close()

REDIS_SENTINEL_HOSTS = [('localhost', 26379), ('localhost', 26380), ('localhost', 26381)]
REDIS_MASTER_NAME = "mymaster"
CELERY_DB_BROKER = 2
CELERY_DB_BACKEND = 3
REDIS_DB_POSITION = 4
REDIS_DB_METRICS = 5
REDIS_DB_IP_INTEL = 6

DEVICE_POSITION_KEY_PREFIX = "device:position"
DEVICE_POSITION_TTL_SECONDS = 30 * 24 * 3600
DEVICE_BATCH_TS_KEY_PREFIX = "device:batch_ts"
DEVICE_BATCH_TS_TTL_SECONDS = 6 * 3600
CELLULAR_ANALYSIS_KEY_PREFIX = "cellular:analysis"
CELLULAR_ANALYSIS_TTL_SECONDS = 30 * 24 * 3600

async def ensure_db_initialized(conn: aiosqlite.Connection):
    await conn.executescript(DB_SCHEMA)
    await conn.commit()

def _get_redis_position_key(device_id: str) -> str:
    return f"{DEVICE_POSITION_KEY_PREFIX}:{device_id}"

def _get_redis_batch_ts_key(device_id: str) -> str:
    return f"{DEVICE_BATCH_TS_KEY_PREFIX}:{device_id}"

def _get_redis_cellular_analysis_key(device_id: str) -> str:
    return f"{CELLULAR_ANALYSIS_KEY_PREFIX}:{device_id}"

async def get_all_oui_vendors(conn: aiosqlite.Connection) -> Dict[str, str]:
    conn.row_factory = aiosqlite.Row
    cursor = await conn.execute("SELECT oui, vendor FROM oui_vendors")
    rows = await cursor.fetchall()
    return {row["oui"]: row["vendor"] for row in rows}

async def get_device_batch_ts(redis_client: redis.Redis, device_id: str) -> Optional[int]:
    redis_key = _get_redis_batch_ts_key(device_id)
    try:
        ts = await redis_client.get(redis_key)
        return int(ts) if ts else None
    except (redis.RedisError, ValueError, TypeError):
        return None

async def save_device_batch_ts(redis_client: redis.Redis, device_id: str, ts: int):
    redis_key = _get_redis_batch_ts_key(device_id)
    try:
        await redis_client.set(redis_key, ts, ex=DEVICE_BATCH_TS_TTL_SECONDS)
    except redis.RedisError:
        pass

async def delete_device_batch_ts(redis_client: redis.Redis, device_id: str):
    redis_key = _get_redis_batch_ts_key(device_id)
    try:
        await redis_client.delete(redis_key)
    except redis.RedisError:
        pass

async def get_cellular_analysis_state(redis_client: redis.Redis, device_id: str) -> Optional[Dict[str, Any]]:
    redis_key = _get_redis_cellular_analysis_key(device_id)
    try:
        state_data = await redis_client.get(redis_key)
        return orjson.loads(state_data) if state_data else None
    except (redis.RedisError, orjson.JSONDecodeError):
        return None

async def save_cellular_analysis_state(redis_client: redis.Redis, device_id: str, state: Dict[str, Any]):
    redis_key = _get_redis_cellular_analysis_key(device_id)
    try:
        await redis_client.set(redis_key, orjson.dumps(state), ex=CELLULAR_ANALYSIS_TTL_SECONDS)
    except redis.RedisError:
        pass

async def get_device_position(redis_client: redis.Redis, device_id: str) -> Optional[Dict[str, Any]]:
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

    telemetry_to_save = [
        (
            r.get("original_ingest_id"),
            r.get("device_id"),
            orjson.dumps(r.get("historical_payload")).decode(),
            r.get("calculated_event_timestamp"),
            r.get("request_size_bytes"),
        )
        for r in records
    ]
    latest_state_to_save = [
        (
            r.get("device_id"),
            orjson.dumps(r.get("latest_payload")).decode(),
            r.get("calculated_event_timestamp"),
        )
        for r in records
    ]

    try:
        async with aiosqlite.connect(DB_PATH, timeout=30) as db:
            await ensure_db_initialized(db)
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")

            await db.executemany(
                "INSERT OR IGNORE INTO enriched_telemetry (original_ingest_id, device_id, enriched_payload, calculated_event_timestamp, request_size_bytes) VALUES (?, ?, ?, ?, ?)",
                telemetry_to_save,
            )
            await db.executemany(
                """INSERT INTO latest_enriched_state (device_id, enriched_payload, last_updated_ts) 
                   VALUES (?, ?, ?) 
                   ON CONFLICT(device_id) 
                   DO UPDATE SET 
                       enriched_payload=excluded.enriched_payload, 
                       last_updated_ts=excluded.last_updated_ts
                   WHERE excluded.last_updated_ts > latest_enriched_state.last_updated_ts""",
                latest_state_to_save,
            )
            await db.commit()
    except Exception as e:
        print(f"ERROR saving stateful data: {e}")
        raise
