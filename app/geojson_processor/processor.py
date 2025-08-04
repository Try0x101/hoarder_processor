import aiosqlite
import asyncio
import os
from . import settings, state, converter, writer
from app.database import ensure_db_initialized

async def fetch_new_records(conn: aiosqlite.Connection, last_id: int):
    conn.row_factory = aiosqlite.Row
    query = """
        SELECT id, enriched_payload, calculated_event_timestamp
        FROM enriched_telemetry
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
    """
    cursor = await conn.execute(query, (last_id, settings.QUERY_BATCH_SIZE))
    return await cursor.fetchall()

async def run_processor_once():
    last_processed_id = await state.load_last_processed_id()
    print(f"GEOJSON: Starting run. Last processed ID: {last_processed_id}")
    
    manager = writer.GeoJSONManager()
    
    try:
        await manager.start_writing()
        
        async with aiosqlite.connect(settings.DB_PATH, timeout=30) as db:
            await ensure_db_initialized(db)
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            
            records = await fetch_new_records(db, last_processed_id)
            if not records:
                cursor = await db.execute("SELECT MAX(id) FROM enriched_telemetry")
                max_db_id_row = await cursor.fetchone()
                max_db_id = max_db_id_row[0] if max_db_id_row and max_db_id_row[0] is not None else 0
                
                if last_processed_id > 0 and max_db_id < last_processed_id:
                    print(f"GEOJSON: Stale state detected. Max DB ID ({max_db_id}) is less than last processed ID ({last_processed_id}). Resetting state.")
                    await state.save_last_processed_id(0)
                    last_processed_id = 0
                    records = await fetch_new_records(db, last_processed_id)

            if not records:
                print("GEOJSON: No new records found.")
            else:
                while records:
                    print(f"GEOJSON: Fetched {len(records)} new records.")
                    features = []
                    for row in records:
                        feature = converter.process_row_to_geojson(dict(row))
                        if feature:
                            features.append(feature)
                    
                    if features:
                        await manager.write_features(features)
                    
                    new_last_id = records[-1]['id']
                    await state.save_last_processed_id(new_last_id)
                    last_processed_id = new_last_id
                    print(f"GEOJSON: Advanced to last processed ID: {last_processed_id}")
                    
                    if len(records) < settings.QUERY_BATCH_SIZE:
                        break 
                    records = await fetch_new_records(db, last_processed_id)

    except aiosqlite.Error as e:
        print(f"GeoJSON Processor DB Error: {e}")
    except Exception as e:
        print(f"GeoJSON Processor Error: {e}")
    finally:
        await manager.finalize()
        print("GEOJSON: Run finished.")
