import aiosqlite
import asyncio
import os
from . import settings, converter, writer
from app.database import ensure_db_initialized

async def fetch_latest_records(conn: aiosqlite.Connection, offset: int):
    conn.row_factory = aiosqlite.Row
    query = """
        SELECT id, enriched_payload, calculated_event_timestamp
        FROM enriched_telemetry
        ORDER BY id DESC
        LIMIT ?
        OFFSET ?
    """
    cursor = await conn.execute(query, (settings.QUERY_BATCH_SIZE, offset))
    return await cursor.fetchall()

async def run_processor_once():
    print("GEOJSON: Starting snapshot generation.")
    
    manager = writer.GeoJSONManager()
    
    try:
        await manager.start_writing()
        
        async with aiosqlite.connect(settings.DB_PATH, timeout=30) as db:
            await ensure_db_initialized(db)
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            
            offset = 0
            total_features_written = 0

            while True:
                records = await fetch_latest_records(db, offset)
                if not records:
                    print("GEOJSON: No more records to process.")
                    break

                features = []
                for row in records:
                    feature = converter.process_row_to_geojson(dict(row))
                    if feature:
                        features.append(feature)
                
                if features:
                    await manager.write_features(features)
                    total_features_written += len(features)

                current_size = 0
                if manager.temp_path and os.path.exists(manager.temp_path):
                    current_size = os.path.getsize(manager.temp_path)
                
                print(f"GEOJSON: Written {len(features)} features. Total: {total_features_written}. File size: {current_size // 1024}KB.")

                if current_size >= settings.MAX_FILE_SIZE_BYTES:
                    print(f"GEOJSON: File size limit ({settings.MAX_FILE_SIZE_BYTES // (1024*1024)}MB) reached. Stopping.")
                    break
                
                if len(records) < settings.QUERY_BATCH_SIZE:
                    break
                
                offset += settings.QUERY_BATCH_SIZE

    except aiosqlite.Error as e:
        print(f"GeoJSON Processor DB Error: {e}")
    except Exception as e:
        print(f"GeoJSON Processor Error: {e}")
    finally:
        await manager.finalize()
        print("GEOJSON: Run finished.")
