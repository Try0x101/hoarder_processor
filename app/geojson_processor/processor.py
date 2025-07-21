import aiosqlite
import asyncio
from . import settings, state, converter, writer

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
    
    manager = writer.GeoJSONManager()
    
    try:
        await manager.start_writing()
        
        async with aiosqlite.connect(f"file:{settings.DB_PATH}?mode=ro", uri=True) as db:
            while True:
                records = await fetch_new_records(db, last_processed_id)
                if not records:
                    break

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

    except aiosqlite.Error as e:
        print(f"GeoJSON Processor DB Error: {e}")
    except Exception as e:
        print(f"GeoJSON Processor Error: {e}")
    finally:
        await manager.finalize()
