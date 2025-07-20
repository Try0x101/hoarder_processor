import asyncio
import aiosqlite
from typing import List, Dict, Any
from celery_app import celery_app
from app.processing import enrich_record
from app.database import get_latest_state_for_device, save_stateful_data, DB_PATH
from app.utils import deep_merge

async def _process_and_store_statefully(records: List[Dict[str, Any]]):
    records_to_save = []
    
    # Sort records by timestamp to process deltas in order
    sorted_records = sorted(records, key=lambda r: r.get('calculated_event_timestamp', ''))

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            for record in sorted_records:
                device_id = record.get("device_id")
                if not device_id:
                    continue

                # 1. Enrich the new partial data
                partial_enrichment = await enrich_record(record)
                if not partial_enrichment:
                    continue
                
                new_data = partial_enrichment["enriched_payload"]

                # 2. Get the last known full state
                base_state = await get_latest_state_for_device(db, device_id) or {}
                
                # 3. Merge new data onto the old state
                merged_state = deep_merge(new_data, base_state)
                
                # Prepare the final record for saving
                final_record = {
                    "original_ingest_id": partial_enrichment["original_ingest_id"],
                    "device_id": device_id,
                    "enriched_payload": merged_state,
                    "calculated_event_timestamp": partial_enrichment["calculated_event_timestamp"],
                }
                records_to_save.append(final_record)

        if records_to_save:
            await save_stateful_data(records_to_save)

    except Exception as e:
        print(f"Error in stateful processing task: {e}")


@celery_app.task(name="processor.process_and_store_data")
def process_and_store_data(records: List[Dict[str, Any]]):
    if not records:
        return
    asyncio.run(_process_and_store_statefully(records))
