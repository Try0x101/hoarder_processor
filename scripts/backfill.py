import os
import httpx
import time
from app.tasks import process_and_store_data

INGEST_API_BASE_URL = os.environ.get("INGEST_API_BASE_URL")

def fetch_and_queue_history():
    if not INGEST_API_BASE_URL:
        print("ERROR: INGEST_API_BASE_URL environment variable is not set.")
        print("Please set it to the full URL of the hoarder_ingest server, e.g., http://hoarder-ingest:8000")
        return

    print(f"Starting historical backfill from: {INGEST_API_BASE_URL}")

    page_count = 0
    records_processed = 0
    cursor = None
    
    with httpx.Client(timeout=30.0) as client:
        while True:
            page_count += 1
            url = f"{INGEST_API_BASE_URL}/data/history?limit=500"
            if cursor:
                url += f"&cursor={cursor}"

            try:
                print(f"Fetching page {page_count}...")
                response = client.get(url)
                response.raise_for_status()
                data = response.json()
            except httpx.RequestError as e:
                print(f"ERROR: Could not connect to ingest server: {e}")
                break
            except httpx.HTTPStatusError as e:
                print(f"ERROR: Ingest server returned an error: {e.response.status_code} - {e.response.text}")
                break
            
            records = data.get("data")
            if not records:
                print("No more records found. Backfill complete.")
                break

            process_and_store_data.delay(records)
            records_processed += len(records)
            print(f"  Queued {len(records)} records for processing. Total so far: {records_processed}")
            
            pagination = data.get("pagination", {})
            next_cursor = pagination.get("next_cursor")
            
            if not next_cursor:
                print("Last page reached. Backfill complete.")
                break
            
            cursor = next_cursor.get("raw") if isinstance(next_cursor, dict) else next_cursor
            if not cursor:
                print("Could not determine next cursor. Backfill complete.")
                break

            time.sleep(0.5)

    print(f"\nFinished. Total pages fetched: {page_count}. Total records queued: {records_processed}.")
    print("Run the celery worker to process the queued data.")

if __name__ == "__main__":
    fetch_and_queue_history()
