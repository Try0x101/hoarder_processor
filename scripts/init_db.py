import sqlite3
import os
import sys

DB_FILE = "hoarder_processor.db"
DB_PATH = os.path.join(os.path.dirname(__file__), "..", DB_FILE)

def initialize_database():
    try:
        print(f"Initializing database at: {DB_PATH}")
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS enriched_telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_ingest_id INTEGER NOT NULL,
                device_id TEXT NOT NULL,
                enriched_payload TEXT NOT NULL,
                calculated_event_timestamp TEXT NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(original_ingest_id)
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS latest_enriched_state (
                device_id TEXT PRIMARY KEY,
                enriched_payload TEXT NOT NULL,
                last_updated_ts TEXT NOT NULL
            )
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_enriched_device_event_time ON enriched_telemetry (device_id, calculated_event_timestamp DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_enriched_event_time ON enriched_telemetry (calculated_event_timestamp DESC)")

        con.commit()
        con.close()
        print("Database initialized successfully.")
    except Exception as e:
        print(f"CRITICAL: An error occurred during database initialization: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    initialize_database()
