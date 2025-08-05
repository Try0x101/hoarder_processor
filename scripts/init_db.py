import sqlite3
import os
import sys
import re
import urllib.request

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app.database import DB_PATH

OUI_URL = "http://standards-oui.ieee.org/oui.txt"

def update_oui_database(cur: sqlite3.Cursor):
    print("Updating OUI vendor database...")
    try:
        with urllib.request.urlopen(OUI_URL, timeout=20) as response:
            if response.status != 200:
                print(f"WARNING: Failed to download OUI file. Status: {response.status}")
                return
            oui_data = response.read().decode('utf-8')
    except Exception as e:
        print(f"WARNING: Could not download OUI file: {e}")
        return

    oui_pattern = re.compile(r"^([0-9A-F]{2}-[0-9A-F]{2}-[0-9A-F]{2})\s+\(hex\)\s+(.*)$")
    vendors = []
    for line in oui_data.splitlines():
        match = oui_pattern.match(line)
        if match:
            oui = match.group(1).replace("-", "").upper()
            vendor = match.group(2).strip()
            vendors.append((oui, vendor))

    if not vendors:
        print("WARNING: OUI data could not be parsed. Vendor lookups will be unavailable.")
        return

    print(f"Found {len(vendors)} OUI vendor entries. Populating database...")
    cur.execute("DELETE FROM oui_vendors")
    cur.executemany("INSERT OR REPLACE INTO oui_vendors (oui, vendor) VALUES (?, ?)", vendors)
    print("OUI vendor database update complete.")

def initialize_database():
    try:
        print(f"Initializing database. Absolute DB path: {DB_PATH}")
        if not os.path.exists(os.path.dirname(DB_PATH)):
            print(f"CRITICAL: DB directory does not exist: {os.path.dirname(DB_PATH)}", file=sys.stderr)
            sys.exit(1)
            
        con = sqlite3.connect(DB_PATH, timeout=20.0)
        cur = con.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS enriched_telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_ingest_id INTEGER NOT NULL,
                device_id TEXT NOT NULL,
                enriched_payload TEXT NOT NULL,
                calculated_event_timestamp TEXT NOT NULL,
                request_size_bytes INTEGER NOT NULL DEFAULT 0,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(original_ingest_id)
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS oui_vendors (
                oui TEXT PRIMARY KEY,
                vendor TEXT NOT NULL
            )
        """)
        con.commit()

        print("Checking for 'request_size_bytes' column in enriched_telemetry table...")
        cur.execute("PRAGMA table_info(enriched_telemetry)")
        columns = [row[1] for row in cur.fetchall()]
        
        if 'request_size_bytes' not in columns:
            print("Column 'request_size_bytes' not found. Attempting to add it now...")
            try:
                cur.execute("ALTER TABLE enriched_telemetry ADD COLUMN request_size_bytes INTEGER NOT NULL DEFAULT 0")
                con.commit()
                print("Column 'request_size_bytes' was added successfully.")
            except Exception as alter_e:
                print(f"CRITICAL: Failed to add column to table: {alter_e}", file=sys.stderr)
        else:
            print("Column 'request_size_bytes' already exists. No changes needed.")

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
        
        update_oui_database(cur)

        con.commit()
        con.close()
        print("Database initialization tasks completed.")
    except Exception as e:
        print(f"CRITICAL: A fatal error occurred during database initialization: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    initialize_database()