import sqlite3
import os
import sys
import re
import urllib.request

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from app.database import DB_PATH

OUI_URL = "https://standards-oui.ieee.org/oui/oui.txt"

def main():
    print(f"Connecting to database: {DB_PATH}")
    try:
        con = sqlite3.connect(DB_PATH, timeout=20.0)
        cur = con.cursor()
    except Exception as e:
        print(f"CRITICAL: Failed to connect to database: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Attempting to download OUI list from {OUI_URL}...")
    try:
        # Add a common browser User-Agent header to avoid being blocked
        req = urllib.request.Request(
            OUI_URL,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.status != 200:
                print(f"ERROR: Failed to download OUI file. Server returned status: {response.status}")
                sys.exit(1)
            oui_data = response.read().decode('utf-8')
            print("Download successful.")
    except Exception as e:
        print(f"ERROR: Could not download OUI file. Please check network connectivity and firewall rules. Details: {e}")
        sys.exit(1)

    oui_pattern = re.compile(r"^([0-9A-F]{2}-[0-9A-F]{2}-[0-9A-F]{2})\s+\(hex\)\s+(.*)$")
    vendors = []
    for line in oui_data.splitlines():
        match = oui_pattern.match(line)
        if match:
            oui = match.group(1).replace("-", "").upper()
            vendor = match.group(2).strip()
            vendors.append((oui, vendor))

    if not vendors:
        print("ERROR: OUI data was downloaded but could not be parsed. The format may have changed.")
        sys.exit(1)

    print(f"Found {len(vendors)} OUI vendor entries. Populating database...")
    try:
        cur.execute("BEGIN")
        cur.execute("DELETE FROM oui_vendors")
        cur.executemany("INSERT OR REPLACE INTO oui_vendors (oui, vendor) VALUES (?, ?)", vendors)
        con.commit()
        print("OUI vendor database update complete.")
    except Exception as e:
        print(f"ERROR: Failed to write to database: {e}")
        con.rollback()
        sys.exit(1)
    finally:
        con.close()

if __name__ == "__main__":
    main()
