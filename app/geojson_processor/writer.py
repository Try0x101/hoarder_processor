import os
import orjson
import aiofiles
import datetime
from typing import Dict, Any, Tuple, Optional
from . import settings

def find_latest_file() -> Optional[str]:
    try:
        files = [os.path.join(settings.OUTPUT_DIR, f) for f in os.listdir(settings.OUTPUT_DIR) if f.startswith("hoarder_") and f.endswith(".geojson")]
        if not files:
            return None
        return max(files, key=os.path.getctime)
    except (FileNotFoundError, OSError):
        return None

async def load_or_create_feature_collection(path: Optional[str]) -> Tuple[Dict, str]:
    if path and os.path.getsize(path) < settings.MAX_FILE_SIZE_BYTES:
        try:
            async with aiofiles.open(path, "rb") as f:
                content = await f.read()
            if content:
                return orjson.loads(content), path
        except (IOError, orjson.JSONDecodeError):
            pass
    
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    new_path = os.path.join(settings.OUTPUT_DIR, f"hoarder_{timestamp}.geojson")
    collection = {"type": "FeatureCollection", "features": []}
    return collection, new_path

async def save_feature_collection(path: str, collection: Dict[str, Any]):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        content = orjson.dumps(collection)
        async with aiofiles.open(path, "wb") as f:
            await f.write(content)
    except (IOError, orjson.JSONEncodeError):
        pass
