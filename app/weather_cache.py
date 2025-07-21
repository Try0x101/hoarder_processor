import os
import orjson
import time
import datetime
import asyncio
import aiofiles
from typing import Optional, Dict, Any, Tuple
from app.utils import calculate_distance_km

CACHE_DIR = "/tmp/weather_cache"
CACHE_DURATION_SECONDS = 3600
DISTANCE_THRESHOLD_KM = 1.0
MAX_CACHE_FILES = 100
MAX_CACHE_SIZE_MB = 50
_cleanup_lock = asyncio.Lock()

WEATHER_KEYS = {
    'temperature', 'humidity', 'apparent_temp', 'precipitation', 'code',
    'wind_speed', 'wind_direction', 'marine_wave_height',
    'marine_wave_direction', 'marine_wave_period'
}

def safe_ensure_cache_dir():
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        return True
    except OSError:
        return False

async def find_cached_weather(lat: float, lon: float) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not safe_ensure_cache_dir(): return None, None
    try:
        cache_files = await asyncio.to_thread(lambda: [os.path.join(CACHE_DIR, f) for f in os.listdir(CACHE_DIR) if f.endswith('.json')])
    except FileNotFoundError:
        return None, None

    for path in cache_files:
        try:
            if time.time() - os.path.getmtime(path) > CACHE_DURATION_SECONDS:
                continue
            async with aiofiles.open(path, 'rb') as f:
                content = await f.read()
            cached_data = orjson.loads(content)
            
            if calculate_distance_km(lat, lon, cached_data.get('_meta', {}).get('lat', 0), cached_data.get('_meta', {}).get('lon', 0)) <= DISTANCE_THRESHOLD_KM:
                weather_data = {k: v for k, v in cached_data.items() if k in WEATHER_KEYS}
                timestamp = cached_data.get('_meta', {}).get('cached_at')
                return weather_data, timestamp
        except (orjson.JSONDecodeError, KeyError, OSError, FileNotFoundError):
            continue
    return None, None

async def save_weather_to_cache(lat: float, lon: float, data: Dict[str, Any]) -> Optional[str]:
    if not safe_ensure_cache_dir(): return None
    
    key = f"{round(lat, 2)}_{round(lon, 2)}.json"
    filepath = os.path.join(CACHE_DIR, key)
    
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    cache_data = {k: v for k, v in data.items() if k in WEATHER_KEYS and v is not None}
    cache_data['_meta'] = {'lat': lat, 'lon': lon, 'cached_at': timestamp}
    
    try:
        async with aiofiles.open(filepath, 'wb') as f:
            await f.write(orjson.dumps(cache_data))
        await _enforce_cache_limits()
        return timestamp
    except (OSError, orjson.JSONEncodeError):
        return None

async def _enforce_cache_limits():
    async with _cleanup_lock:
        try:
            files = [os.path.join(CACHE_DIR, f) for f in os.listdir(CACHE_DIR) if f.endswith('.json')]
            if not files: return

            files_meta = [(p, os.path.getsize(p), os.path.getmtime(p)) for p in files]
            total_size_mb = sum(size for _, size, _ in files_meta) / (1024*1024)
            
            if len(files_meta) <= MAX_CACHE_FILES and total_size_mb <= MAX_CACHE_SIZE_MB: return
            
            files_meta.sort(key=lambda x: x[2])
            
            while len(files_meta) > MAX_CACHE_FILES or sum(f[1] for f in files_meta)/(1024*1024) > MAX_CACHE_SIZE_MB:
                if not files_meta: break
                os.remove(files_meta.pop(0)[0])
        except (OSError, FileNotFoundError):
            pass