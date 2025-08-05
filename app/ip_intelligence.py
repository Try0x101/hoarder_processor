import httpx
import orjson
import redis.asyncio as redis
from typing import Optional, Dict, Any
from app.utils import ip_api_breaker
from app.database import REDIS_DB_IP_INTEL

IP_INTEL_CACHE_TTL_SECONDS = 24 * 3600  # Cache for 24 hours
API_ENDPOINT = "http://ip-api.com/json/{ip}?fields=status,message,country,regionName,city,zip,lat,lon,timezone,isp,org,as,proxy,hosting,query"

async def _fetch_from_api(ip_address: str) -> Optional[Dict[str, Any]]:
    """Internal function to fetch data from the ip-api.com API."""
    async with httpx.AsyncClient(timeout=3.0) as client:
        response = await client.get(API_ENDPOINT.format(ip=ip_address))
        response.raise_for_status()
        return response.json()

async def get_ip_intelligence(redis_client: redis.Redis, ip_address: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves IP intelligence data for a given IP address, using a cache to avoid redundant lookups.
    """
    if not ip_address or not isinstance(ip_address, str):
        return None

    cache_key = f"ip_intel:{ip_address}"
    
    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return orjson.loads(cached_data)

        # If not in cache, call the API via the circuit breaker
        api_data = await ip_api_breaker.call(_fetch_from_api, ip_address)

        if api_data and api_data.get("status") == "success":
            await redis_client.set(cache_key, orjson.dumps(api_data), ex=IP_INTEL_CACHE_TTL_SECONDS)
            return api_data
        
    except Exception:
        # If any error occurs (Redis, HTTP, etc.), we fail silently
        # to avoid halting the entire processing pipeline.
        return None
    
    return None