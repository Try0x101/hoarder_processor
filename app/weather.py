import httpx
import asyncio
import datetime
from typing import Optional, Dict, Any

from app.utils import weather_breaker, wttr_breaker, calculate_distance_km
from app.database import get_device_position, save_device_position

MOVEMENT_THRESHOLD_KM = 1.0
WEATHER_EXPIRATION_SECONDS = 3600

async def fetch_openmeteo_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    api_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat, "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,wind_direction_10m",
        "timezone": "UTC"
    }
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.get(api_url, params=params)
        response.raise_for_status()
        current = response.json().get("current", {})
        return {
            "temperature": current.get("temperature_2m"), "humidity": current.get("relative_humidity_2m"),
            "apparent_temp": current.get("apparent_temperature"), "precipitation": current.get("precipitation"),
            "code": current.get("weather_code"), "wind_speed": current.get("wind_speed_10m"),
            "wind_direction": current.get("wind_direction_10m")
        }

async def fetch_wttr_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=4.0) as client:
        response = await client.get(f'https://wttr.in/{lat},{lon}?format=j1')
        response.raise_for_status()
        data = response.json().get('current_condition', [{}])[0]
        return {
            "temperature": float(data.get('temp_C', 0)), "humidity": int(data.get('humidity', 0)),
            "apparent_temp": float(data.get('FeelsLikeC', 0)), "precipitation": float(data.get('precipMM', 0)),
            "wind_speed": float(data.get('windspeedKmph', 0)) * (1000/3600), "wind_direction": int(data.get('winddirDegree', 0))
        }

async def get_weather_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    try:
        return await weather_breaker.call(fetch_openmeteo_weather, lat, lon)
    except Exception:
        try:
            return await wttr_breaker.call(fetch_wttr_weather, lat, lon)
        except Exception:
            return None

async def should_force_weather_update(redis_client, device_id: str, lat: float, lon: float) -> bool:
    last_position = await get_device_position(redis_client, device_id)
    if not last_position: return True
    last_update_iso = last_position.get('last_weather_update')
    if not last_update_iso: return True
    try:
        last_update_dt = datetime.datetime.fromisoformat(last_update_iso)
        if (datetime.datetime.now(datetime.timezone.utc) - last_update_dt).total_seconds() > WEATHER_EXPIRATION_SECONDS: return True
        if calculate_distance_km(lat, lon, last_position.get("lat"), last_position.get("lon")) > MOVEMENT_THRESHOLD_KM: return True
    except (ValueError, TypeError): return True
    return False

async def get_weather_enrichment(redis_client, device_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        lat_str = data.get("y")
        lon_str = data.get("x")
        if not lat_str or not lon_str: return data
        
        current_lat, current_lon = float(lat_str), float(lon_str)
        if await should_force_weather_update(redis_client, device_id, current_lat, current_lon):
            weather = await get_weather_data(current_lat, current_lon)
            if weather:
                data.update(weather)
                await save_device_position(
                    redis_client, device_id, {
                        "lat": current_lat, "lon": current_lon,
                        "last_weather_update": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                )
    except (ValueError, TypeError, httpx.RequestError, httpx.HTTPStatusError):
        pass
    return data
