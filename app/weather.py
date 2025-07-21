import httpx
import asyncio
import datetime
from typing import Optional, Dict, Any, Tuple

from app.utils import weather_breaker, wttr_breaker, calculate_distance_km, WeatherRateLimiter
from app.database import get_device_position, save_device_position
from app.weather_cache import find_cached_weather, save_weather_to_cache

MOVEMENT_THRESHOLD_KM = 1.0
WEATHER_EXPIRATION_SECONDS = 3600
WEATHER_COOLDOWN_SECONDS = 60

async def fetch_openmeteo_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=5.0) as client:
        weather_params = {
            "latitude": lat, "longitude": lon, "timezone": "UTC", "wind_speed_unit": "ms",
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m,pressure_msl,cloud_cover"
        }
        marine_params = {
            "latitude": lat, "longitude": lon, "timezone": "UTC",
            "current": "wave_height,wave_direction,wave_period,swell_wave_height,swell_wave_direction,swell_wave_period"
        }
        weather_task = client.get("https://api.open-meteo.com/v1/forecast", params=weather_params)
        marine_task = client.get("https://marine-api.open-meteo.com/v1/marine", params=marine_params)
        
        results = await asyncio.gather(weather_task, marine_task, return_exceptions=True)
        
        final_data = {}
        if not isinstance(results[0], Exception) and results[0].status_code == 200:
            current = results[0].json().get("current", {})
            final_data.update({
                "temperature": current.get("temperature_2m"), "humidity": current.get("relative_humidity_2m"),
                "apparent_temp": current.get("apparent_temperature"), "precipitation": current.get("precipitation"),
                "code": current.get("weather_code"), "wind_speed": current.get("wind_speed_10m"),
                "wind_direction": current.get("wind_direction_10m"), "wind_gusts": current.get("wind_gusts_10m"),
                "pressure_msl": current.get("pressure_msl"), "cloud_cover": current.get("cloud_cover")
            })
        if not isinstance(results[1], Exception) and results[1].status_code == 200:
            current = results[1].json().get("current", {})
            final_data.update({
                "marine_wave_height": current.get("wave_height"),
                "marine_wave_direction": current.get("wave_direction"),
                "marine_wave_period": current.get("wave_period"),
                "marine_swell_wave_height": current.get("swell_wave_height"),
                "marine_swell_wave_direction": current.get("swell_wave_direction"),
                "marine_swell_wave_period": current.get("swell_wave_period")
            })
        return final_data if final_data else None

async def fetch_wttr_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    async with httpx.AsyncClient(timeout=4.0) as client:
        response = await client.get(f'https://wttr.in/{lat},{lon}?format=j1')
        response.raise_for_status()
        data = response.json().get('current_condition', [{}])[0]
        return {
            "temperature": float(data.get('temp_C', 0)), "humidity": int(data.get('humidity', 0)),
            "apparent_temp": float(data.get('FeelsLikeC', 0)), "precipitation": float(data.get('precipMM', 0)),
            "wind_speed": float(data.get('windspeedKmph', 0)) * (1000/3600), "wind_direction": int(data.get('winddirDegree', 0)),
            "pressure_msl": float(data.get("pressure", 0)), "cloud_cover": int(data.get("cloudcover", 0))
        }

async def _get_weather_from_api(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    try:
        return await weather_breaker.call(fetch_openmeteo_data, lat, lon)
    except Exception:
        try:
            return await wttr_breaker.call(fetch_wttr_weather, lat, lon)
        except Exception:
            return None

async def get_coordinated_weather_data(rate_limiter: WeatherRateLimiter, lat: float, lon: float) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    cached_data, timestamp = await find_cached_weather(lat, lon)
    if cached_data:
        return cached_data, timestamp
    if await rate_limiter.is_rate_limited():
        return None, None
    
    api_data = await _get_weather_from_api(lat, lon)
    if api_data:
        await rate_limiter.increment()
        timestamp = await save_weather_to_cache(lat, lon, api_data)
        return api_data, timestamp
    return None, None

async def _should_force_weather_update(redis_client, device_id: str, lat: float, lon: float) -> bool:
    last_pos = await get_device_position(redis_client, device_id)
    if not last_pos or not last_pos.get('last_weather_update'):
        return True
    try:
        last_dt = datetime.datetime.fromisoformat(last_pos['last_weather_update'])
        now = datetime.datetime.now(datetime.timezone.utc)
        
        if (now - last_dt).total_seconds() < WEATHER_COOLDOWN_SECONDS:
            return False

        if (now - last_dt).total_seconds() > WEATHER_EXPIRATION_SECONDS:
            return True
            
        if calculate_distance_km(lat, lon, last_pos.get("lat"), last_pos.get("lon")) > MOVEMENT_THRESHOLD_KM:
            return True
    except (ValueError, TypeError, KeyError):
        return True
    return False

async def get_weather_enrichment(redis_client, device_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    lat_str, lon_str = data.get("y"), data.get("x")
    if not lat_str or not lon_str: return data
    
    try:
        lat, lon = float(lat_str), float(lon_str)
        if await _should_force_weather_update(redis_client, device_id, lat, lon):
            rate_limiter = WeatherRateLimiter(redis_client)
            weather, weather_ts = await get_coordinated_weather_data(rate_limiter, lat, lon)
            if weather:
                data.update(weather)
                data['weather_fetch_lat'] = lat
                data['weather_fetch_lon'] = lon
                if weather_ts:
                    data['weather_fetch_ts'] = weather_ts
                await save_device_position(
                    redis_client, device_id, {
                        "lat": lat, "lon": lon,
                        "last_weather_update": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }
                )
    except (ValueError, TypeError):
        pass
    return data