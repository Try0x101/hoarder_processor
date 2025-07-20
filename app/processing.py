import orjson
import httpx
from typing import Dict, Any, Optional
from app.utils import format_utc_timestamp

def safe_int(value):
    if value is None: return None
    try: return int(float(value))
    except (ValueError, TypeError): return None

def safe_float(value):
    if value is None: return None
    try: return float(value)
    except (ValueError, TypeError): return None

def safe_string(value):
    if value is None or value == "": return None
    return str(value)

def cleanup_empty(d: Dict) -> Dict:
    if not isinstance(d, dict):
        return d
    return {
        k: v for k, v in ((k, cleanup_empty(v)) for k, v in d.items())
        if v is not None and v != '' and v != [] and v != {}
    }

async def fetch_weather_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return None
    
    api_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,wind_direction_10m",
        "timezone": "UTC"
    }
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(api_url, params=params)
            response.raise_for_status()
            data = response.json().get("current", {})
            return {"environment": {"weather": {
                "temperature": data.get("temperature_2m"),
                "humidity": data.get("relative_humidity_2m"),
                "apparent_temp": data.get("apparent_temperature"),
                "precipitation": data.get("precipitation"),
                "code": data.get("weather_code"),
                "wind_speed": data.get("wind_speed_10m"),
                "wind_direction": data.get("wind_direction_10m")
            }}}
    except (httpx.RequestError, httpx.HTTPStatusError):
        return None

async def enrich_record(record: Dict[str, Any]) -> Dict[str, Any]:
    try:
        original_payload = record.get("payload", {})
        request_info = record.get("request_info", {})
        
        def f(val, unit):
            int_val = safe_int(val)
            return f"{int_val}{unit}" if int_val is not None else None

        transformed = {
            "identity": {
                "device_id": record.get("device_id"),
                "device_name": safe_string(original_payload.get('n'))
            },
            "network": {
                "source_ip": safe_string(request_info.get('client_ip')),
                "type": safe_string(original_payload.get('t')),
                "operator": safe_string(original_payload.get('o')),
                "wifi_bssid": safe_string(original_payload.get('b')),
                "cellular": {
                    "signal_strength": f(original_payload.get('r'), ' dBm'),
                    "mcc": safe_string(original_payload.get('mc')),
                    "mnc": safe_string(original_payload.get('mn')),
                    "cell_id": safe_string(original_payload.get('ci')),
                    "tac": safe_string(original_payload.get('tc')),
                },
                "bandwidth": {
                    "download": f(original_payload.get('d'), ' Mbps'),
                    "upload": f(original_payload.get('u'), ' Mbps')
                }
            },
            "location": {
                "latitude": safe_string(safe_float(original_payload.get('y'))),
                "longitude": safe_string(safe_float(original_payload.get('x'))),
                "altitude": f(original_payload.get('a'), ' m'),
                "accuracy": f(original_payload.get('ac'), ' m'),
                "speed": f(original_payload.get('s'), ' km/h'),
            },
            "power": {
                "battery_percent": f(original_payload.get('p'), '%'),
                "capacity_mah": f(original_payload.get('c'), ' mAh')
            },
            "diagnostics": {
                "ingest_request_id": record.get("request_id"),
                "timestamps": {
                    "device_event_timestamp_utc": format_utc_timestamp(record.get("calculated_event_timestamp")),
                    "ingest_receive_timestamp_utc": format_utc_timestamp(record.get("received_at") or record.get("calculated_event_timestamp")),
                },
                "ingest_request_info": request_info,
            }
        }
        
        enriched_payload = cleanup_empty(transformed)

        return {
            "original_ingest_id": record.get("id"),
            "device_id": record.get("device_id"),
            "original_payload": original_payload,
            "enriched_payload": enriched_payload,
            "calculated_event_timestamp": record.get("calculated_event_timestamp"),
        }

    except Exception as e:
        print(f"Error enriching record {record.get('id')}: {e}")
        return None
