import pytz
from typing import Dict, Any, Tuple, Optional
from timezonefinderL import TimezoneFinder
from app.utils import format_utc_timestamp

tf = TimezoneFinder()

WEATHER_CODE_DESCRIPTIONS = {
    0: "Clear", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast", 45: "Fog", 48: "Rime Fog",
    51: "Light Drizzle", 53: "Drizzle", 55: "Dense Drizzle", 61: "Slight Rain", 63: "Rain", 65: "Heavy Rain",
    71: "Slight Snow", 73: "Snow", 75: "Heavy Snow", 80: "Slight Showers", 81: "Showers", 82: "Violent Showers",
    85: "Slight Snow Showers", 86: "Heavy Snow Showers", 95: "Thunderstorm"
}

def safe_int(v):
    try: return int(float(v))
    except (ValueError, TypeError, AttributeError): return None

def safe_float(v):
    try: return float(v)
    except (ValueError, TypeError, AttributeError): return None

def get_wind_direction_compass(degrees: Optional[float]) -> Optional[str]:
    if degrees is None: return None
    val = int((degrees / 22.5) + 0.5)
    arr = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return arr[(val % 16)]

def get_temperature_assessment(temp: Optional[float]) -> Optional[str]:
    if temp is None: return None
    if temp < 0: return "Freezing"
    if temp < 10: return "Cold"
    if temp < 20: return "Cool"
    if temp < 25: return "Mild"
    if temp < 30: return "Warm"
    return "Hot"

def get_wind_description(speed_ms: Optional[float]) -> Optional[str]:
    if speed_ms is None: return None
    if speed_ms < 0.3: return "Calm"
    if speed_ms < 1.6: return "Light air"
    if speed_ms < 3.4: return "Light breeze"
    if speed_ms < 5.5: return "Gentle breeze"
    if speed_ms < 8.0: return "Moderate breeze"
    if speed_ms < 10.8: return "Fresh breeze"
    if speed_ms < 13.9: return "Strong breeze"
    return "High wind"

def get_precipitation_info(precip_mm: Optional[float], code: Optional[int]) -> Dict[str, str]:
    precip_type_map = {
        "drizzle": "Drizzle", "rain": "Rain", "snow": "Snow", "showers": "Showers"
    }
    desc = WEATHER_CODE_DESCRIPTIONS.get(code, "").lower()
    precip_type = next((v for k, v in precip_type_map.items() if k in desc), "Unknown")

    if precip_mm is None or precip_mm == 0:
        return {"summary": "No precipitation in the last hour.", "type": "None", "intensity": "None"}
    
    if precip_mm < 0.5: intensity = "Light"
    elif precip_mm < 4.0: intensity = "Moderate"
    else: intensity = "Heavy"
    
    return {"summary": f"{intensity} {precip_type.lower()} in the last hour.", "type": precip_type, "intensity": intensity}

def format_bssid(bssid_val: Any) -> Optional[str]:
    if bssid_val is None:
        return None
    
    s_bssid = str(bssid_val).strip().lower().replace(":", "")

    if not s_bssid or s_bssid in ["0", "null"]:
        return None

    if len(s_bssid) == 12 and all(c in "0123456789abcdef" for c in s_bssid):
        return ":".join(s_bssid[i:i+2] for i in range(0, 12, 2))
        
    return None

def transform_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    def f(key, unit, precision=0):
        val = safe_float(data.get(key))
        if val is None: return None
        return f"{val:.{precision}f}{unit}"

    temp_c = safe_float(data.get('temperature'))
    precip_val = safe_float(data.get('precipitation'))
    weather_code = safe_int(data.get('code'))
    wind_speed_ms = safe_float(data.get('wind_speed'))
    precip_info = get_precipitation_info(precip_val, weather_code)

    formatted_bssid = format_bssid(data.get('b'))
    cellular_type = data.get('t')
    active_network = "Wi-Fi" if formatted_bssid else cellular_type

    transformed = {
        "identity": {"device_id": data.get("device_id"), "device_name": data.get('n')},
        "network": {
            "active_network": active_network,
            "source_ip": data.get('client_ip'), "type": cellular_type, "operator": data.get('o'),
            "wifi_bssid": formatted_bssid,
            "cellular": {
                "signal_strength": f('r', ' dBm'), "mcc": data.get('mc'), "mnc": data.get('mn'),
                "cell_id": data.get('ci'), "tac": data.get('tc'),
            },
            "bandwidth": {"download": f('d', ' Mbps', 1), "upload": f('u', ' Mbps', 1)}
        },
        "location": {
            "latitude": str(safe_float(data.get('y'))) if data.get('y') is not None else None,
            "longitude": str(safe_float(data.get('x'))) if data.get('x') is not None else None,
            "altitude": f('a', ' m'), "accuracy": f('ac', ' m'), "speed": f('s', ' km/h'),
        },
        "power": {"battery_percent": f('p', '%'), "capacity_mah": f('c', ' mAh')},
        "environment": {
            "weather": {
                "description": WEATHER_CODE_DESCRIPTIONS.get(weather_code),
                "temperature": f('temperature', '°C', 1), "feels_like": f('apparent_temp', '°C', 1),
                "assessment": get_temperature_assessment(temp_c), "humidity": f('humidity', '%'),
                "pressure": f('pressure_msl', ' hPa', 0), "cloud_cover": f('cloud_cover', '%', 0)
            },
            "precipitation": precip_info,
            "wind": {
                "speed": f('wind_speed', ' m/s', 1), "gusts": f('wind_gusts', ' m/s', 1),
                "description": get_wind_description(wind_speed_ms),
                "direction": get_wind_direction_compass(safe_float(data.get('wind_direction')))
            },
            "marine": {
                "wave": {
                    "height": f('marine_wave_height', ' m', 2), "period": f('marine_wave_period', ' s', 1),
                    "direction": get_wind_direction_compass(safe_float(data.get('marine_wave_direction')))
                },
                "swell": {
                    "height": f('marine_swell_wave_height', ' m', 2), "period": f('marine_swell_wave_period', ' s', 1),
                    "direction": get_wind_direction_compass(safe_float(data.get('marine_swell_wave_direction')))
                }
            }
        },
        "diagnostics": {
            "ingest_request_id": data.get("request_id"),
            "timestamps": {
                "device_event_timestamp_utc": format_utc_timestamp(data.get("calculated_event_timestamp")),
                "ingest_receive_timestamp_utc": format_utc_timestamp(data.get("received_at") or data.get("calculated_event_timestamp")),
            },
            "ingest_request_info": data.get("request_headers"),
            "ingest_warnings": data.get("warnings"),
        }
    }
    return transformed