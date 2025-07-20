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

def transform_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    def f(key, unit, precision=0):
        val = safe_float(data.get(key))
        if val is None: return None
        return f"{val:.{precision}f}{unit}"

    transformed = {
        "identity": {"device_id": data.get("device_id"), "device_name": data.get('n')},
        "network": {
            "source_ip": data.get('client_ip'), "type": data.get('t'), "operator": data.get('o'),
            "wifi_bssid": data.get('b'),
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
                "description": WEATHER_CODE_DESCRIPTIONS.get(safe_int(data.get('code'))),
                "temperature": f('temperature', '°C', 1), "apparent_temp": f('apparent_temp', '°C', 1),
                "humidity": f('humidity', '%'), "precipitation": f('precipitation', ' mm', 1),
                "wind": {
                    "speed": f('wind_speed', ' km/h', 1),
                    "direction_deg": f('wind_direction', '°'),
                    "direction_compass": get_wind_direction_compass(safe_float(data.get('wind_direction')))
                }
            },
            "marine": {
                "wave_height": f('marine_wave_height', ' m', 2),
                "wave_direction": f('marine_wave_direction', '°'),
                "wave_period": f('marine_wave_period', ' s', 1)
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
