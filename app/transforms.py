import pytz
from typing import Dict, Any, Tuple, Optional
from timezonefinderL import TimezoneFinder
from app.utils import format_utc_timestamp

tf = TimezoneFinder()

WEATHER_CODE_DESCRIPTIONS = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast", 45: "Fog", 48: "Rime fog",
    51: "Light drizzle", 53: "Drizzle", 55: "Dense drizzle", 61: "Slight rain", 63: "Rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Snow", 75: "Heavy snow", 80: "Slight showers", 81: "Showers", 82: "Violent showers",
    85: "Slight snow showers", 86: "Heavy snow showers", 95: "Thunderstorm"
}

def safe_int(v):
    try: return int(float(v))
    except (ValueError, TypeError, AttributeError): return None

def safe_float(v):
    try: return float(v)
    except (ValueError, TypeError, AttributeError): return None

def get_timezone_info(lat, lon) -> Tuple[Optional[str], Optional[pytz.BaseTzInfo]]:
    try:
        lat, lon = safe_float(lat), safe_float(lon)
        if lat is None or lon is None: return None, None
        tz_name = tf.timezone_at(lng=lon, lat=lat)
        if tz_name: return tz_name, pytz.timezone(tz_name)
    except Exception: pass
    return None, None

def transform_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    _, tz_obj = get_timezone_info(data.get('y'), data.get('x'))

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
            "bandwidth": {"download": f('d', ' Mbps'), "upload": f('u', ' Mbps')}
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
                "wind_speed": f('wind_speed', ' km/h', 1), "wind_direction": f('wind_direction', '°')
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
