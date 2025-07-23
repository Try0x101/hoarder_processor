import pytz
import datetime
from typing import Dict, Any, Tuple, Optional
from timezonefinderL import TimezoneFinder
from app.utils import format_utc_timestamp, calculate_distance_km

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

def safe_float(v, precision=None):
    try:
        val = float(v)
        return round(val, precision) if precision is not None else val
    except (ValueError, TypeError, AttributeError):
        return None

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

def format_distance(distance_km: Optional[float]) -> Optional[str]:
    if distance_km is None or distance_km == float('inf'):
        return None
    rounded_km = round(distance_km)
    if rounded_km < 1:
        return "Less than 1 km"
    return f"{rounded_km:.0f} km"

def format_timespan_human(seconds: Optional[float]) -> Optional[str]:
    if seconds is None or seconds < 0:
        return None
    if seconds < 60:
        return "Less than a minute ago"
    if seconds < 3600:
        val = round(seconds / 60)
        return f"{val} minute{'s' if val != 1 else ''} ago"
    if seconds < 86400:
        val = round(seconds / 3600)
        return f"{val} hour{'s' if val != 1 else ''} ago"
    if seconds < 86400 * 7:
        val = round(seconds / 86400)
        return f"{val} day{'s' if val != 1 else ''} ago"
    if seconds < 86400 * 30.44:
        val = round(seconds / (86400 * 7))
        return f"{val} week{'s' if val != 1 else ''} ago"
    if seconds < 86400 * 365.25:
        val = round(seconds / (86400 * 30.44))
        return f"{val} month{'s' if val != 1 else ''} ago"
    
    years = round(seconds / (86400 * 365.25), 1)
    if years == int(years):
        val = int(years)
        return f"{val} year{'s' if val != 1 else ''} ago"
    return f"{years} years ago"

def transform_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    temp_c = safe_float(data.get('temperature'), 1)
    precip_val = safe_float(data.get('precipitation'))
    weather_code = safe_int(data.get('code'))
    wind_speed_ms = safe_float(data.get('wind_speed'), 1)
    precip_info = get_precipitation_info(precip_val, weather_code)

    formatted_bssid = format_bssid(data.get('b'))
    cellular_type = data.get('t')
    currently_used_active_network = "Wi-Fi" if formatted_bssid else cellular_type

    battery_percent_val = safe_int(data.get('p'))
    capacity_mah_val = safe_int(data.get('c'))

    leftover_capacity_mah = None
    if battery_percent_val is not None and capacity_mah_val is not None:
        leftover_capacity_mah = int(round((battery_percent_val / 100.0) * capacity_mah_val))

    fetch_lat = safe_float(data.get('weather_fetch_lat'))
    fetch_lon = safe_float(data.get('weather_fetch_lon'))
    weather_ts_iso = data.get("weather_fetch_ts")
    event_ts_iso = data.get("calculated_event_timestamp")
    
    device_lat = safe_float(data.get('y'))
    device_lon = safe_float(data.get('x'))
    distance_km = calculate_distance_km(device_lat, device_lon, fetch_lat, fetch_lon)
    distance_str = format_distance(distance_km)

    age_str = None
    if event_ts_iso and weather_ts_iso:
        try:
            event_dt = datetime.datetime.fromisoformat(event_ts_iso.replace(" ", "T").replace("Z", "+00:00"))
            weather_dt = datetime.datetime.fromisoformat(weather_ts_iso.replace("Z", "+00:00"))
            age_seconds = (event_dt - weather_dt).total_seconds()
            age_str = format_timespan_human(age_seconds)
        except (ValueError, TypeError):
            age_str = None

    weather_ts_local_str = None
    if weather_ts_iso and fetch_lat is not None and fetch_lon is not None:
        try:
            tz_name = tf.timezone_at(lng=fetch_lon, lat=fetch_lat)
            if tz_name:
                utc_dt = datetime.datetime.fromisoformat(weather_ts_iso.replace("Z", "+00:00"))
                local_tz = pytz.timezone(tz_name)
                local_dt = utc_dt.astimezone(local_tz)
                offset = local_dt.utcoffset()
                if offset is not None:
                    secs, sign = offset.total_seconds(), "+" if offset.total_seconds() >= 0 else "-"
                    h, rem = divmod(abs(secs), 3600)
                    m, _ = divmod(rem, 60)
                    offset_str = f"UTC{sign}{int(h)}" if m == 0 else f"UTC{sign}{int(h)}:{int(m):02d}"
                    weather_ts_local_str = local_dt.strftime(f'%d.%m.%Y %H:%M:%S {offset_str}')
        except Exception:
            weather_ts_local_str = None

    weather_diag = {
        "weather_fetch_location": f"{fetch_lat:.6f}, {fetch_lon:.6f}" if fetch_lat is not None and fetch_lon is not None else None,
        "weather_distance_from_actual_location": distance_str,
        "weather_data_old": age_str,
        "weather_request_timestamp_utc": format_utc_timestamp(weather_ts_iso),
        "weather_request_timestamp_location_time": weather_ts_local_str
    }

    transformed = {
        "identity": {"device_id": data.get("device_id"), "device_name": data.get('n')},
        "network": {
            "currently_used_active_network": currently_used_active_network,
            "source_ip": data.get('client_ip'),
            "wifi_bssid": formatted_bssid,
            "cellular": {
                "type": cellular_type,
                "operator": data.get('o'),
                "signal_strength_in_dbm": safe_int(data.get('r')), "mcc": data.get('mc'), "mnc": data.get('mn'),
                "cell_id": data.get('ci'), "tac": data.get('tc'),
            },
            "bandwidth": {
                "download_in_mbps": safe_float(data.get('d'), 1),
                "upload_in_mbps": safe_float(data.get('u'), 1)
            }
        },
        "location": {
            "latitude": safe_float(data.get('y')),
            "longitude": safe_float(data.get('x')),
            "altitude_in_meters": safe_int(data.get('a')),
            "accuracy_in_meters": safe_int(data.get('ac')),
            "speed_in_kmh": safe_int(data.get('s')),
        },
        "power": {
            "battery_percent": battery_percent_val,
            "capacity_in_mah": capacity_mah_val,
            "calculated_leftover_capacity_in_mah": leftover_capacity_mah
        },
        "environment": {
            "weather": {
                "description": WEATHER_CODE_DESCRIPTIONS.get(weather_code),
                "temperature_in_celsius": temp_c,
                "feels_like_in_celsius": safe_float(data.get('apparent_temp'), 1),
                "assessment": get_temperature_assessment(temp_c),
                "humidity_percent": safe_int(data.get('humidity')),
                "pressure_in_hpa": safe_int(data.get('pressure_msl')),
                "cloud_cover_percent": safe_int(data.get('cloud_cover'))
            },
            "precipitation": precip_info,
            "wind": {
                "speed_in_meters_per_second": wind_speed_ms,
                "gusts_in_meters_per_second": safe_float(data.get('wind_gusts'), 1),
                "description": get_wind_description(wind_speed_ms),
                "direction": get_wind_direction_compass(safe_float(data.get('wind_direction')))
            },
            "marine": {
                "wave": {
                    "height_in_meters": safe_float(data.get('marine_wave_height'), 2),
                    "period_in_seconds": safe_float(data.get('marine_wave_period'), 1),
                    "direction": get_wind_direction_compass(safe_float(data.get('marine_wave_direction')))
                },
                "swell": {
                    "height_in_meters": safe_float(data.get('marine_swell_wave_height'), 2),
                    "period_in_seconds": safe_float(data.get('marine_swell_wave_period'), 1),
                    "direction": get_wind_direction_compass(safe_float(data.get('marine_swell_wave_direction')))
                }
            }
        },
        "diagnostics": {
            "ingest_request_id": data.get("request_id"),
            "weather": weather_diag,
            "timestamps": {
                "device_event_timestamp_utc": format_utc_timestamp(data.get("calculated_event_timestamp")),
                "ingest_receive_timestamp_utc": format_utc_timestamp(data.get("received_at") or data.get("calculated_event_timestamp")),
            },
            "ingest_request_info": data.get("request_headers"),
            "ingest_warnings": data.get("warnings"),
        }
    }
    return transformed
