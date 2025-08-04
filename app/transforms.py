import pytz
import datetime
from typing import Dict, Any, Optional
from timezonefinderL import TimezoneFinder
from app.utils import (
    format_utc_timestamp, 
    calculate_distance_km, 
    decode_geohash, 
    decode_base62, 
    decode_bssid_base64,
    get_nested
)

tf = TimezoneFinder()

WEATHER_CODE_DESCRIPTIONS = {
    0: "Clear", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast", 45: "Fog", 48: "Rime Fog",
    51: "Light Drizzle", 53: "Drizzle", 55: "Dense Drizzle", 61: "Slight Rain", 63: "Rain", 65: "Heavy Rain",
    71: "Slight Snow", 73: "Snow", 75: "Heavy Snow", 80: "Slight Showers", 81: "Showers", 82: "Violent Showers",
    85: "Slight Snow Showers", 86: "Heavy Snow Showers", 95: "Thunderstorm"
}
CELLULAR_TYPE_MAP = {
    1: "GSM", 2: "GPRS/EDGE", 3: "UMTS/HSPA", 4: "LTE",
    5: "NR(5G)", 6: "CDMA", 7: "IDEN", 0: "Other"
}
CHARGING_STATE_MAP = {0: "Not Charging", 1: "AC", 2: "USB", 3: "Wireless", 4: "Full"}
DATA_ACTIVITY_MAP = {0: "None", 1: "In", 2: "Out", 3: "In/Out"}
WIFI_STANDARD_MAP = {1: "Other", 4: "Wi-Fi 4", 5: "Wi-Fi 5", 6: "Wi-Fi 6"}
SYSTEM_AUDIO_MAP = {0: "Idle", 1: "Media", 2: "In Call"}
PHONE_ACTIVITY_MAP = {0: "Stable/Upside Down", 1: "Stable", 2: "Moving"}

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

def _get_val_or_base(data: Dict[str, Any], key: str, base_state: Dict[str, Any], path: list, default: Any, error_val: Any = object(), transform_func=None):
    if key in data:
        val = data[key]
        if val == error_val:
            return None
        if transform_func:
            return transform_func(val)
        return val
    
    return get_nested(base_state, path, default)

def transform_payload(data: Dict[str, Any], base_state: Dict[str, Any]) -> Dict[str, Any]:
    lat, lon, precision = None, None, None
    if 'g' in data and data['g'] != "":
        decoded = decode_geohash(data['g'])
        if decoded:
            lat, lon, precision = decoded
        else:
            lat = get_nested(base_state, ['location', 'latitude'])
            lon = get_nested(base_state, ['location', 'longitude'])
            precision = get_nested(base_state, ['location', 'coordinate_precision'])
    else:
        lat = get_nested(base_state, ['location', 'latitude'])
        lon = get_nested(base_state, ['location', 'longitude'])
        precision = get_nested(base_state, ['location', 'coordinate_precision'])

    cellular_type_str = _get_val_or_base(data, 't', base_state, ['network', 'cellular', 'type'], None, -1, lambda v: CELLULAR_TYPE_MAP.get(v))
    formatted_bssid = _get_val_or_base(data, 'b', base_state, ['network', 'wifi', 'bssid'], None, "", decode_bssid_base64)
    currently_used_active_network = "Wi-Fi" if formatted_bssid else cellular_type_str
    
    battery_percent_val = _get_val_or_base(data, 'p', base_state, ['power', 'battery_percent'], None)
    capacity_mah_val = _get_val_or_base(data, 'c', base_state, ['power', 'capacity_in_mah'], 0, 0, lambda v: safe_int(v) * 100)
    leftover_capacity_mah = None
    if battery_percent_val is not None and capacity_mah_val is not None and capacity_mah_val > 0:
        leftover_capacity_mah = int(round((battery_percent_val / 100.0) * capacity_mah_val))

    temp_c = _get_val_or_base(data, 'temperature', base_state, ['environment', 'weather', 'temperature_in_celsius'], None, None, lambda v: safe_float(v, 1))
    precip_val = _get_val_or_base(data, 'precipitation', base_state, ['environment', 'precipitation', 'value_mm'], None, None, safe_float)
    weather_code = _get_val_or_base(data, 'code', base_state, ['environment', 'weather', 'code'], None, None, safe_int)
    wind_speed_ms = _get_val_or_base(data, 'wind_speed', base_state, ['environment', 'wind', 'speed_in_meters_per_second'], None, None, lambda v: safe_float(v, 1))
    precip_info = get_precipitation_info(precip_val, weather_code)
    
    fetch_lat = safe_float(data.get('weather_fetch_lat', get_nested(base_state, ['diagnostics', 'weather', 'weather_fetch_lat'])))
    fetch_lon = safe_float(data.get('weather_fetch_lon', get_nested(base_state, ['diagnostics', 'weather', 'weather_fetch_lon'])))
    weather_ts_iso = data.get("weather_fetch_ts", get_nested(base_state, ['diagnostics', 'weather', 'weather_fetch_ts']))
    event_ts_iso = data.get("calculated_event_timestamp")
    
    distance_km = calculate_distance_km(lat, lon, fetch_lat, fetch_lon)
    distance_str = format_distance(distance_km)

    age_str = None
    if event_ts_iso and weather_ts_iso:
        try:
            event_dt = datetime.datetime.fromisoformat(event_ts_iso.replace(" ", "T").replace("Z", "+00:00"))
            weather_dt = datetime.datetime.fromisoformat(weather_ts_iso.replace("Z", "+00:00"))
            age_seconds = (event_dt - weather_dt).total_seconds()
            age_str = format_timespan_human(age_seconds)
        except (ValueError, TypeError): pass

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
        except Exception: pass

    weather_diag = {
        "weather_fetch_location": f"{fetch_lat:.6f}, {fetch_lon:.6f}" if fetch_lat is not None and fetch_lon is not None else None,
        "weather_distance_from_actual_location": distance_str,
        "weather_data_old": age_str,
        "weather_request_timestamp_utc": format_utc_timestamp(weather_ts_iso),
        "weather_request_timestamp_location_time": weather_ts_local_str
    }
    
    old_app_settings = base_state.get('app_settings', {})
    new_app_settings_update = data.get('ad', {})
    merged_app_settings = {**old_app_settings, **new_app_settings_update}

    wind_dir_val = _get_val_or_base(data, 'wind_direction', base_state, ['environment', 'wind', 'direction'], None)
    wind_dir_str = get_wind_direction_compass(safe_float(wind_dir_val)) if isinstance(wind_dir_val, (int, float)) else wind_dir_val

    wave_dir_val = _get_val_or_base(data, 'marine_wave_direction', base_state, ['environment', 'marine', 'wave', 'direction'], None)
    wave_dir_str = get_wind_direction_compass(safe_float(wave_dir_val)) if isinstance(wave_dir_val, (int, float)) else wave_dir_val

    swell_dir_val = _get_val_or_base(data, 'marine_swell_wave_direction', base_state, ['environment', 'marine', 'swell', 'direction'], None)
    swell_dir_str = get_wind_direction_compass(safe_float(swell_dir_val)) if isinstance(swell_dir_val, (int, float)) else swell_dir_val

    transformed = {
        "identity": {
            "device_id": data.get("device_id"), 
            "device_name": _get_val_or_base(data, 'n', base_state, ['identity', 'device_name'], None)
        },
        "network": {
            "currently_used_active_network": currently_used_active_network,
            "source_ip": _get_val_or_base(data, 'client_ip', base_state, ['network', 'source_ip'], None),
            "wifi": {
                "bssid": formatted_bssid,
                "ssid": _get_val_or_base(data, 'wn', base_state, ['network', 'wifi', 'ssid'], None, ""),
                "frequency_channel": _get_val_or_base(data, 'wf', base_state, ['network', 'wifi', 'frequency_channel'], None, 0),
                "rssi_dbm": _get_val_or_base(data, 'wr', base_state, ['network', 'wifi', 'rssi_dbm'], None, 0, lambda v: -safe_int(v)),
                "link_speed_quality_index": _get_val_or_base(data, 'ws', base_state, ['network', 'wifi', 'link_speed_quality_index'], None, -1),
                "standard": _get_val_or_base(data, 'wt', base_state, ['network', 'wifi', 'standard'], None, -1, lambda v: WIFI_STANDARD_MAP.get(v))
            },
            "cellular": {
                "type": cellular_type_str, 
                "operator": _get_val_or_base(data, 'o', base_state, ['network', 'cellular', 'operator'], None, ""),
                "signal_strength_in_dbm": _get_val_or_base(data, 'r', base_state, ['network', 'cellular', 'signal_strength_in_dbm'], None, 0, lambda v: -safe_int(v)),
                "signal_quality": _get_val_or_base(data, 'rq', base_state, ['network', 'cellular', 'signal_quality'], None, 0), 
                "mcc": _get_val_or_base(data, 'mc', base_state, ['network', 'cellular', 'mcc'], None, -1), 
                "mnc": _get_val_or_base(data, 'mn', base_state, ['network', 'cellular', 'mnc'], None, ""),
                "cell_id": _get_val_or_base(data, 'ci', base_state, ['network', 'cellular', 'cell_id'], None, "", decode_base62),
                "tac": _get_val_or_base(data, 'tc', base_state, ['network', 'cellular', 'tac'], None, -1), 
                "timing_advance": _get_val_or_base(data, 'ta', base_state, ['network', 'cellular', 'timing_advance'], None, -1)
            },
            "bandwidth": {
                "download_in_mbps": _get_val_or_base(data, 'd', base_state, ['network', 'bandwidth', 'download_in_mbps'], None, 0, lambda v: safe_float(v, 1)),
                "upload_in_mbps": _get_val_or_base(data, 'u', base_state, ['network', 'bandwidth', 'upload_in_mbps'], None, 0, lambda v: safe_float(v, 1))
            }
        },
        "location": {
            "latitude": lat, "longitude": lon, "coordinate_precision": precision,
            "altitude_in_meters": _get_val_or_base(data, 'a', base_state, ['location', 'altitude_in_meters'], None, -1, safe_int), 
            "accuracy_in_meters": _get_val_or_base(data, 'ac', base_state, ['location', 'accuracy_in_meters'], None, -1, safe_int),
            "speed_in_kmh": _get_val_or_base(data, 's', base_state, ['location', 'speed_in_kmh'], None, -1, safe_int),
        },
        "power": {
            "battery_percent": battery_percent_val, "capacity_in_mah": capacity_mah_val,
            "calculated_leftover_capacity_in_mah": leftover_capacity_mah,
            "charging_state": _get_val_or_base(data, 'cs', base_state, ['power', 'charging_state'], None, None, lambda v: CHARGING_STATE_MAP.get(v)),
            "power_save_mode": _get_val_or_base(data, 'pm', base_state, ['power', 'power_save_mode'], False, None, lambda v: v == 1)
        },
        "environment": {
            "weather": {
                "description": WEATHER_CODE_DESCRIPTIONS.get(weather_code),
                "temperature_in_celsius": temp_c,
                "feels_like_in_celsius": _get_val_or_base(data, 'apparent_temp', base_state, ['environment', 'weather', 'feels_like_in_celsius'], None, None, lambda v: safe_float(v, 1)),
                "assessment": get_temperature_assessment(temp_c),
                "humidity_percent": _get_val_or_base(data, 'humidity', base_state, ['environment', 'weather', 'humidity_percent'], None, None, safe_int),
                "pressure_in_hpa": _get_val_or_base(data, 'pressure_msl', base_state, ['environment', 'weather', 'pressure_in_hpa'], None, None, safe_int),
                "cloud_cover_percent": _get_val_or_base(data, 'cloud_cover', base_state, ['environment', 'weather', 'cloud_cover_percent'], None, None, safe_int)
            },
            "precipitation": precip_info,
            "wind": {
                "speed_in_meters_per_second": wind_speed_ms,
                "gusts_in_meters_per_second": _get_val_or_base(data, 'wind_gusts', base_state, ['environment', 'wind', 'gusts_in_meters_per_second'], None, None, lambda v: safe_float(v, 1)),
                "description": get_wind_description(wind_speed_ms),
                "direction": wind_dir_str
            },
            "marine": {
                "wave": {
                    "height_in_meters": _get_val_or_base(data, 'marine_wave_height', base_state, ['environment', 'marine', 'wave', 'height_in_meters'], None, None, lambda v: safe_float(v, 2)),
                    "period_in_seconds": _get_val_or_base(data, 'marine_wave_period', base_state, ['environment', 'marine', 'wave', 'period_in_seconds'], None, None, lambda v: safe_float(v, 1)),
                    "direction": wave_dir_str
                },
                "swell": {
                    "height_in_meters": _get_val_or_base(data, 'marine_swell_wave_height', base_state, ['environment', 'marine', 'swell', 'height_in_meters'], None, None, lambda v: safe_float(v, 2)),
                    "period_in_seconds": _get_val_or_base(data, 'marine_swell_wave_period', base_state, ['environment', 'marine', 'swell', 'period_in_seconds'], None, None, lambda v: safe_float(v, 1)),
                    "direction": swell_dir_str
                }
            }
        },
        "device_state": {
            "screen_on": _get_val_or_base(data, 'sc', base_state, ['device_state', 'screen_on'], False, None, lambda v: v == 1),
            "vpn_active": _get_val_or_base(data, 'vp', base_state, ['device_state', 'vpn_active'], False, None, lambda v: v == 1),
            "network_metered": _get_val_or_base(data, 'nm', base_state, ['device_state', 'network_metered'], False, None, lambda v: v == 1),
            "data_activity": _get_val_or_base(data, 'da', base_state, ['device_state', 'data_activity'], None, -1, lambda v: DATA_ACTIVITY_MAP.get(v)),
            "system_audio_state": _get_val_or_base(data, 'au', base_state, ['device_state', 'system_audio_state'], None, None, lambda v: SYSTEM_AUDIO_MAP.get(v)),
            "camera_active": _get_val_or_base(data, 'ca', base_state, ['device_state', 'camera_active'], False, None, lambda v: v == 1),
            "flashlight_on": _get_val_or_base(data, 'fl', base_state, ['device_state', 'flashlight_on'], False, None, lambda v: v == 1),
            "phone_activity_state": _get_val_or_base(data, 'pa', base_state, ['device_state', 'phone_activity_state'], None, -1, lambda v: PHONE_ACTIVITY_MAP.get(v))
        },
        "sensors": {
            "device_temperature_celsius": _get_val_or_base(data, 'dt', base_state, ['sensors', 'device_temperature_celsius'], None), 
            "ambient_light_level": _get_val_or_base(data, 'lx', base_state, ['sensors', 'ambient_light_level'], None, -1),
            "barometer_hpa": _get_val_or_base(data, 'pr', base_state, ['sensors', 'barometer_hpa'], None, 0), 
            "steps_since_boot": _get_val_or_base(data, 'st', base_state, ['sensors', 'steps_since_boot'], None, -1),
            "proximity_near": _get_val_or_base(data, 'px', base_state, ['sensors', 'proximity_near'], False, -1, lambda v: v == 0),
        },
        "diagnostics": {
            "ingest_request_id": data.get("request_id"), "weather": weather_diag,
            "timestamps": {
                "device_event_timestamp_utc": format_utc_timestamp(data.get("calculated_event_timestamp")),
                "ingest_receive_timestamp_utc": format_utc_timestamp(data.get("received_at") or data.get("calculated_event_timestamp")),
            },
            "ingest_request_info": data.get("request_headers"), "ingest_warnings": data.get("warnings")
        },
        "app_settings": merged_app_settings
    }
    return transformed
