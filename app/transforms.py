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
DATA_ACTIVITY_MAP = {1: "None", 2: "In", 3: "Out", 4: "In/Out"}
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

def transform_payload(data: Dict[str, Any], base_state: Dict[str, Any]) -> Dict[str, Any]:
    lat, lon = decode_geohash(data.get('g')) if 'g' in data else (get_nested(base_state, ['location', 'latitude']), get_nested(base_state, ['location', 'longitude']))

    temp_c = safe_float(data.get('temperature', get_nested(base_state, ['environment', 'weather', 'temperature_in_celsius'])), 1)
    precip_val = safe_float(data.get('precipitation', get_nested(base_state, ['environment', 'precipitation', 'value_mm']))) # Assuming a placeholder
    weather_code = safe_int(data.get('code', get_nested(base_state, ['environment', 'weather', 'code'])))
    wind_speed_ms = safe_float(data.get('wind_speed', get_nested(base_state, ['environment', 'wind', 'speed_in_meters_per_second'])), 1)
    precip_info = get_precipitation_info(precip_val, weather_code)

    formatted_bssid = decode_bssid_base64(data.get('b')) if 'b' in data else get_nested(base_state, ['network', 'wifi_bssid'])
    cellular_type_code = data.get('t') if 't' in data else get_nested(base_state, ['network', 'cellular', 'type_code'])
    cellular_type_str = CELLULAR_TYPE_MAP.get(cellular_type_code)
    currently_used_active_network = "Wi-Fi" if formatted_bssid else cellular_type_str

    battery_percent_val = safe_int(data.get('p', get_nested(base_state, ['power', 'battery_percent'])))
    capacity_mah_val = safe_int(data.get('c', get_nested(base_state, ['power', 'capacity_in_mah'], 0) // 100)) * 100 if data.get('c') is not None or get_nested(base_state, ['power', 'capacity_in_mah']) is not None else None
    leftover_capacity_mah = None
    if battery_percent_val is not None and capacity_mah_val is not None and capacity_mah_val > 0:
        leftover_capacity_mah = int(round((battery_percent_val / 100.0) * capacity_mah_val))

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

    transformed = {
        "identity": {
            "device_id": data.get("device_id"), 
            "device_name": data.get('n', get_nested(base_state, ['identity', 'device_name']))
        },
        "network": {
            "currently_used_active_network": currently_used_active_network,
            "source_ip": data.get('client_ip', get_nested(base_state, ['network', 'source_ip'])),
            "wifi_bssid": formatted_bssid,
            "wifi_name_ssid": data.get('wn', get_nested(base_state, ['network', 'wifi_name_ssid'])),
            "cellular": {
                "type": cellular_type_str, 
                "operator": data.get('o', get_nested(base_state, ['network', 'cellular', 'operator'])),
                "signal_strength_in_dbm": -safe_int(data.get('r')) if 'r' in data else get_nested(base_state, ['network', 'cellular', 'signal_strength_in_dbm']),
                "signal_quality": data.get('rq', get_nested(base_state, ['network', 'cellular', 'signal_quality'])), 
                "mcc": data.get('mc', get_nested(base_state, ['network', 'cellular', 'mcc'])), 
                "mnc": data.get('mn', get_nested(base_state, ['network', 'cellular', 'mnc'])),
                "cell_id": decode_base62(data.get('ci')) if 'ci' in data else get_nested(base_state, ['network', 'cellular', 'cell_id']),
                "tac": data.get('tc', get_nested(base_state, ['network', 'cellular', 'tac'])), 
                "timing_advance": data.get('ta', get_nested(base_state, ['network', 'cellular', 'timing_advance']))
            },
            "bandwidth": {
                "download_in_mbps": safe_float(data.get('d', get_nested(base_state, ['network', 'bandwidth', 'download_in_mbps'])), 1),
                "upload_in_mbps": safe_float(data.get('u', get_nested(base_state, ['network', 'bandwidth', 'upload_in_mbps'])), 1)
            }
        },
        "location": {
            "latitude": safe_float(lat), "longitude": safe_float(lon),
            "altitude_in_meters": safe_int(data.get('a', get_nested(base_state, ['location', 'altitude_in_meters']))), 
            "accuracy_in_meters": safe_int(data.get('ac', get_nested(base_state, ['location', 'accuracy_in_meters']))),
            "speed_in_kmh": safe_int(data.get('s', get_nested(base_state, ['location', 'speed_in_kmh']))),
        },
        "power": {
            "battery_percent": battery_percent_val, "capacity_in_mah": capacity_mah_val,
            "calculated_leftover_capacity_in_mah": leftover_capacity_mah,
            "charging_state": CHARGING_STATE_MAP.get(data.get('cs', get_nested(base_state, ['power', 'charging_state_code']))),
            "power_save_mode": data.get('pm') == 1 if 'pm' in data else get_nested(base_state, ['power', 'power_save_mode'])
        },
        "environment": {
            "weather": {
                "description": WEATHER_CODE_DESCRIPTIONS.get(weather_code),
                "temperature_in_celsius": temp_c, "feels_like_in_celsius": safe_float(data.get('apparent_temp', get_nested(base_state, ['environment', 'weather', 'feels_like_in_celsius'])), 1),
                "assessment": get_temperature_assessment(temp_c), "humidity_percent": safe_int(data.get('humidity', get_nested(base_state, ['environment', 'weather', 'humidity_percent']))),
                "pressure_in_hpa": safe_int(data.get('pressure_msl', get_nested(base_state, ['environment', 'weather', 'pressure_in_hpa']))), "cloud_cover_percent": safe_int(data.get('cloud_cover', get_nested(base_state, ['environment', 'weather', 'cloud_cover_percent'])))
            },
            "precipitation": precip_info,
            "wind": {
                "speed_in_meters_per_second": wind_speed_ms, "gusts_in_meters_per_second": safe_float(data.get('wind_gusts', get_nested(base_state, ['environment', 'wind', 'gusts_in_meters_per_second'])), 1),
                "description": get_wind_description(wind_speed_ms),
                "direction": get_wind_direction_compass(safe_float(data.get('wind_direction', get_nested(base_state, ['environment', 'wind', 'direction_degrees']))))
            },
            "marine": {
                "wave": { "height_in_meters": safe_float(data.get('marine_wave_height'), 2), "period_in_seconds": safe_float(data.get('marine_wave_period'), 1), "direction": get_wind_direction_compass(safe_float(data.get('marine_wave_direction')))},
                "swell": { "height_in_meters": safe_float(data.get('marine_swell_wave_height'), 2), "period_in_seconds": safe_float(data.get('marine_swell_wave_period'), 1), "direction": get_wind_direction_compass(safe_float(data.get('marine_swell_wave_direction')))}
            }
        },
        "diagnostics": {
            "ingest_request_id": data.get("request_id"), "weather": weather_diag,
            "timestamps": {
                "device_event_timestamp_utc": format_utc_timestamp(data.get("calculated_event_timestamp")),
                "ingest_receive_timestamp_utc": format_utc_timestamp(data.get("received_at") or data.get("calculated_event_timestamp")),
            },
            "ingest_request_info": data.get("request_headers"), "ingest_warnings": data.get("warnings"),
            "device_state": {
                "screen_on": data.get('sc') == 1 if 'sc' in data else get_nested(base_state, ['diagnostics', 'device_state', 'screen_on']),
                "vpn_active": data.get('vp') == 1 if 'vp' in data else get_nested(base_state, ['diagnostics', 'device_state', 'vpn_active']),
                "network_metered": data.get('nm') == 1 if 'nm' in data else get_nested(base_state, ['diagnostics', 'device_state', 'network_metered']),
                "data_activity": DATA_ACTIVITY_MAP.get(data.get('da', get_nested(base_state, ['diagnostics', 'device_state', 'data_activity_code']))),
                "system_audio_state": SYSTEM_AUDIO_MAP.get(data.get('au', get_nested(base_state, ['diagnostics', 'device_state', 'system_audio_code']))),
                "camera_active": data.get('ca') == 1 if 'ca' in data else get_nested(base_state, ['diagnostics', 'device_state', 'camera_active']),
                "flashlight_on": data.get('fl') == 1 if 'fl' in data else get_nested(base_state, ['diagnostics', 'device_state', 'flashlight_on']),
                "phone_activity_state": PHONE_ACTIVITY_MAP.get(data.get('pa', get_nested(base_state, ['diagnostics', 'device_state', 'phone_activity_code'])))
            },
            "sensors": {
                "device_temperature_celsius": data.get('dt', get_nested(base_state, ['diagnostics', 'sensors', 'device_temperature_celsius'])), 
                "ambient_light_level": data.get('lx', get_nested(base_state, ['diagnostics', 'sensors', 'ambient_light_level'])),
                "barometer_hpa": data.get('pr', get_nested(base_state, ['diagnostics', 'sensors', 'barometer_hpa'])), 
                "steps_since_boot": data.get('st', get_nested(base_state, ['diagnostics', 'sensors', 'steps_since_boot'])),
                "proximity_near": data.get('px') == 0 if 'px' in data else get_nested(base_state, ['diagnostics', 'sensors', 'proximity_near']),
            },
            "wifi_details": {
                "wifi_frequency_channel": data.get('wf', get_nested(base_state, ['diagnostics', 'wifi_details', 'wifi_frequency_channel'])),
                "wifi_rssi_dbm": -safe_int(data.get('wr')) if 'wr' in data else get_nested(base_state, ['diagnostics', 'wifi_details', 'wifi_rssi_dbm']),
                "wifi_link_speed_quality_index": data.get('ws', get_nested(base_state, ['diagnostics', 'wifi_details', 'wifi_link_speed_quality_index'])),
                "wifi_standard": WIFI_STANDARD_MAP.get(data.get('wt', get_nested(base_state, ['diagnostics', 'wifi_details', 'wifi_standard_code'])))
            }
        },
        "app_settings": data.get('ad', base_state.get('app_settings', {}))
    }
    return transformed
