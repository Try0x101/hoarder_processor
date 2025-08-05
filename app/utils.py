import copy
import math
import datetime
import time
from enum import Enum
from typing import Dict, Any, Tuple, Optional
import redis.asyncio as redis
import geohash as pygeohash
import base64

GEOHASH_LEN_TO_METERS = {
    1: 5000000, 2: 1250000, 3: 156000, 4: 39000,
    5: 4900, 6: 1200, 7: 152, 8: 38,
    9: 5, 10: 1, 11: 1, 12: 1
}

OUI_VENDOR_MAP: Dict[str, str] = {}

def get_vendor_from_mac(mac_address: str) -> Optional[str]:
    if not mac_address or not isinstance(mac_address, str):
        return None
    try:
        oui = mac_address.replace(":", "").replace("-", "").upper()[:6]
        return OUI_VENDOR_MAP.get(oui)
    except Exception:
        return None

def get_nested(d: dict, keys: list, default: Any = None) -> Any:
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key)
        else:
            return default
    return d if d is not None else default

def decode_geohash(geohash_str: str) -> Optional[Tuple[float, float, int]]:
    if not geohash_str or not isinstance(geohash_str, str):
        return None
    try:
        lat, lon = pygeohash.decode(geohash_str)
        precision_meters = GEOHASH_LEN_TO_METERS.get(len(geohash_str), 5000000)
        return float(lat), float(lon), precision_meters
    except (ValueError, TypeError):
        return None

def decode_base62(b62_str: str) -> Optional[int]:
    if not b62_str or not isinstance(b62_str, str):
        return None
    
    BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    base = len(BASE62_ALPHABET)
    num = 0
    
    try:
        for char in b62_str:
            num = num * base + BASE62_ALPHABET.index(char)
    except ValueError:
        return None
        
    return num

def decode_bssid_base64(b64_str: str) -> Optional[str]:
    if not b64_str or not isinstance(b64_str, str):
        return None
    try:
        padded_b64_str = b64_str + '=' * (-len(b64_str) % 4)
        mac_bytes = base64.b64decode(padded_b64_str)
        if len(mac_bytes) != 6:
            return None
        return ":".join(f"{byte:02x}" for byte in mac_bytes).lower()
    except (base64.binascii.Error, ValueError):
        return None

def reconstruct_from_freshness(freshness_payload: Dict) -> Dict:
    simple_payload = {}
    if not isinstance(freshness_payload, dict):
        return freshness_payload

    for key, node in freshness_payload.items():
        if isinstance(node, dict):
            if "value" in node and "ts" in node:
                simple_payload[key] = node["value"]
            else:
                simple_payload[key] = reconstruct_from_freshness(node)
        else:
            simple_payload[key] = node
    return simple_payload

def _convert_to_freshness(payload: Dict, timestamp: str) -> Dict:
    fresh_payload = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            fresh_payload[key] = _convert_to_freshness(value, timestamp)
        elif value is not None:
            fresh_payload[key] = {"value": value, "ts": timestamp}
    return fresh_payload

def update_freshness_from_full_state(base_freshness: dict, new_full_simple: dict, new_ts: str) -> dict:
    new_freshness = {}
    all_keys = set(base_freshness.keys()) | set(new_full_simple.keys())

    for key in all_keys:
        new_simple_value = new_full_simple.get(key)
        base_node = base_freshness.get(key)

        if key not in new_full_simple:
            continue

        if isinstance(new_simple_value, dict):
            base_sub_freshness = base_node if isinstance(base_node, dict) and "value" not in base_node else {}
            updated_sub_freshness = update_freshness_from_full_state(base_sub_freshness, new_simple_value, new_ts)
            if updated_sub_freshness:
                new_freshness[key] = updated_sub_freshness
        else:
            old_value = base_node.get("value") if isinstance(base_node, dict) else None
            
            if not base_node or old_value != new_simple_value:
                new_freshness[key] = {"value": new_simple_value, "ts": new_ts}
            else:
                new_freshness[key] = base_node
                
    return new_freshness

def parse_freshness_payload(freshness_payload: Dict) -> Tuple[Dict, Dict]:
    now = datetime.datetime.now(datetime.timezone.utc)
    data_payload = {}
    freshness_info = {}

    if not isinstance(freshness_payload, dict):
        return {}, {}
        
    for key, node in freshness_payload.items():
        if isinstance(node, dict):
            if "value" in node and "ts" in node:
                data_payload[key] = node["value"]
                new_key = f"{key}_age_in_seconds"
                try:
                    ts_str = node["ts"]
                    if isinstance(ts_str, str):
                        naive_dt = datetime.datetime.fromisoformat(ts_str.replace(" ", "T"))
                        aware_dt = naive_dt.replace(tzinfo=datetime.timezone.utc)
                        age_seconds = (now - aware_dt).total_seconds()
                        freshness_info[new_key] = round(age_seconds)
                    else:
                        freshness_info[new_key] = -1
                except (ValueError, TypeError):
                    freshness_info[new_key] = -1
            else:
                sub_data, sub_freshness = parse_freshness_payload(node)
                if sub_data or key in freshness_payload:
                    data_payload[key] = sub_data
                if sub_freshness:
                    freshness_info[key] = sub_freshness
        else:
            data_payload[key] = node
            new_key = f"{key}_age_in_seconds"
            freshness_info[new_key] = "untracked"
            
    return data_payload, freshness_info

class WeatherRateLimiter:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.limit = 9000
        self.key = f"global_weather_limit:{datetime.date.today().isoformat()}"

    async def is_rate_limited(self) -> bool:
        if not self.redis:
            return True
        try:
            count = await self.redis.get(self.key)
            return int(count or 0) >= self.limit
        except (redis.RedisError, ValueError):
            return True

    async def increment(self):
        if not self.redis:
            return
        try:
            current_val = await self.redis.incr(self.key)
            if current_val == 1:
                await self.redis.expire(self.key, 86400)
        except redis.RedisError:
            pass

def deep_merge(source: dict, destination: dict) -> dict:
    result = copy.deepcopy(destination)
    for key, value in source.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(value, result[key])
        else:
            result[key] = value
    return result

def cleanup_empty(d: Any) -> Any:
    if isinstance(d, dict):
        temp_dict = {k: cleanup_empty(v) for k, v in d.items()}
        return {k: v for k, v in temp_dict.items() if v != '' and v != [] and v != {}}
    if isinstance(d, list):
        temp_list = [cleanup_empty(item) for item in d]
        return [v for v in temp_list if v is not None and v != '' and v != [] and v != {}]
    return d

def sort_dict_recursive(d: Any) -> Any:
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [sort_dict_recursive(v) for v in d]
    return {k: sort_dict_recursive(v) for k, v in sorted(d.items())}

def calculate_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    if None in [lat1, lon1, lat2, lon2]:
        return float('inf')
    try:
        lat1_rad, lon1_rad = math.radians(float(lat1)), math.radians(float(lon1))
        lat2_rad, lon2_rad = math.radians(float(lat2)), math.radians(float(lon2))
    except (ValueError, TypeError):
        return float('inf')
    dlat, dlon = lat2_rad - lat1_rad, lon2_rad - lon1_rad
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def diff_states(new_state: dict, old_state: dict) -> dict:
    delta = {}
    all_keys = set(new_state.keys()) | set(old_state.keys())

    for key in all_keys:
        new_val = new_state.get(key)
        old_val = old_state.get(key)

        if key not in old_state:
            if new_val is not None:
                delta[key] = new_val
        elif key not in new_state:
            delta[key] = None
        elif new_val is None and old_val is not None:
             delta[key] = None
        elif isinstance(new_val, dict) and isinstance(old_val, dict):
            sub_delta = diff_states(new_val, old_val)
            if sub_delta:
                delta[key] = sub_delta
        elif new_val != old_val:
            delta[key] = new_val
    return delta

def format_utc_timestamp(ts_str: str) -> str:
    if not ts_str or not isinstance(ts_str, str):
        return None
    try:
        dt = datetime.datetime.fromisoformat(ts_str.replace(" ", "T"))
        return dt.strftime('%d.%m.%Y %H:%M:%S UTC')
    except (ValueError, TypeError):
        return None

class BreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class SimpleCircuitBreaker:
    def __init__(self, name, failure_threshold=3, timeout=30):
        self.name, self.failure_threshold, self.timeout = name, failure_threshold, timeout
        self.failure_count, self.last_failure_time = 0, 0
        self.state = BreakerState.CLOSED

    async def call(self, func, *args, **kwargs):
        if self.state == BreakerState.OPEN:
            if time.time() - self.last_failure_time > self.timeout:
                self.state = BreakerState.HALF_OPEN
            else:
                raise Exception(f"{self.name} circuit breaker is OPEN")
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _on_success(self):
        self.failure_count = 0
        if self.state == BreakerState.HALF_OPEN:
            self.state = BreakerState.CLOSED

    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = BreakerState.OPEN
    
    def get_status(self):
        return {'name': self.name, 'state': self.state.value, 'failures': self.failure_count}

weather_breaker = SimpleCircuitBreaker("OpenMeteo", failure_threshold=3, timeout=30)
wttr_breaker = SimpleCircuitBreaker("WTTR", failure_threshold=2, timeout=20)
ip_api_breaker = SimpleCircuitBreaker("IP-API", failure_threshold=5, timeout=60)

APP_SETTINGS_KEY_MAP = {
    "av": "app_version_code", "dc": "data_collection_toggle", "su": "server_upload_toggle",
    "fc": "force_continuous", "p1": "continuous_power_mode", "p2": "optimized_power_mode",
    "p3": "passive_power_mode", "x1": "wifi_rssi_precision", "xa": "gps_altitude_precision",
    "xb": "battery_precision", "xc": "step_counter_precision", "xg": "gps_precision",
    "xl": "ambient_light_precision", "xn": "network_speed_precision", "xp": "barometer_precision",
    "xr": "cellular_rssi_precision", "xs": "speed_precision", "dm": "diagnostics_master_switch",
    "ea": "system_audio_toggle", "eb": "barometer_toggle", "ec": "charging_state_toggle",
    "ed": "cellular_data_activity_toggle", "ef": "cell_signal_quality_toggle",
    "eg": "timing_advance_toggle", "ek": "step_counter_toggle", "el": "ambient_light_toggle",
    "em": "network_metered_toggle", "ep": "power_save_toggle", "es": "screen_state_toggle",
    "et": "device_temp_toggle", "ev": "vpn_status_toggle", "ex": "camera_state_toggle",
    "ey": "flashlight_state_toggle", "w1": "wifi_rssi_toggle", "w2": "wifi_frequency_toggle",
    "w3": "wifi_link_speed_toggle", "w4": "wifi_standard_toggle", "w5": "wifi_name_ssid_toggle",
    "b1": "trigger_by_count", "b2": "trigger_by_timeout", "b3": "trigger_by_max_size",
    "bc": "batch_record_count", "be": "batching_toggle", "bl": "compression_level",
    "bs": "batch_max_size_kb", "bt": "batch_timeout_sec", "m1": "gps_permission_state",
    "m2": "phone_state_permission", "m3": "activity_recognition_permission",
    "m4": "post_notifications_permission", "q1": "barometer_sensor_state",
    "q2": "step_counter_sensor_state", "q3": "ambient_light_sensor_state",
    "q4": "proximity_sensor_state", "q5": "motion_detector_state", "bo": "battery_optimization_state",
    "c1": "calibrated_stationary_thresh", "c2": "calibrated_moving_thresh"
}

def rename_app_settings_freshness_keys(freshness_dict: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(freshness_dict, dict):
        return freshness_dict
    
    renamed_dict = {}
    for key, value in freshness_dict.items():
        if key.endswith('_age_in_seconds'):
            short_key = key[:-len('_age_in_seconds')]
            long_key = APP_SETTINGS_KEY_MAP.get(short_key, short_key)
            renamed_dict[f"{long_key}_age_in_seconds"] = value
        else:
            renamed_dict[key] = value
    return renamed_dict

def group_and_rename_app_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(settings, dict):
        return settings
    
    s = settings
    
    PERMISSION_MAP = {0: "Denied", 1: "Foreground (While-in-use)", 2: "Background (All the time)"}
    BOOL_PERMISSION_MAP = {0: "Not Granted", 1: "Granted"}
    SENSOR_HEALTH_MAP = {1: "Not Available", 2: "OK", 3: "Stale", 4: "Quarantined"}
    MOTION_DETECTOR_MAP = {2: "OK", 3: "Stale"}
    BATTERY_OPTIMIZATION_MAP = {0: "Unrestricted", 1: "Optimized", 2: "Restricted"}
    PRECISION_MAP = {
        'x1': {0:"Smart", 1:"Max", 2:"3dBm", 3:"5dBm"},
        'xa': {0:"Smart", 1:"Max", 2:"2m", 3:"10m", 4:"25m", 5:"50m", 6:"100m"},
        'xb': {0:"Smart", 1:"Max", 2:"2%", 3:"5%", 4:"10%"},
        'xc': {0:"Smart", 1:"Max", 2:"10 steps", 3:"100 steps", 4:"1000 steps"},
        'xg': {0:"Smart", 1:"Max", 2:"20m", 3:"100m", 4:"1km", 5:"10km"},
        'xl': {0:"Smart", 1:"Max", 2:"1-lux", 3:"10-lux", 4:"100-lux"},
        'xn': {0:"Smart", 1:"Max", 2:"1Mbps", 3:"2Mbps", 4:"5Mbps"},
        'xp': {0:"Smart", 1:"Max", 2:"0.1hPa", 3:"1hPa", 4:"10hPa"},
        'xr': {0:"Smart", 1:"Max", 2:"3dBm", 3:"5dBm", 4:"10dBm"},
        'xs': {0:"Smart", 1:"Max", 2:"1km/h", 3:"3km/h", 4:"5km/h", 5:"10km/h"}
    }
    
    grouped = {
        "general": {
            "app_version_code": s.get("av"), "data_collection_enabled": s.get("dc") == 1, "server_upload_enabled": s.get("su") == 1
        },
        "power_management": {
            "power_modes": {
                "force_continuous": s.get("fc") == 1, "continuous": s.get("p1") == 1,
                "optimized": s.get("p2") == 1, "passive": s.get("p3") == 1
            },
            "battery_optimization_state": BATTERY_OPTIMIZATION_MAP.get(s.get("bo"))
        },
        "batching_and_upload": {
            "batching_enabled": s.get("be") == 1, "compression_level": s.get("bl"),
            "triggers": {
                "by_record_count": s.get("b1") == 1, "by_timeout": s.get("b2") == 1, "by_max_size": s.get("b3") == 1
            },
            "trigger_values": {
                "record_count": s.get("bc"), "timeout_seconds": s.get("bt"), "max_size_kb": s.get("bs")
            }
        },
        "precision_controls": {
            "wifi_signal_strength": PRECISION_MAP.get('x1', {}).get(s.get('x1')),
            "gps_altitude": PRECISION_MAP.get('xa', {}).get(s.get('xa')),
            "battery_level": PRECISION_MAP.get('xb', {}).get(s.get('xb')),
            "step_counter": PRECISION_MAP.get('xc', {}).get(s.get('xc')),
            "gps_coordinates": PRECISION_MAP.get('xg', {}).get(s.get('xg')),
            "ambient_light": PRECISION_MAP.get('xl', {}).get(s.get('xl')),
            "network_speed": PRECISION_MAP.get('xn', {}).get(s.get('xn')),
            "barometer": PRECISION_MAP.get('xp', {}).get(s.get('xp')),
            "cellular_signal_strength": PRECISION_MAP.get('xr', {}).get(s.get('xr')),
            "speed": PRECISION_MAP.get('xs', {}).get(s.get('xs'))
        },
        "diagnostics_toggles": {
            "master_switch": s.get("dm") == 1,
            "general_state": {
                "system_audio": s.get("ea") == 1, "charging_state": s.get("ec") == 1, "data_activity": s.get("ed") == 1,
                "network_metered": s.get("em") == 1, "power_save_mode": s.get("ep") == 1, "screen_state": s.get("es") == 1,
                "device_temperature": s.get("et") == 1, "vpn_status": s.get("ev") == 1, "camera_state": s.get("ex") == 1,
                "flashlight_state": s.get("ey") == 1
            },
            "sensor_state": {
                "barometer": s.get("eb") == 1, "cell_signal_quality": s.get("ef") == 1, "timing_advance": s.get("eg") == 1,
                "step_counter": s.get("ek") == 1, "ambient_light": s.get("el") == 1
            },
            "wifi_details": {
                "signal_strength": s.get("w1") == 1, "frequency": s.get("w2") == 1, "link_speed": s.get("w3") == 1,
                "standard": s.get("w4") == 1, "ssid": s.get("w5") == 1
            }
        },
        "system_status": {
            "permissions": {
                "gps": PERMISSION_MAP.get(s.get("m1")), "phone_state": BOOL_PERMISSION_MAP.get(s.get("m2")),
                "activity_recognition": BOOL_PERMISSION_MAP.get(s.get("m3")), "post_notifications": BOOL_PERMISSION_MAP.get(s.get("m4"))
            },
            "sensor_health": {
                "barometer": SENSOR_HEALTH_MAP.get(s.get("q1")), "step_counter": SENSOR_HEALTH_MAP.get(s.get("q2")),
                "ambient_light": SENSOR_HEALTH_MAP.get(s.get("q3")), "proximity": SENSOR_HEALTH_MAP.get(s.get("q4")),
                "motion_detector": MOTION_DETECTOR_MAP.get(s.get("q5"))
            },
            "calibration": {
                "stationary_threshold_variance": s.get("c1"), "moving_threshold_variance": s.get("c2")
            }
        }
    }
    return cleanup_empty(grouped)