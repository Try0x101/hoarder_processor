import copy
import math
import datetime
from typing import Dict

def deep_merge(source: dict, destination: dict) -> dict:
    result = copy.deepcopy(destination)
    for key, value in source.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(value, result[key])
        else:
            result[key] = value
    return result

def cleanup_empty(d: Dict) -> Dict:
    if not isinstance(d, dict):
        return d
    return {
        k: v for k, v in ((k, cleanup_empty(v)) for k, v in d.items())
        if v is not None and v != ''
    }

def calculate_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    if None in [lat1, lon1, lat2, lon2]:
        return float('inf')

    try:
        lat1_rad = math.radians(float(lat1))
        lon1_rad = math.radians(float(lon1))
        lat2_rad = math.radians(float(lat2))
        lon2_rad = math.radians(float(lon2))
    except (ValueError, TypeError):
        return float('inf')

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance

def diff_states(new_state: dict, old_state: dict) -> dict:
    delta = {}
    for key, new_val in new_state.items():
        if key not in old_state:
            delta[key] = new_val
        elif isinstance(new_val, dict) and isinstance(old_state.get(key), dict):
            sub_delta = diff_states(new_val, old_state[key])
            if sub_delta:
                delta[key] = sub_delta
        elif new_val != old_state.get(key):
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
