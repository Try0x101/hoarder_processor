import copy
import math
import datetime
import time
from enum import Enum
from typing import Dict, Any

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

class BreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class SimpleCircuitBreaker:
    def __init__(self, name, failure_threshold=3, timeout=30):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0
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
