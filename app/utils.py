import copy
import math
import datetime
import time
from enum import Enum
from typing import Dict, Any, Tuple, Optional
import redis.asyncio as redis

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

def merge_and_update_freshness(base_freshness: Dict, new_simple: Dict, timestamp: str) -> Dict:
    if not base_freshness:
        return _convert_to_freshness(new_simple, timestamp)

    result = copy.deepcopy(base_freshness)
    
    for key, new_value in new_simple.items():
        if new_value is None:
            if key in result:
                del result[key]
            continue
            
        if isinstance(new_value, dict):
            base_node = result.get(key)
            if isinstance(base_node, dict) and "value" not in base_node:
                result[key] = merge_and_update_freshness(base_node, new_value, timestamp)
            else:
                result[key] = _convert_to_freshness(new_value, timestamp)
        else:
            result[key] = {"value": new_value, "ts": timestamp}

    return result

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
                if sub_data:
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

def cleanup_empty(d: Dict) -> Dict:
    if not isinstance(d, dict):
        return d
    return {
        k: v for k, v in ((k, cleanup_empty(v)) for k, v in d.items())
        if v is not None and v != ''
    }

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
            delta[key] = new_val
        elif key not in new_state:
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
