import orjson
import datetime
from typing import Dict, Any, Optional

def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def _to_unix_timestamp(ts_str: Optional[str]) -> Optional[int]:
    if not ts_str:
        return None
    try:
        naive_dt = datetime.datetime.fromisoformat(ts_str.replace(" ", "T"))
        aware_dt = naive_dt.replace(tzinfo=datetime.timezone.utc)
        return int(aware_dt.timestamp())
    except (ValueError, TypeError):
        return None

def _get_decimal_places_from_meters(meters: Optional[int]) -> int:
    if meters is None:
        return 7
    if meters > 1000:
        return 3
    if meters > 100:
        return 4
    if meters > 5:
        return 5
    if meters > 0:
        return 6
    return 7

def process_row_to_geojson(db_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        payload = orjson.loads(db_row["enriched_payload"])
        
        location = payload.get("location", {})
        lon = _safe_float(location.get("longitude"))
        lat = _safe_float(location.get("latitude"))

        if lon is None or lat is None:
            return None

        precision_meters = location.get("geohash_precision_in_meters")
        decimal_places = _get_decimal_places_from_meters(precision_meters)
        
        identity = payload.get("identity", {})
        network = payload.get("network", {})
        power = payload.get("power", {})
        wifi = network.get("wifi", {})
        cellular = network.get("cellular", {})
        signal = cellular.get("signal", {})
        strength = signal.get("strength", {})
        device_state = payload.get("device_state", {})
        sensors = payload.get("sensors", {})
        environment = payload.get("environment", {})
        weather = environment.get("weather", {})
        
        device_id = identity.get("device_id")
        device_name = identity.get("device_name")
        device_id_name = f"{device_id} {device_name}" if device_id and device_name else device_id

        properties = {
            "internal_id": db_row["id"],
            "device_id_name": device_id_name,
            "data_timestamp": _to_unix_timestamp(db_row.get("calculated_event_timestamp")),
            "latitude": round(lat, decimal_places),
            "longitude": round(lon, decimal_places),
            "gps_altitude_in_meters": location.get("altitude_in_meters"),
            "gps_accuracy_in_meters": location.get("accuracy_in_meters"),
            "geohash_precision_in_meters": precision_meters,
            "location_actual_timezone": location.get("location_actual_timezone"),
            "speed_in_kmh": location.get("speed_in_kmh"),
            "source_ip": network.get("source_ip"),
            "active_network": network.get("currently_used_active_network"),
            "cell_network_type": cellular.get("type"),
            "cell_operator": cellular.get("operator"),
            "signal_strength_in_dbm": strength.get("value_dbm"),
            "link_speed_mbps_range": wifi.get("link_speed_mbps_range"),
            "battery_percent": power.get("battery_percent"),
            "phone_activity_state": device_state.get("phone_activity_state"),
            "screen_on": device_state.get("screen_on"),
            "device_proximity_sensor_closer_than_5cm": sensors.get("device_proximity_sensor_closer_than_5cm"),
            "device_temperature_celsius": sensors.get("device_temperature_celsius"),
            "device_ambient_light_level": sensors.get("device_ambient_light_level"),
            "device_ambient_light_lux_range": sensors.get("device_ambient_light_lux_range"),
            "device_barometer_hpa": sensors.get("device_barometer_hpa"),
            "device_steps_since_boot": sensors.get("device_steps_since_boot"),
            "temperature_in_celsius": weather.get("temperature_in_celsius"),
            "wind_chill_in_celsius": weather.get("wind_chill_in_celsius"),
        }

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": {k: v for k, v in properties.items() if v is not None}
        }
        
        return feature
    except (orjson.JSONDecodeError, KeyError, TypeError):
        return None
