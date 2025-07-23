import orjson
from typing import Dict, Any, Optional

def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def process_row_to_geojson(db_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        payload = orjson.loads(db_row["enriched_payload"])
        
        location = payload.get("location", {})
        lon = _safe_float(location.get("longitude"))
        lat = _safe_float(location.get("latitude"))

        if lon is None or lat is None:
            return None

        identity = payload.get("identity", {})
        network = payload.get("network", {})
        power = payload.get("power", {})
        cellular = network.get("cellular", {})

        properties = {
            "latitude": lat,
            "longitude": lon,
            "internal_id": db_row["id"],
            "device_id": identity.get("device_id"),
            "device_name": identity.get("device_name"),
            "timestamp_utc": db_row["calculated_event_timestamp"],
            "active_network": network.get("currently_used_active_network"),
            "operator": cellular.get("operator"),
            "battery_percent": power.get("battery_percent"),
            "speed_in_kmh": location.get("speed_in_kmh"),
            "altitude_in_meters": location.get("altitude_in_meters"),
            "accuracy_in_meters": location.get("accuracy_in_meters"),
            "signal_strength_in_dbm": cellular.get("signal_strength_in_dbm"),
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
