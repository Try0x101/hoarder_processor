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
        environment = payload.get("environment", {})

        properties = {
            "internal_id": db_row["id"],
            "original_ingest_id": db_row["original_ingest_id"],
            "device_id": identity.get("device_id"),
            "device_name": identity.get("device_name"),
            "timestamp_utc": db_row["calculated_event_timestamp"],
            "active_network": network.get("active_network"),
            "network_type": network.get("type"),
            "operator": network.get("operator"),
            "wifi_bssid": network.get("wifi_bssid"),
            "battery_percent": power.get("battery_percent"),
            "weather_description": environment.get("weather", {}).get("description"),
            "temperature": environment.get("weather", {}).get("temperature"),
            "source_ip": network.get("source_ip"),
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
