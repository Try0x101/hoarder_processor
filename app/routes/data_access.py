import aiosqlite
import os
import orjson
import datetime
from fastapi import APIRouter, Request, HTTPException, Query, Depends
from typing import Optional, List, Dict, Any
from urllib.parse import quote_plus
from app.utils import (
    diff_states, format_utc_timestamp, cleanup_empty, parse_freshness_payload, 
    reconstruct_from_freshness, sort_dict_recursive, group_and_rename_app_settings,
    rename_app_settings_freshness_keys, get_nested
)
from app.database import DB_PATH
from app.security import get_current_user

router = APIRouter(
    prefix="/data", 
    tags=["Data Access"],
    dependencies=[Depends(get_current_user)]
)

KEY_ORDERS = {
    'data': ['identity', 'location', 'power', 'device_state', 'sensors', 'network', 'environment', 'app_settings', 'diagnostics'],
    'identity': ['device_name', 'device_id'],
    'location': ['latitude', 'longitude', 'altitude_in_meters', 'elevation_in_meters', 'accuracy_in_meters', 'speed_in_kmh', 'geohash_precision_in_meters', 'location_actual_timezone'],
    'power': ['battery_percent', 'capacity_in_mah', 'calculated_leftover_capacity_in_mah', 'charging_state', 'power_save_mode'],
    'device_state': ['screen_on', 'vpn_active', 'network_metered', 'data_activity', 'system_audio_state', 'camera_active', 'flashlight_on', 'phone_activity_state'],
    'sensors': ['device_temperature_celsius', 'device_ambient_light_level', 'device_ambient_light_lux_range', 'device_barometer_hpa', 'device_steps_since_boot', 'device_proximity_sensor_closer_than_5cm'],
    'network': ['currently_used_active_network', 'source_ip', 'wifi', 'bandwidth', 'cellular'],
    'wifi': ['ssid', 'bssid', 'frequency_channel', 'frequency_band', 'rssi_dbm', 'link_speed_quality_index', 'link_speed_mbps_range', 'standard'],
    'bandwidth': ['download_in_mbps', 'upload_in_mbps'],
    'cellular': ['type', 'operator', 'signal', 'mcc', 'mnc', 'cell_id', 'tac', 'timing_advance'],
    'signal': ['strength', 'quality'],
    'strength': ['metric', 'value_dbm', 'unit'],
    'quality': ['metric', 'value', 'unit'],
    'environment': ['weather', 'precipitation', 'wind', 'marine', 'solar', 'air_quality'],
    'weather': ['temperature_in_celsius', 'feels_like_in_celsius', 'wind_chill_in_celsius', 'description', 'assessment', 'humidity_percent', 'pressure_in_hpa', 'cloud_cover_percent'],
    'precipitation': ['type', 'intensity', 'summary'],
    'wind': ['speed_in_meters_per_second', 'gusts_in_meters_per_second', 'direction', 'description'],
    'solar': ['sunrise', 'sunset'],
    'air_quality': ['us_aqi', 'assessment', 'pm2_5', 'carbon_monoxide', 'nitrogen_dioxide', 'sulphur_dioxide', 'ozone'],
    'diagnostics': ['timestamps', 'weather', 'ingest_request_id', 'ingest_request_info', 'ingest_warnings', 'data_freshness'],
    'app_settings': ['general', 'power_management', 'batching_and_upload', 'precision_controls', 'diagnostics_toggles', 'system_status'],
    'power_management': ['power_modes', 'battery_optimization_state'],
    'batching_and_upload': ['batching_enabled', 'compression_level', 'triggers', 'trigger_values'],
    'diagnostics_toggles': ['master_switch', 'general_state', 'sensor_state', 'wifi_details'],
    'system_status': ['permissions', 'sensor_health', 'calibration']
}

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

def _apply_custom_sorting(data: Any, level_key: str = 'data') -> Any:
    if not isinstance(data, dict):
        return [_apply_custom_sorting(item, level_key) for item in data] if isinstance(data, list) else data

    order = KEY_ORDERS.get(level_key, [])
    sorted_dict = {}

    for key in order:
        if key in data:
            sorted_dict[key] = _apply_custom_sorting(data[key], key)
            
    remaining_keys = sorted([k for k in data if k not in order])
    for key in remaining_keys:
        sorted_dict[key] = _apply_custom_sorting(data[key], key)

    return sorted_dict

def build_base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"

async def query_recent_devices(limit: int = 10) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            query = """
                SELECT
                    les.device_id,
                    les.last_updated_ts,
                    les.enriched_payload,
                    COALESCE((SELECT COUNT(*) FROM enriched_telemetry et WHERE et.device_id = les.device_id), 0) as total_records,
                    COALESCE((SELECT SUM(et.request_size_bytes) FROM enriched_telemetry et WHERE et.device_id = les.device_id), 0) as total_bytes,
                    (SELECT MIN(et.calculated_event_timestamp) FROM enriched_telemetry et WHERE et.device_id = les.device_id) as first_seen_ts
                FROM latest_enriched_state les
                ORDER BY les.last_updated_ts DESC
                LIMIT ?
            """
            cursor = await db.execute(query, (limit,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        print(f"ERROR QUERYING RECENT DEVICES: {e}")
        return []

async def get_enriched_history(device_id: Optional[str], limit: int, cursor_ts: Optional[str], cursor_id: Optional[int]) -> List[Dict[str, Any]]:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            query_parts = ["SELECT id, original_ingest_id, enriched_payload, calculated_event_timestamp FROM enriched_telemetry"]
            params = []
            conditions = []
            if device_id:
                conditions.append("device_id = ?")
                params.append(device_id)
            if cursor_ts and cursor_id is not None:
                conditions.append("(calculated_event_timestamp, id) < (?, ?)")
                params.extend([cursor_ts, cursor_id])
            if conditions:
                query_parts.append("WHERE " + " AND ".join(conditions))
            query_parts.append("ORDER BY calculated_event_timestamp DESC, id DESC LIMIT ?")
            params.append(limit + 1)
            
            cursor = await db.execute(" ".join(query_parts), tuple(params))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception:
        return []

@router.get("/history")
async def get_device_history(
    request: Request,
    device_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    cursor: Optional[str] = Query(None)
):
    base_url = build_base_url(request)
    cursor_ts, cursor_id = None, None
    if cursor:
        try:
            parts = cursor.split(',')
            if len(parts) == 2:
                cursor_ts, cursor_id = parts[0], int(parts[1])
        except (ValueError, IndexError):
            raise HTTPException(status_code=400, detail="Invalid cursor format.")

    history_rows = await get_enriched_history(device_id, limit, cursor_ts, cursor_id)
    
    processed_payloads = []
    for row in history_rows:
        payload = orjson.loads(row['enriched_payload'])
        
        if 'app_settings' in payload:
            payload['app_settings'] = group_and_rename_app_settings(payload.get('app_settings', {}))
        
        precision_meters = get_nested(payload, ['location', 'geohash_precision_in_meters'])
        if precision_meters is not None and 'location' in payload and payload.get('location'):
            decimal_places = _get_decimal_places_from_meters(precision_meters)
            location = payload['location']
            if 'latitude' in location and location['latitude'] is not None:
                location['latitude'] = round(location['latitude'], decimal_places)
            if 'longitude' in location and location['longitude'] is not None:
                location['longitude'] = round(location['longitude'], decimal_places)
        
        processed_payloads.append(payload)

    data_with_deltas = []
    if processed_payloads:
        for i, current_payload in enumerate(processed_payloads[:limit]):
            previous_payload = processed_payloads[i + 1] if (i + 1) < len(processed_payloads) else None
            
            changes = diff_states(current_payload, previous_payload) if previous_payload else current_payload

            if get_nested(changes, ['environment', 'solar']):
                changes['environment']['solar'].pop('sunrise_utc', None)
                changes['environment']['solar'].pop('sunset_utc', None)

            original_payload = orjson.loads(history_rows[i]['enriched_payload'])
            current_diagnostics = original_payload.get("diagnostics", {})
            event_diagnostics = {
                "ingest_request_id": current_diagnostics.get("ingest_request_id"),
                "timestamps": current_diagnostics.get("timestamps"),
            }
            
            changes.pop("diagnostics", None)

            data_with_deltas.append({
                "id": history_rows[i]['id'],
                "original_ingest_id": history_rows[i].get('original_ingest_id'),
                "changes": _apply_custom_sorting(cleanup_empty(changes)),
                "diagnostics": cleanup_empty(event_diagnostics)
            })

    base_params = [f"limit={limit}"]
    if device_id: base_params.append(f"device_id={device_id}")

    self_params = base_params[:]
    if cursor: self_params.append(f"cursor={quote_plus(cursor)}")
    self_url = f"{base_url}/data/history?{'&'.join(self_params)}"

    navigation = {"root": f"{base_url}/"}
    if device_id:
        navigation["latest"] = f"{base_url}/data/latest/{device_id}"
    if cursor:
        navigation["first_page"] = f"{base_url}/data/history?{'&'.join(base_params)}"

    next_cursor_obj = None
    if len(history_rows) > limit:
        last_record = history_rows[limit-1]
        next_ts, next_id = last_record['calculated_event_timestamp'], last_record['id']
        raw_cursor = f"{next_ts},{next_id}"
        navigation["next_page"] = f"{base_url}/data/history?{'&'.join(base_params + [f'cursor={quote_plus(raw_cursor)}'])}"
        next_cursor_obj = {
            "raw": raw_cursor,
            "timestamp": format_utc_timestamp(next_ts),
            "id": next_id
        }
    
    time_range = {}
    if history_rows and data_with_deltas:
        start_ts = history_rows[0]['calculated_event_timestamp']
        end_ts = history_rows[len(data_with_deltas)-1]['calculated_event_timestamp']
        time_range = {"start": format_utc_timestamp(start_ts), "end": format_utc_timestamp(end_ts)}

    pagination = {
        "limit": limit,
        "records_returned": len(data_with_deltas),
        "next_cursor": next_cursor_obj,
        "time_range": time_range
    }

    return {
        "request": {"self_url": self_url},
        "navigation": navigation,
        "pagination": cleanup_empty(pagination),
        "data": data_with_deltas
    }

@router.get("/latest/{device_id}")
async def get_latest_device_data(request: Request, device_id: str):
    base_url = build_base_url(request)
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT enriched_payload FROM latest_enriched_state WHERE device_id = ?", (device_id,))
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"No state found for device '{device_id}'.")
            
            freshness_payload = orjson.loads(row['enriched_payload'])
            data_payload, freshness_info = parse_freshness_payload(freshness_payload)

            if 'location' in data_payload and data_payload.get('location'):
                precision_meters = get_nested(data_payload, ['location', 'geohash_precision_in_meters'])
                if precision_meters is not None:
                    decimal_places = _get_decimal_places_from_meters(precision_meters)
                    lat = data_payload['location'].get('latitude')
                    lon = data_payload['location'].get('longitude')
                    if lat is not None:
                        data_payload['location']['latitude'] = round(lat, decimal_places)
                    if lon is not None:
                        data_payload['location']['longitude'] = round(lon, decimal_places)

            if 'app_settings' in data_payload and isinstance(data_payload['app_settings'], dict):
                data_payload['app_settings'] = group_and_rename_app_settings(data_payload['app_settings'])
            
            if 'app_settings' in freshness_info and isinstance(freshness_info['app_settings'], dict):
                freshness_info['app_settings'] = rename_app_settings_freshness_keys(freshness_info['app_settings'])

            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if 'diagnostics' in freshness_info and 'weather' in freshness_info['diagnostics']:
                weather_freshness = freshness_info['diagnostics']['weather']
                weather_freshness.pop('weather_data_old_age_in_seconds', None)
                weather_freshness.pop('weather_distance_from_actual_location_age_in_seconds', None)
                weather_freshness.pop('weather_request_timestamp_location_time_age_in_seconds', None)
                
                weather_ts_str = get_nested(data_payload, ['diagnostics', 'weather', 'weather_request_timestamp_utc'])
                if weather_ts_str:
                    try:
                        weather_dt = datetime.datetime.strptime(weather_ts_str, '%d.%m.%Y %H:%M:%S UTC')
                        weather_dt = weather_dt.replace(tzinfo=datetime.timezone.utc)
                        actual_age_sec = (now_utc - weather_dt).total_seconds()
                        weather_freshness['weather_data_actual_age_in_seconds'] = round(actual_age_sec)
                    except (ValueError, TypeError):
                        pass
                
                weather_freshness.pop('weather_request_timestamp_utc_age_in_seconds', None)

                if not weather_freshness:
                    freshness_info['diagnostics'].pop('weather', None)

            diagnostics_block = data_payload.pop("diagnostics", {})
            diagnostics_block["data_freshness"] = cleanup_empty(freshness_info)
            
            sorted_data = _apply_custom_sorting(data_payload)
            
            diag_order = ['timestamps', 'weather', 'ingest_request_id', 'ingest_request_info', 'ingest_warnings', 'data_freshness']
            weather_diag_order = ['weather_distance_from_actual_location', 'weather_fetch_location', 'weather_data_old', 'weather_request_timestamp_location_time', 'weather_request_timestamp_utc']

            sorted_diagnostics = {}
            for key in diag_order:
                if key in diagnostics_block:
                    value = diagnostics_block[key]
                    if key == 'weather' and isinstance(value, dict):
                        sorted_weather = {}
                        for w_key in weather_diag_order:
                            if w_key in value:
                                sorted_weather[w_key] = value[w_key]
                        for w_key in sorted(value):
                            if w_key not in sorted_weather:
                                sorted_weather[w_key] = value[w_key]
                        sorted_diagnostics[key] = sorted_weather
                    else:
                        sorted_diagnostics[key] = sort_dict_recursive(value)

            for key in sorted(diagnostics_block):
                if key not in sorted_diagnostics:
                    sorted_diagnostics[key] = sort_dict_recursive(diagnostics_block[key])
            
            sorted_data['diagnostics'] = sorted_diagnostics
            
            if get_nested(sorted_data, ['environment', 'solar']):
                sorted_data['environment']['solar'].pop('sunrise_utc', None)
                sorted_data['environment']['solar'].pop('sunset_utc', None)

            return {
                "request": {"self_url": f"{base_url}/data/latest/{device_id}"},
                "navigation": {
                    "root": f"{base_url}/",
                    "history": f"{base_url}/data/history?device_id={device_id}&limit=50"
                },
                "data": sorted_data
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve latest data: {e}")

@router.get("/devices")
async def get_devices_endpoint(request: Request, limit: int = 20):
    base_url = build_base_url(request)
    devices = await query_recent_devices(limit=limit)
    
    processed_devices = []
    for device in devices:
        freshness_payload = orjson.loads(device['enriched_payload'])
        payload = reconstruct_from_freshness(freshness_payload)
        processed_devices.append({
            "device_id": device['device_id'],
            "device_name": payload.get("identity", {}).get("device_name"),
            "client_ip": payload.get("network", {}).get("source_ip"),
            "last_seen": format_utc_timestamp(device['last_updated_ts']),
            "total_records": device['total_records'],
            'links': {
                'latest': f"{base_url}/data/latest/{device['device_id']}",
                'history': f"{base_url}/data/history?device_id={device['device_id']}&limit=50"
            }
        })

    self_url = f"{base_url}/data/devices?limit={limit}"
    return {
        "request": {"self_url": self_url},
        "navigation": {"root": f"{base_url}/"},
        "data": processed_devices
    }
