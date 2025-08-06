import math
import datetime
from collections import deque
from typing import Dict, Any, Tuple, Optional, List

# Constants
PROFILE_HISTORY_LENGTH = 30
STABLE_AGL_THRESHOLD_METERS = 4.0
FLOOR_HEIGHT_METERS = 3.0
REFERENCE_PRESSURE_EXPIRATION_SECONDS = 2 * 3600
BARO_ALTITUDE_CONSTANT = 44330.0
BARO_ALTITUDE_EXPONENT = 1 / 5.255
GPS_ERROR_MARGIN_FOR_GROUND_LOCK_METERS = 50.0

def _initialize_profile() -> Dict[str, Any]:
    return {
        "surface_altitude_history": [],
        "ground_reference_pressure_hpa": None,
        "last_ground_reference_ts": None,
    }

def _calculate_barometer_altitude(device_pressure: float, sea_level_pressure: float) -> Optional[float]:
    if not all([isinstance(p, (int, float)) for p in [device_pressure, sea_level_pressure]]) or device_pressure <= 0 or sea_level_pressure <= 0:
        return None
    try:
        pressure_ratio = device_pressure / sea_level_pressure
        altitude = BARO_ALTITUDE_CONSTANT * (1.0 - math.pow(pressure_ratio, BARO_ALTITUDE_EXPONENT))
        return altitude
    except (ValueError, TypeError):
        return None

def analyze_altitude(
    current_payload: Dict[str, Any],
    previous_profile: Optional[Dict[str, Any]]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    
    profile = previous_profile or _initialize_profile()
    analysis = {
        "altitude_above_ground_level_meters": None,
        "height_above_surface_meters": None,
        "relative_altitude_change_meters": None,
        "estimated_floor": None,
        "altitude_source": "Unknown",
    }

    location = current_payload.get("location", {})
    sensors = current_payload.get("sensors", {})
    environment = current_payload.get("environment", {})
    
    altitude_asl = location.get("altitude_in_meters")
    ground_elevation_asl = location.get("elevation_in_meters")
    gps_accuracy = location.get("accuracy_in_meters")
    device_pressure = sensors.get("device_barometer_hpa")
    sea_level_pressure = environment.get("weather", {}).get("pressure_in_hpa")
    phone_activity = current_payload.get("device_state", {}).get("phone_activity_state")
    event_ts = current_payload.get("diagnostics", {}).get("timestamps", {}).get("device_event_timestamp_utc")

    baro_altitude_asl = _calculate_barometer_altitude(device_pressure, sea_level_pressure)

    reliable_altitude_asl = baro_altitude_asl if baro_altitude_asl is not None else altitude_asl
    if reliable_altitude_asl is baro_altitude_asl:
        analysis["altitude_source"] = "Barometer-Only"
    elif reliable_altitude_asl is altitude_asl:
        analysis["altitude_source"] = "GPS+Baro (Fused)"

    if reliable_altitude_asl is not None and ground_elevation_asl is not None:
        analysis["altitude_above_ground_level_meters"] = round(reliable_altitude_asl - ground_elevation_asl, 1)

    blended_surface_altitude = altitude_asl if gps_accuracy is not None and gps_accuracy < 75 and altitude_asl is not None else baro_altitude_asl
    
    history = deque(profile.get("surface_altitude_history", []), maxlen=PROFILE_HISTORY_LENGTH)
    if blended_surface_altitude is not None:
        history.append(blended_surface_altitude)
        profile["surface_altitude_history"] = list(history)

    if history:
        dynamic_surface_altitude = sum(history) / len(history)
        if blended_surface_altitude is not None:
            analysis["height_above_surface_meters"] = round(blended_surface_altitude - dynamic_surface_altitude, 1)

    is_grounded_heuristic = False
    if all(v is not None for v in [altitude_asl, ground_elevation_asl]):
        if abs(altitude_asl - ground_elevation_asl) < GPS_ERROR_MARGIN_FOR_GROUND_LOCK_METERS:
            is_grounded_heuristic = True

    if phone_activity == "Stable" and is_grounded_heuristic and device_pressure is not None:
        profile["ground_reference_pressure_hpa"] = device_pressure
        profile["last_ground_reference_ts"] = event_ts

    ref_pressure = profile.get("ground_reference_pressure_hpa")
    ref_ts_str = profile.get("last_ground_reference_ts")

    if ref_pressure is not None and ref_ts_str and event_ts:
        try:
            ref_dt = datetime.datetime.strptime(ref_ts_str, '%d.%m.%Y %H:%M:%S UTC').replace(tzinfo=datetime.timezone.utc)
            evt_dt = datetime.datetime.strptime(event_ts, '%d.%m.%Y %H:%M:%S UTC').replace(tzinfo=datetime.timezone.utc)
            if (evt_dt - ref_dt).total_seconds() > REFERENCE_PRESSURE_EXPIRATION_SECONDS:
                profile["ground_reference_pressure_hpa"] = None
                profile["last_ground_reference_ts"] = None
                ref_pressure = None
        except (ValueError, TypeError):
             ref_pressure = None

    if ref_pressure is not None and device_pressure is not None:
        pressure_delta = ref_pressure - device_pressure
        relative_height = pressure_delta * 8.3
        analysis["relative_altitude_change_meters"] = round(relative_height, 1)
        if abs(relative_height) > 1.5:
            analysis["estimated_floor"] = int(round(relative_height / FLOOR_HEIGHT_METERS))
        else:
            analysis["estimated_floor"] = 0

    return analysis, profile
