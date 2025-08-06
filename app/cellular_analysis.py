import statistics
from collections import deque
from typing import Dict, Any, Tuple, Optional, List

PROFILE_HISTORY_LENGTH = 20
VOLATILITY_SAMPLES = 10
CRITICAL_THROUGHPUT_MBPS = 0.5
CELL_EDGE_STRENGTH_DBM = -115.0
CELL_EDGE_EFFECTIVE_LINK_SPEED_CAP_MBPS = 4.0

class ConnectionState:
    STABLE = "Stable"
    DEGRADING = "Degrading"
    CRITICAL = "Critical"
    UNSTABLE = "Unstable"
    HANDOFF = "Handoff"
    LIMITED = "Limited"
    UNKNOWN = "Unknown"

def _get_quality_derating_factor(abs_quality: float, cell_type: str) -> float:
    if cell_type == "NR(5G)": # SS-SINR (higher is better)
        if abs_quality > 20: return 0.90
        if abs_quality > 10: return 0.70
        if abs_quality > 0: return 0.45
        return 0.20
    else: # RSRQ for LTE (lower absolute value is better)
        if abs_quality < 10: return 0.90 # Excellent (>-10dB)
        if abs_quality < 15: return 0.60 # Good (>-15dB)
        if abs_quality < 20: return 0.25 # Poor (>-20dB)
        return 0.10 # Bad Signal

def _get_strength_derating_factor(strength: float) -> float:
    if strength > -95: return 0.95
    if strength > -105: return 0.80
    if strength > -115: return 0.60
    return 0.30

def _get_volatility_metrics(history: List[Dict[str, float]]) -> Tuple[float, float]:
    if len(history) < 3: return 0.0, 1.0
    
    strengths = [h['strength'] for h in history if 'strength' in h]
    if not strengths: return 0.0, 1.0

    std_dev = statistics.stdev(strengths)
    mean = statistics.mean(strengths)
    if mean == 0: return 0.0, 1.0

    cov = (std_dev / abs(mean))
    volatility_index = min(100.0, cov / 0.05 * 100.0)
    derating_factor = 1.0 - (0.25 * (volatility_index / 100.0))
    
    return round(volatility_index, 1), derating_factor

def _initialize_profile() -> Dict[str, Any]:
    return {"last_cell_id": None, "metric_history": []}

def analyze_cellular_state(
    current_payload: Dict[str, Any],
    previous_profile: Optional[Dict[str, Any]]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    
    profile = previous_profile or _initialize_profile()
    
    network = current_payload.get("network", {})
    cellular = network.get("cellular", {})
    bandwidth = network.get("bandwidth", {})
    
    strength = cellular.get("signal", {}).get("strength", {}).get("value_dbm")
    quality = cellular.get("signal", {}).get("quality", {}).get("value")
    cell_type = cellular.get("type", "Other")
    active_network_type = network.get("currently_used_active_network")
    cell_id = cellular.get("cell_id")
    link_speed = bandwidth.get("upload_in_mbps")

    analysis = {
        "predicted_upload_throughput_mbps": None,
        "connection_state": None,
        "prediction_source": None,
        "effective_link_speed_mbps": None,
        "quality_derating_factor": None,
        "strength_derating_factor": None,
        "volatility_index": None,
    }

    if active_network_type != "LTE":
        return analysis, profile

    analysis["prediction_source"] = "Heuristic (Link Speed Based)"
    analysis["connection_state"] = ConnectionState.UNKNOWN
    
    history_deque = deque(profile.get("metric_history", []), maxlen=PROFILE_HISTORY_LENGTH)
    if strength is not None and quality is not None:
        history_deque.append({"strength": strength, "quality": quality})
        profile["metric_history"] = list(history_deque)

    volatility_index, volatility_factor = _get_volatility_metrics(profile["metric_history"][-VOLATILITY_SAMPLES:])
    analysis["volatility_index"] = volatility_index

    if link_speed is not None and strength is not None and quality is not None:
        q_factor = _get_quality_derating_factor(quality, cell_type)
        s_factor = _get_strength_derating_factor(strength)
        
        analysis["quality_derating_factor"] = round(q_factor, 2)
        analysis["strength_derating_factor"] = round(s_factor, 2)

        effective_link_speed = link_speed
        if strength < CELL_EDGE_STRENGTH_DBM:
            effective_link_speed = min(link_speed, CELL_EDGE_EFFECTIVE_LINK_SPEED_CAP_MBPS)
        analysis["effective_link_speed_mbps"] = round(effective_link_speed, 1)

        predicted_throughput = effective_link_speed * q_factor * s_factor * volatility_factor
        analysis["predicted_upload_throughput_mbps"] = round(max(0.0, predicted_throughput), 1)

    predicted_speed = analysis["predicted_upload_throughput_mbps"]
    if cell_id and profile.get("last_cell_id") and cell_id != profile["last_cell_id"]:
        analysis["connection_state"] = ConnectionState.HANDOFF
    elif predicted_speed is None:
        analysis["connection_state"] = ConnectionState.UNKNOWN
    elif predicted_speed < CRITICAL_THROUGHPUT_MBPS:
        analysis["connection_state"] = ConnectionState.CRITICAL
    elif link_speed is not None and predicted_speed < (link_speed * 0.3):
        analysis["connection_state"] = ConnectionState.LIMITED
    elif volatility_index > 50:
        analysis["connection_state"] = ConnectionState.UNSTABLE
    else:
        analysis["connection_state"] = ConnectionState.STABLE

    if cell_id:
        profile["last_cell_id"] = cell_id

    return analysis, profile
