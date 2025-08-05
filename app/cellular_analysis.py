import math
import copy
from collections import deque
from typing import Dict, Any, Tuple, Optional, List

# --- Configuration Constants ---
HISTORY_LENGTH = 10
EMA_ALPHA = 0.3
SCORE_SMOOTHING_FACTOR = 0.6

class ConnectionState:
    STABLE = "Stable"
    DEGRADING = "Degrading"
    CRITICAL = "Critical"
    UNSTABLE = "Unstable"
    HANDOFF = "Handoff"
    UNKNOWN = "Unknown"

# --- Heuristic Model Parameters (Recalibrated based on all test data) ---
QUALITY_THRESHOLDS = {'good': -10, 'fair': -16}
QUALITY_SCORE_MOD = {'good': 5, 'fair': -15, 'poor': -30}
STRENGTH_THRESHOLDS = {'good': -95, 'fair': -105, 'poor': -115}
STRENGTH_SCORE_MOD = {'good': 5, 'fair': -20, 'poor': -35, 'critical': -50}
STATE_THRESHOLDS = {'critical': 60, 'degrading': 85}

BASE_BANDWIDTH_MBPS = {"NR(5G)": 40, "LTE": 25, "UMTS/HSPA": 4, "Other": 2}
QUALITY_MULTIPLIER = {'good': 1.0, 'fair': 0.6, 'poor': 0.25, 'unknown': 0.4}
STRENGTH_MULTIPLIER = {'good': 1.0, 'fair': 0.7, 'poor': 0.3, 'critical': 0.1, 'unknown': 0.5}

def _update_history(history: dict, metric: str, value: Optional[float]):
    if value is not None:
        if metric not in history: history[metric] = deque(maxlen=HISTORY_LENGTH)
        history[metric].append(value)

def analyze_cellular_state(
    current_payload: Dict[str, Any], 
    previous_state: Optional[Dict[str, Any]]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if not previous_state:
        previous_state = {'score': 100, 'state': ConnectionState.STABLE}

    new_state = copy.deepcopy(previous_state)
    
    cellular = current_payload.get('network', {}).get('cellular', {})
    strength = cellular.get('signal', {}).get('strength', {}).get('value_dbm')
    quality = cellular.get('signal', {}).get('quality', {}).get('value')
    cell_type = cellular.get('type', 'Other')

    analysis, score = {}, 100
    
    if quality is not None:
        if quality > QUALITY_THRESHOLDS['good']: score += QUALITY_SCORE_MOD['good']
        elif quality > QUALITY_THRESHOLDS['fair']: score += QUALITY_SCORE_MOD['fair']
        else: score += QUALITY_SCORE_MOD['poor']
    
    if strength is not None:
        if strength > STRENGTH_THRESHOLDS['good']: score += STRENGTH_SCORE_MOD['good']
        elif strength > STRENGTH_THRESHOLDS['fair']: score += STRENGTH_SCORE_MOD['fair']
        elif strength > STRENGTH_THRESHOLDS['poor']: score += STRENGTH_SCORE_MOD['poor']
        else: score += STRENGTH_SCORE_MOD['critical']

    previous_score = new_state.get('score', 100)
    new_state['score'] = (previous_score * (1 - SCORE_SMOOTHING_FACTOR)) + (score * SCORE_SMOOTHING_FACTOR)
    
    if new_state['score'] < STATE_THRESHOLDS['critical']: new_state['state'] = ConnectionState.CRITICAL
    elif new_state['score'] < STATE_THRESHOLDS['degrading']: new_state['state'] = ConnectionState.DEGRADING
    else: new_state['state'] = ConnectionState.STABLE
    
    analysis.update({'health_score': round(new_state['score']), 'connection_state': new_state['state']})

    q_mult = QUALITY_MULTIPLIER['unknown']
    if quality is not None:
        if quality > QUALITY_THRESHOLDS['good']: q_mult = QUALITY_MULTIPLIER['good']
        elif quality > QUALITY_THRESHOLDS['fair']: q_mult = QUALITY_MULTIPLIER['fair']
        else: q_mult = QUALITY_MULTIPLIER['poor']
    
    s_mult = STRENGTH_MULTIPLIER['unknown']
    if strength is not None:
        if strength > STRENGTH_THRESHOLDS['good']: s_mult = STRENGTH_MULTIPLIER['good']
        elif strength > STRENGTH_THRESHOLDS['fair']: s_mult = STRENGTH_MULTIPLIER['fair']
        elif strength > STRENGTH_THRESHOLDS['poor']: s_mult = STRENGTH_MULTIPLIER['poor']
        else: s_mult = STRENGTH_MULTIPLIER['critical']
        
    predicted_bw = BASE_BANDWIDTH_MBPS.get(cell_type, BASE_BANDWIDTH_MBPS['Other']) * q_mult * s_mult
    analysis['predicted_upload_mbps'] = round(predicted_bw, 1)
    
    return analysis, new_state