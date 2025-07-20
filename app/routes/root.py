from fastapi import APIRouter, Request
from fastapi.routing import APIRoute
from collections import defaultdict
from .data_access import query_recent_devices, build_base_url

router = APIRouter()

@router.get("/", tags=["Root"])
async def root(request: Request):
    base_url = build_base_url(request)
    
    endpoint_groups = defaultdict(list)
    for route in request.app.routes:
        if isinstance(route, APIRoute) and route.tags and route.include_in_schema:
            group = route.tags[0]
            endpoint_groups[group].append({
                "path": f"{base_url}{route.path}",
                "name": route.name,
                "methods": sorted(list(route.methods)),
            })

    recent_devices = await query_recent_devices(limit=10)
    for device in recent_devices:
        device['links'] = {
            'latest': f"{base_url}/data/latest/{device['device_id']}",
            'history': f"{base_url}/data/history?device_id={device['device_id']}"
        }

    return {
        "server": "Hoarder Processor",
        "status": "online",
        "diagnostics": {
            "webhook_status": "Receiving data from ingest server",
            "broker_status": "Configured and active (see worker logs for status)"
        },
        "recently_processed_devices": recent_devices,
        "api_endpoints": dict(sorted(endpoint_groups.items()))
    }
