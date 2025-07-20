from fastapi import APIRouter, Request, status, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from app.tasks import process_and_store_data

router = APIRouter(prefix="/api/internal", tags=["Internal"])

class NotificationPayload(BaseModel):
    records: List[Dict[str, Any]]

@router.post("/notify", status_code=status.HTTP_202_ACCEPTED)
async def receive_notification(payload: NotificationPayload):
    if not payload.records:
        return {"status": "accepted", "message": "No records to process."}
    
    try:
        process_and_store_data.delay(payload.records)
        return {"status": "accepted", "records_queued": len(payload.records)}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to queue task for processing: {e}"
        )
