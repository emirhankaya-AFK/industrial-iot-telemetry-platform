"""FastAPI router for anomalies log, incident alerts, and operator ACK."""
from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query

from src.models.schemas import AlertEvent, AnomalyRecord
from src.pipeline.stream_processor import StreamProcessor

router = APIRouter()


def get_processor() -> StreamProcessor:
    from api.main import stream_processor
    return stream_processor


@router.get("/anomalies", response_model=List[AnomalyRecord])
def get_anomalies_log(
    limit: int = Query(50, ge=1, le=500),
    processor: StreamProcessor = Depends(get_processor),
):
    """Returns recent anomaly detections across all monitored machines."""
    return processor.get_recent_anomalies(limit=limit)


@router.get("/alerts", response_model=List[AlertEvent])
def get_incident_alerts(
    limit: int = Query(50, ge=1, le=200),
    processor: StreamProcessor = Depends(get_processor),
):
    """Returns deduplicated, rate-limited incident alert events."""
    return processor.dispatcher.get_recent_alerts(limit=limit)


@router.post("/alerts/{alert_id}/ack")
def acknowledge_alert(
    alert_id: str,
    operator: str = Query("Operator_01", description="Name of acknowledging technician"),
    processor: StreamProcessor = Depends(get_processor),
):
    """Marks an alert incident as acknowledged by an operator."""
    success = processor.dispatcher.acknowledge_alert(alert_id, operator_name=operator)
    if not success:
        raise HTTPException(status_code=404, detail=f"Alert incident '{alert_id}' not found")
    return {"status": "ACKNOWLEDGED", "alert_id": alert_id, "acknowledged_by": operator}


@router.get("/dlq")
def get_dead_letter_queue(
    processor: StreamProcessor = Depends(get_processor),
):
    """Returns entries routed to Dead-Letter Queue due to failed webhook dispatches."""
    records = processor.dispatcher.get_dlq_records()
    return {
        "dlq_count": len(records),
        "items": [
            {
                "alert_id": r.alert.alert_id,
                "error": r.error,
                "attempts": r.attempts,
                "timestamp_ms": r.timestamp_ms,
            }
            for r in records
        ],
    }
