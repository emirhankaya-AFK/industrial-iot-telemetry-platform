"""FastAPI router for telemetry ingestion and live stream polling."""
from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query

from src.models.schemas import BatchTelemetryPayload, TelemetryRecord
from src.pipeline.stream_processor import StreamProcessor

router = APIRouter()


def get_processor() -> StreamProcessor:
    from api.main import stream_processor
    return stream_processor


@router.post("/telemetry", status_code=202)
def ingest_single_reading(
    record: TelemetryRecord,
    sync_detect: bool = Query(False, description="If true, executes synchronous detection immediately"),
    processor: StreamProcessor = Depends(get_processor),
):
    """Ingests a single sensor reading from an edge node."""
    try:
        if sync_detect:
            anomalies = processor.process_record_sync(record)
            return {
                "status": "PROCESSED",
                "device_id": record.device_id,
                "anomalies_found": len(anomalies),
                "anomalies": anomalies,
            }
        else:
            msg_id = processor.ingest_record(record)
            return {
                "status": "QUEUED",
                "stream_msg_id": msg_id,
                "device_id": record.device_id,
            }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {e}")


@router.post("/telemetry/batch", status_code=202)
def ingest_batch_readings(
    payload: BatchTelemetryPayload,
    processor: StreamProcessor = Depends(get_processor),
):
    """Ingests a batch of readings (e.g. from an edge gateway collector)."""
    try:
        msg_ids = processor.ingest_batch(payload.records)
        return {
            "status": "BATCH_QUEUED",
            "batch_id": payload.batch_id,
            "total_records": len(msg_ids),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch ingestion failed: {e}")


@router.get("/telemetry/live/{device_id}", response_model=List[TelemetryRecord])
def get_live_device_telemetry(
    device_id: str,
    processor: StreamProcessor = Depends(get_processor),
):
    """Returns recent time-series telemetry buffer for a device."""
    readings = processor.get_live_device_telemetry(device_id)
    if not readings:
        dev = processor.auth.get_device(device_id)
        if not dev:
            raise HTTPException(status_code=404, detail=f"Device '{device_id}' not found")
    return readings
