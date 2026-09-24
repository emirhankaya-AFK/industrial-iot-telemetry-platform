"""FastAPI router for device registry and operational status."""
from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query

from src.models.schemas import DeviceStatus
from src.pipeline.stream_processor import StreamProcessor

router = APIRouter()


def get_processor() -> StreamProcessor:
    from api.main import stream_processor
    return stream_processor


@router.get("/devices", response_model=List[DeviceStatus])
def list_devices(
    offline_timeout: int = Query(60, ge=10, le=3600, description="Seconds without telemetry to mark offline"),
    processor: StreamProcessor = Depends(get_processor),
):
    """Lists all registered industrial edge devices with online status."""
    return processor.auth.list_devices(offline_timeout_seconds=offline_timeout)


@router.get("/devices/{device_id}", response_model=DeviceStatus)
def get_device_detail(
    device_id: str,
    processor: StreamProcessor = Depends(get_processor),
):
    """Returns registration metadata and last known reading for a device."""
    dev = processor.auth.get_device(device_id)
    if not dev:
        raise HTTPException(status_code=404, detail=f"Device '{device_id}' not found in registry")
    return dev
