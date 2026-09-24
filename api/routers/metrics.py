"""FastAPI router for pipeline throughput, backpressure, and latency metrics."""
from __future__ import annotations

from fastapi import APIRouter, Depends

from src.models.schemas import PipelineMetrics
from src.pipeline.stream_processor import StreamProcessor

router = APIRouter()


def get_processor() -> StreamProcessor:
    from api.main import stream_processor
    return stream_processor


@router.get("/metrics", response_model=PipelineMetrics)
def get_pipeline_metrics(
    processor: StreamProcessor = Depends(get_processor),
):
    """Returns real-time pipeline performance: throughput (eps), latency p95/p99, backlog."""
    return processor.get_metrics()
