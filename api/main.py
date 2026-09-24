"""FastAPI Application entry point for Industrial IoT Edge Telemetry Gateway."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import anomalies, devices, metrics, telemetry
from src.ingestion.mqtt_subscriber import MQTTTelemetrySubscriber
from src.pipeline.stream_processor import StreamProcessor

# Shared singleton processor
stream_processor = StreamProcessor()

# Background consumer task handle
_bg_consumer_task = None
_mqtt_subscriber: MQTTTelemetrySubscriber | None = None


async def _background_stream_worker():
    """Asynchronous worker pulling from Redis stream buffer and running detectors."""
    while True:
        try:
            stream_processor.process_pending_stream(count=50)
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _bg_consumer_task, _mqtt_subscriber

    # Start background stream consumer
    _bg_consumer_task = asyncio.create_task(_background_stream_worker())

    # Try starting MQTT subscriber (runs gracefully in background if Mosquitto available)
    _mqtt_subscriber = MQTTTelemetrySubscriber(
        broker_host="localhost",
        broker_port=1883,
        on_record_received=stream_processor.ingest_record,
    )
    _mqtt_subscriber.start()

    yield

    # Teardown
    if _mqtt_subscriber:
        _mqtt_subscriber.stop()
    if _bg_consumer_task:
        _bg_consumer_task.cancel()
        try:
            await _bg_consumer_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Industrial Edge Telemetry & Streaming Anomaly Detection Platform",
    version="1.0.0",
    description="Mission-critical Edge IoT Gateway with Redis Streams, EWMA, and Robust Z-Score detectors.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(telemetry.router, prefix="/api/v1", tags=["Telemetry Ingestion"])
app.include_router(devices.router, prefix="/api/v1", tags=["Device Registry"])
app.include_router(anomalies.router, prefix="/api/v1", tags=["Anomalies & Alerts"])
app.include_router(metrics.router, prefix="/api/v1", tags=["Operational Metrics"])


@app.get("/health", tags=["Health"])
def health_check():
    """Health check probe."""
    return {
        "status": "HEALTHY",
        "service": "industrial-iot-telemetry-platform",
        "version": "1.0.0",
        "redis_connected": stream_processor.stream.is_connected_to_redis,
        "mqtt_connected": _mqtt_subscriber.is_connected if _mqtt_subscriber else False,
    }
