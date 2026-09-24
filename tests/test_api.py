"""Unit tests for FastAPI REST API endpoints using TestClient."""
from __future__ import annotations

import sys
import time
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from api.main import app  # noqa: E402
from src.models.schemas import SensorType, TelemetryRecord  # noqa: E402

client = TestClient(app, raise_server_exceptions=False)


def _make_payload(dev: str = "motor_unit_01", temp: float = 55.0) -> dict:
    return {
        "device_id": dev,
        "timestamp_ms": int(time.time() * 1000),
        "sensor_type": SensorType.MOTOR.value,
        "temperature": temp,
        "vibration_rms": 0.85,
        "vibration_kurtosis": 3.0,
        "current": 24.0,
        "voltage": 400.0,
        "power_factor": 0.88,
        "frequency": 50.0,
    }


class TestHealthAndMetadata:
    def test_health_check(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "HEALTHY"
        assert "version" in data


class TestTelemetryEndpoints:
    def test_ingest_single_queued(self):
        payload = _make_payload("motor_api_01")
        resp = client.post("/api/v1/telemetry", json=payload, params={"sync_detect": False})
        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "QUEUED"
        assert data["device_id"] == "motor_api_01"
        assert "stream_msg_id" in data

    def test_ingest_single_sync_detect(self):
        payload = _make_payload("motor_api_sync", temp=55.0)
        resp = client.post("/api/v1/telemetry", json=payload, params={"sync_detect": True})
        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "PROCESSED"
        assert "anomalies_found" in data

    def test_ingest_batch(self):
        records = [_make_payload(f"batch_dev_{i}") for i in range(5)]
        resp = client.post("/api/v1/telemetry/batch", json={"records": records})
        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "BATCH_QUEUED"
        assert data["total_records"] == 5

    def test_get_live_telemetry(self):
        dev = "live_test_dev"
        client.post("/api/v1/telemetry", json=_make_payload(dev))
        resp = client.get(f"/api/v1/telemetry/live/{dev}")
        assert resp.status_code == 200
        readings = resp.json()
        assert len(readings) >= 1
        assert readings[0]["device_id"] == dev


class TestDevicesAndAlertsEndpoints:
    def test_list_devices(self):
        resp = client.get("/api/v1/devices")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_get_anomalies_and_alerts(self):
        resp_anom = client.get("/api/v1/anomalies")
        assert resp_anom.status_code == 200

        resp_alt = client.get("/api/v1/alerts")
        assert resp_alt.status_code == 200

    def test_get_metrics(self):
        resp = client.get("/api/v1/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert "events_ingested" in data
        assert "throughput_eps" in data
        assert "buffer" in data

    def test_get_dlq(self):
        resp = client.get("/api/v1/dlq")
        assert resp.status_code == 200
        data = resp.json()
        assert "dlq_count" in data
