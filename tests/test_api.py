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


class TestApiAuthenticationEnforcement:
    def test_api_auth_enforcement_rejections(self):
        from api.main import stream_processor
        original_enforce = stream_processor.auth.enforce_auth
        original_secret = stream_processor.auth.default_secret

        try:
            stream_processor.auth.enforce_auth = True
            stream_processor.auth.default_secret = "api_secret_789"
            dev_id = "secure_cnc_01"
            stream_processor.auth.register_device(dev_id, secret_key="api_secret_789")

            # 1. Unregistered device rejected
            unreg_payload = _make_payload("unregistered_attacker_01")
            unreg_payload["auth_token"] = "fake"
            resp1 = client.post("/api/v1/telemetry", json=unreg_payload)
            assert resp1.status_code == 400
            assert "unregistered device" in resp1.json()["detail"]

            # 2. Missing token rejected
            no_token_payload = _make_payload(dev_id)
            resp2 = client.post("/api/v1/telemetry", json=no_token_payload)
            assert resp2.status_code == 400
            assert "missing auth_token" in resp2.json()["detail"]

            # 3. Invalid signature rejected
            tampered_payload = _make_payload(dev_id)
            tampered_payload["auth_token"] = "bad_sig_123"
            resp3 = client.post("/api/v1/telemetry", json=tampered_payload)
            assert resp3.status_code == 400
            assert "invalid token signature" in resp3.json()["detail"]

            # 4. Valid signature accepted
            now_ms = int(time.time() * 1000)
            valid_payload = _make_payload(dev_id)
            valid_payload["timestamp_ms"] = now_ms
            valid_token = stream_processor.auth.generate_token(dev_id, now_ms)
            valid_payload["auth_token"] = valid_token
            resp4 = client.post("/api/v1/telemetry", json=valid_payload)
            assert resp4.status_code == 202
            assert resp4.json()["status"] == "QUEUED"

        finally:
            stream_processor.auth.enforce_auth = original_enforce
            stream_processor.auth.default_secret = original_secret
