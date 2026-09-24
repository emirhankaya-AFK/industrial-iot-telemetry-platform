"""Unit tests for Pydantic schemas, Protobuf bridge, and Device Authenticator."""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.ingestion.authenticator import DeviceAuthenticator
from src.models.protobuf_bridge import (
    decode_json_telemetry,
    decode_protobuf_telemetry,
    encode_json_telemetry,
    encode_protobuf_telemetry,
)
from src.models.schemas import BatchTelemetryPayload, SensorType, TelemetryRecord


def _make_sample_record(offset_sec: int = 0) -> TelemetryRecord:
    now_ms = int(time.time() * 1000) + offset_sec * 1000
    return TelemetryRecord(
        device_id="motor_unit_01",
        timestamp_ms=now_ms,
        sensor_type=SensorType.MOTOR,
        temperature=55.4,
        vibration_rms=0.88,
        vibration_kurtosis=3.12,
        current=24.5,
        voltage=400.0,
        power_factor=0.87,
        frequency=50.0,
        auth_token="auth_sample_tok",
        metadata={"site": "plant_01"},
    )


class TestSchemasAndValidation:
    def test_valid_telemetry_record_creation(self):
        rec = _make_sample_record()
        assert rec.device_id == "motor_unit_01"
        assert rec.temperature == 55.4
        assert rec.sensor_type == SensorType.MOTOR

    def test_clock_drift_rejection(self):
        now_ms = int(time.time() * 1000)
        # More than 24 hours in past
        stale_time = now_ms - (26 * 3600 * 1000)
        with pytest.raises(ValueError, match="clock drift exceeds"):
            TelemetryRecord(
                device_id="motor_unit_01",
                timestamp_ms=stale_time,
                temperature=50.0,
                vibration_rms=1.0,
                current=20.0,
            )

    def test_batch_payload_validation(self):
        rec1 = _make_sample_record()
        rec2 = _make_sample_record()
        batch = BatchTelemetryPayload(records=[rec1, rec2])
        assert len(batch.records) == 2
        assert batch.batch_id.startswith("batch_")


class TestProtobufAndJsonBridge:
    def test_protobuf_wire_format_roundtrip(self):
        rec = _make_sample_record()
        proto_bytes = encode_protobuf_telemetry(rec)
        assert isinstance(proto_bytes, bytes)
        assert len(proto_bytes) > 20

        decoded = decode_protobuf_telemetry(proto_bytes)
        assert decoded.device_id == rec.device_id
        assert decoded.timestamp_ms == rec.timestamp_ms
        assert decoded.sensor_type == rec.sensor_type
        assert abs(decoded.temperature - rec.temperature) < 1e-3
        assert abs(decoded.vibration_rms - rec.vibration_rms) < 1e-3
        assert abs(decoded.current - rec.current) < 1e-3
        assert decoded.auth_token == rec.auth_token

    def test_json_bridge_roundtrip(self):
        rec = _make_sample_record()
        json_str = encode_json_telemetry(rec)
        decoded = decode_json_telemetry(json_str)
        assert decoded.device_id == rec.device_id
        assert decoded.temperature == rec.temperature


class TestDeviceAuthenticator:
    def test_device_registration_and_auth(self):
        auth = DeviceAuthenticator(enforce_auth=True)
        dev_id = "cnc_spindle_02"
        now_ms = int(time.time() * 1000)
        token = auth.generate_token(dev_id, now_ms)

        rec = TelemetryRecord(
            device_id=dev_id,
            timestamp_ms=now_ms,
            temperature=40.0,
            vibration_rms=1.0,
            current=15.0,
            auth_token=token,
        )
        assert auth.authenticate(rec) is True
        dev_status = auth.get_device(dev_id)
        assert dev_status is not None
        assert dev_status.is_online is True
        assert dev_status.total_events == 1

    def test_invalid_token_rejected(self):
        auth = DeviceAuthenticator(enforce_auth=True)
        dev_id = "cnc_spindle_02"
        now_ms = int(time.time() * 1000)

        rec = TelemetryRecord(
            device_id=dev_id,
            timestamp_ms=now_ms,
            temperature=40.0,
            vibration_rms=1.0,
            current=15.0,
            auth_token="tampered_fake_token",
        )
        with pytest.raises(ValueError, match="Authentication failed"):
            auth.authenticate(rec)
