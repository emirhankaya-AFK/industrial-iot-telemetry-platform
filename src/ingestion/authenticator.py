"""Device authentication and clock-drift verification gateway."""
from __future__ import annotations

import hashlib
import hmac
import time
from typing import Dict

from src.models.schemas import DeviceStatus, SensorType, TelemetryRecord


class DeviceAuthenticator:
    """Manages device credentials, HMAC signatures, and clock drift."""

    def __init__(
        self,
        default_secret: str = "industrial_iot_super_secret_key_2026",
        max_clock_drift_seconds: int = 300,
        enforce_auth: bool = False,
    ):
        self.default_secret = default_secret
        self.max_clock_drift_seconds = max_clock_drift_seconds
        self.enforce_auth = enforce_auth
        self._device_keys: Dict[str, str] = {}
        self._device_registry: Dict[str, DeviceStatus] = {}

    def register_device(self, device_id: str, secret_key: str | None = None, sensor_type: SensorType = SensorType.MOTOR) -> None:
        """Registers a known device with its HMAC secret key."""
        now_ms = int(time.time() * 1000)
        self._device_keys[device_id] = secret_key or self.default_secret
        if device_id not in self._device_registry:
            self._device_registry[device_id] = DeviceStatus(
                device_id=device_id,
                sensor_type=sensor_type,
                first_seen_ms=now_ms,
                last_seen_ms=now_ms,
                total_events=0,
                is_online=True,
            )

    def generate_token(self, device_id: str, timestamp_ms: int) -> str:
        """Generates valid HMAC-SHA256 token for a device and timestamp."""
        secret = self._device_keys.get(device_id, self.default_secret)
        msg = f"{device_id}:{timestamp_ms}".encode("utf-8")
        return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()[:16]

    def authenticate(self, record: TelemetryRecord) -> bool:
        """Validates device token and clock drift. Raises ValueError on failure."""
        now_ms = int(time.time() * 1000)
        drift_sec = abs(now_ms - record.timestamp_ms) / 1000.0

        if drift_sec > self.max_clock_drift_seconds:
            raise ValueError(
                f"Clock drift error: device '{record.device_id}' timestamp drifted by {drift_sec:.1f}s "
                f"(max allowed: {self.max_clock_drift_seconds}s)"
            )

        if self.enforce_auth:
            if not record.auth_token:
                raise ValueError(f"Authentication failed: missing auth_token for device '{record.device_id}'")
            expected_token = self.generate_token(record.device_id, record.timestamp_ms)
            if not hmac.compare_digest(record.auth_token, expected_token):
                raise ValueError(f"Authentication failed: invalid token signature for device '{record.device_id}'")

        # Update registry status
        if record.device_id not in self._device_registry:
            self.register_device(record.device_id, sensor_type=record.sensor_type)

        dev = self._device_registry[record.device_id]
        dev.last_seen_ms = record.timestamp_ms
        dev.total_events += 1
        dev.is_online = True
        dev.last_telemetry = record
        return True

    def get_device(self, device_id: str) -> DeviceStatus | None:
        """Returns registered status of a device."""
        return self._device_registry.get(device_id)

    def list_devices(self, offline_timeout_seconds: int = 60) -> list[DeviceStatus]:
        """Lists all registered devices and marks stale ones as offline."""
        now_ms = int(time.time() * 1000)
        result = []
        for dev in self._device_registry.values():
            if (now_ms - dev.last_seen_ms) > (offline_timeout_seconds * 1000):
                dev.is_online = False
            result.append(dev)
        return result
