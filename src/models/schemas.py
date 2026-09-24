"""Pydantic v2 schemas for Industrial Edge Telemetry & Anomaly Detection Platform."""
from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class SensorType(str, Enum):
    MOTOR = "MOTOR"
    CNC_SPINDLE = "CNC_SPINDLE"
    PUMP = "PUMP"
    TRANSFORMER = "TRANSFORMER"


class SeverityLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AnomalyType(str, Enum):
    VIBRATION_SPIKE = "VIBRATION_SPIKE"
    THERMAL_DRIFT = "THERMAL_DRIFT"
    CURRENT_SURGE = "CURRENT_SURGE"
    BEARING_FATIGUE = "BEARING_FATIGUE"
    PHASE_IMBALANCE = "PHASE_IMBALANCE"
    CORRELATED_SEIZURE = "CORRELATED_SEIZURE"
    OUT_OF_BOUNDS = "OUT_OF_BOUNDS"


class TelemetryRecord(BaseModel):
    """Single telemetry reading from an industrial machine."""
    device_id: str = Field(min_length=3, max_length=64, description="Unique equipment ID, e.g. motor_unit_01")
    timestamp_ms: int = Field(description="Epoch millisecond timestamp of measurement")
    sensor_type: SensorType = Field(default=SensorType.MOTOR)
    temperature: float = Field(ge=-40.0, le=250.0, description="Temperature in Celsius")
    vibration_rms: float = Field(ge=0.0, le=50.0, description="RMS vibration acceleration in g")
    vibration_kurtosis: float = Field(ge=0.0, le=50.0, default=3.0, description="Vibration signal kurtosis")
    current: float = Field(ge=0.0, le=500.0, description="Stator / line current in Amperes")
    voltage: float = Field(ge=0.0, le=1000.0, default=400.0, description="Operating line-to-line voltage (V)")
    power_factor: float = Field(ge=0.0, le=1.0, default=0.88, description="Electrical power factor cos(phi)")
    frequency: float = Field(ge=0.0, le=120.0, default=50.0, description="Mains electrical frequency in Hz")
    auth_token: Optional[str] = Field(default=None, description="Optional HMAC device signature or bearer token")
    metadata: Dict[str, str] = Field(default_factory=dict, description="Custom edge metadata key-value tags")

    @field_validator("timestamp_ms")
    @classmethod
    def validate_timestamp_window(cls, v: int) -> int:
        now_ms = int(time.time() * 1000)
        # Allow +/- 24 hours to accommodate edge clock drift while preventing epoch 0 bugs
        max_drift = 24 * 3600 * 1000
        if v < (now_ms - max_drift) or v > (now_ms + max_drift):
            raise ValueError(f"Timestamp {v} rejected: clock drift exceeds +/- 24h window (now: {now_ms})")
        return v


class BatchTelemetryPayload(BaseModel):
    """Batch payload containing multiple sensor events."""
    batch_id: str = Field(default_factory=lambda: f"batch_{uuid.uuid4().hex[:8]}")
    sent_at_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    records: List[TelemetryRecord] = Field(min_length=1, max_length=1000)


class DeviceStatus(BaseModel):
    """Status record of a registered industrial edge equipment."""
    device_id: str
    sensor_type: SensorType
    first_seen_ms: int
    last_seen_ms: int
    total_events: int = 0
    is_online: bool = True
    last_telemetry: Optional[TelemetryRecord] = None


class AnomalyRecord(BaseModel):
    """Detailed record of a single detected telemetry anomaly."""
    anomaly_id: str = Field(default_factory=lambda: f"anom_{uuid.uuid4().hex[:8]}")
    device_id: str
    timestamp_ms: int
    anomaly_type: AnomalyType
    detector_name: str = Field(description="EWMA, ZSCORE, CORRELATION, or ISOLATION_FOREST")
    severity: SeverityLevel
    metric_name: str
    actual_value: float
    expected_value: float
    score: float
    threshold: float
    description: str


class AlertEvent(BaseModel):
    """Incident alert produced by deduplicated and escalated anomalies."""
    alert_id: str = Field(default_factory=lambda: f"alt_{uuid.uuid4().hex[:8]}")
    device_id: str
    created_at_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    severity: SeverityLevel
    title: str
    details: str
    anomalies: List[AnomalyRecord] = Field(default_factory=list)
    acknowledged: bool = False
    acknowledged_at_ms: Optional[int] = None
    acknowledged_by: Optional[str] = None


class BufferMetrics(BaseModel):
    """Internal streaming buffer and queue metrics."""
    stream_name: str
    backlog_count: int
    dropped_count: int
    max_len: int
    consumer_groups: List[str] = Field(default_factory=list)


class PipelineMetrics(BaseModel):
    """High-level operational metrics of the edge telemetry system."""
    events_ingested: int = 0
    events_processed: int = 0
    events_dropped: int = 0
    anomalies_detected: int = 0
    alerts_dispatched: int = 0
    dlq_count: int = 0
    throughput_eps: float = 0.0
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    buffer: Optional[BufferMetrics] = None
