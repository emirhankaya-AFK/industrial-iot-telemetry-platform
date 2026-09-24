"""Streaming Exponentially Weighted Moving Average (EWMA) Anomaly Detector.
Maintains recursive, online estimation of mean and variance per device & metric.
"""
from __future__ import annotations

import math
from typing import Dict, List, Optional

from src.models.schemas import (
    AnomalyRecord,
    AnomalyType,
    SeverityLevel,
    TelemetryRecord,
)


class MetricEWMAState:
    """Online running state for a single scalar metric."""
    def __init__(self, alpha: float = 0.15, beta: float = 0.10):
        self.alpha = alpha
        self.beta = beta
        self.mean: Optional[float] = None
        self.variance: float = 1.0
        self.count: int = 0

    def update(self, val: float) -> tuple[float, float, float]:
        """Evaluates deviation against current model, then updates running mean and variance."""
        self.count += 1
        if self.mean is None:
            self.mean = val
            self.variance = 1.0
            return self.mean, 1.0, 0.0

        prior_mean = self.mean
        prior_std = math.sqrt(max(self.variance, 1e-4))
        z = abs(val - prior_mean) / prior_std

        # Update running estimates
        diff = val - prior_mean
        self.mean = self.alpha * val + (1.0 - self.alpha) * self.mean
        self.variance = self.beta * (diff ** 2) + (1.0 - self.beta) * self.variance

        return self.mean, prior_std, z


class EWMADetector:
    """Detects continuous drift and trend anomalies across industrial telemetry streams."""

    def __init__(
        self,
        alpha: float = 0.15,
        threshold_sigma: float = 3.2,
        metrics: list[str] | None = None,
    ):
        self.alpha = alpha
        self.threshold_sigma = threshold_sigma
        self.target_metrics = metrics or ["temperature", "vibration_rms", "current"]
        # Device -> Metric -> State
        self._states: Dict[str, Dict[str, MetricEWMAState]] = {}

    def process(self, record: TelemetryRecord) -> List[AnomalyRecord]:
        """Evaluates telemetry record against online EWMA models. Returns list of detected anomalies."""
        dev_id = record.device_id
        if dev_id not in self._states:
            self._states[dev_id] = {m: MetricEWMAState(alpha=self.alpha) for m in self.target_metrics}

        anomalies: List[AnomalyRecord] = []
        dev_states = self._states[dev_id]

        for metric in self.target_metrics:
            val = getattr(record, metric, None)
            if val is None:
                continue

            state = dev_states[metric]
            mean, std, z_score = state.update(float(val))

            # Warmup: ignore first 5 readings to establish reliable baseline
            if state.count < 5:
                continue

            if z_score >= self.threshold_sigma:
                severity = SeverityLevel.CRITICAL if z_score >= 4.5 else SeverityLevel.WARNING
                anom_type = (
                    AnomalyType.THERMAL_DRIFT if metric == "temperature"
                    else AnomalyType.VIBRATION_SPIKE if metric == "vibration_rms"
                    else AnomalyType.CURRENT_SURGE
                )
                anomalies.append(
                    AnomalyRecord(
                        device_id=dev_id,
                        timestamp_ms=record.timestamp_ms,
                        anomaly_type=anom_type,
                        detector_name="EWMA",
                        severity=severity,
                        metric_name=metric,
                        actual_value=round(val, 2),
                        expected_value=round(mean, 2),
                        score=round(z_score, 2),
                        threshold=self.threshold_sigma,
                        description=f"EWMA drift on {metric}: value {val:.2f} deviates by {z_score:.1f}σ from mean {mean:.2f}",
                    )
                )

        return anomalies

    def reset_device(self, device_id: str) -> None:
        """Clears state for a specific device."""
        if device_id in self._states:
            del self._states[device_id]
