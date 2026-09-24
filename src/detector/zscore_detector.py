"""Rolling Robust Z-Score Anomaly Detector using Median Absolute Deviation (MAD).
Resilient against extreme outlier contamination in windowed telemetry history.
"""
from __future__ import annotations

import collections
from typing import Dict, List

import numpy as np

from src.models.schemas import (
    AnomalyRecord,
    AnomalyType,
    SeverityLevel,
    TelemetryRecord,
)


class RobustZScoreDetector:
    """Detects acute transient spikes using rolling window Median Absolute Deviation (MAD)."""

    def __init__(
        self,
        window_size: int = 30,
        threshold: float = 3.5,
        metrics: list[str] | None = None,
    ):
        self.window_size = window_size
        self.threshold = threshold
        self.target_metrics = metrics or ["temperature", "vibration_rms", "vibration_kurtosis", "current"]
        # Device -> Metric -> collections.deque
        self._windows: Dict[str, Dict[str, collections.deque]] = {}

    def process(self, record: TelemetryRecord) -> List[AnomalyRecord]:
        """Evaluates telemetry record against rolling robust statistics."""
        dev_id = record.device_id
        if dev_id not in self._windows:
            self._windows[dev_id] = {m: collections.deque(maxlen=self.window_size) for m in self.target_metrics}

        anomalies: List[AnomalyRecord] = []
        dev_windows = self._windows[dev_id]

        for metric in self.target_metrics:
            val = getattr(record, metric, None)
            if val is None:
                continue

            window = dev_windows[metric]

            # Require at least 8 observations to compute meaningful median & MAD
            if len(window) >= 8:
                arr = np.array(window, dtype=float)
                med = float(np.median(arr))
                mad = float(np.median(np.abs(arr - med)))

                # If MAD is near zero (constant flat signal), fallback to empirical std
                if mad < 1e-4:
                    std = float(np.std(arr))
                    denom = max(std, 0.05)
                    mod_z = abs(val - med) / denom
                else:
                    mod_z = 0.6745 * abs(val - med) / mad

                if mod_z >= self.threshold:
                    severity = SeverityLevel.CRITICAL if mod_z >= 5.0 else SeverityLevel.WARNING
                    anom_type = (
                        AnomalyType.VIBRATION_SPIKE if "vibration" in metric
                        else AnomalyType.THERMAL_DRIFT if metric == "temperature"
                        else AnomalyType.CURRENT_SURGE
                    )
                    anomalies.append(
                        AnomalyRecord(
                            device_id=dev_id,
                            timestamp_ms=record.timestamp_ms,
                            anomaly_type=anom_type,
                            detector_name="ROBUST_ZSCORE",
                            severity=severity,
                            metric_name=metric,
                            actual_value=round(val, 2),
                            expected_value=round(med, 2),
                            score=round(mod_z, 2),
                            threshold=self.threshold,
                            description=f"Robust Z-Score shock on {metric}: value {val:.2f} (med: {med:.2f}, score: {mod_z:.1f})",
                        )
                    )

            # Store current value into rolling window
            window.append(float(val))

        return anomalies

    def reset_device(self, device_id: str) -> None:
        """Clears rolling windows for a device."""
        if device_id in self._windows:
            del self._windows[device_id]
