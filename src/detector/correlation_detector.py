"""Multi-sensor cross-correlation anomaly detector.
Detects complex multivariate failure modes (bearing seizure, locked rotor, thermal runaway)
by fusing normalized vibration, electrical current, and thermal deviations.
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


class MultiSensorCorrelationDetector:
    """Fuses multi-sensor signals into an integrated Machinery Health & Fault Index."""

    def __init__(
        self,
        window_size: int = 25,
        fault_threshold: float = 3.0,
        weight_vibration: float = 0.45,
        weight_current: float = 0.35,
        weight_temperature: float = 0.20,
    ):
        self.window_size = window_size
        self.fault_threshold = fault_threshold
        self.wv = weight_vibration
        self.wi = weight_current
        self.wt = weight_temperature
        # Device -> History deque of (vib, curr, temp)
        self._histories: Dict[str, collections.deque] = {}

    def process(self, record: TelemetryRecord) -> List[AnomalyRecord]:
        dev_id = record.device_id
        if dev_id not in self._histories:
            self._histories[dev_id] = collections.deque(maxlen=self.window_size)

        history = self._histories[dev_id]
        v = float(record.vibration_rms)
        c = float(record.current)
        t = float(record.temperature)

        anomalies: List[AnomalyRecord] = []

        if len(history) >= 8:
            mat = np.array(history, dtype=float)
            v_mean, v_std = float(np.mean(mat[:, 0])), float(max(np.std(mat[:, 0]), 0.1))
            c_mean, c_std = float(np.mean(mat[:, 1])), float(max(np.std(mat[:, 1]), 0.2))
            t_mean, t_std = float(np.mean(mat[:, 2])), float(max(np.std(mat[:, 2]), 0.3))

            z_v = max(0.0, (v - v_mean) / v_std)
            z_c = max(0.0, (c - c_mean) / c_std)
            z_t = max(0.0, (t - t_mean) / t_std)

            # Combined Fault Index
            fault_index = (self.wv * z_v) + (self.wi * z_c) + (self.wt * z_t)

            if fault_index >= self.fault_threshold:
                # Determine specific correlation signature
                if z_v > 2.5 and z_c > 2.0 and z_t > 1.5:
                    anom_type = AnomalyType.CORRELATED_SEIZURE
                    sev = SeverityLevel.CRITICAL
                    desc = f"Critical mechanical seizure: simultaneous elevation in vibration ({v:.1f}g, z={z_v:.1f}), current ({c:.1f}A, z={z_c:.1f}), and temp ({t:.1f}°C, z={z_t:.1f})"
                elif z_v > 3.0:
                    anom_type = AnomalyType.BEARING_FATIGUE
                    sev = SeverityLevel.WARNING if fault_index < 4.5 else SeverityLevel.CRITICAL
                    desc = f"Impending bearing degradation: mechanical vibration {v:.2f}g with elevated cross-sensor stress"
                elif z_c > 3.0:
                    anom_type = AnomalyType.PHASE_IMBALANCE
                    sev = SeverityLevel.CRITICAL
                    desc = f"Electrical phase/stator fault: high current draw {c:.2f}A (z={z_c:.1f})"
                else:
                    anom_type = AnomalyType.CORRELATED_SEIZURE
                    sev = SeverityLevel.WARNING
                    desc = f"Multivariate equipment stress index {fault_index:.2f} exceeded threshold {self.fault_threshold:.2f}"

                anomalies.append(
                    AnomalyRecord(
                        device_id=dev_id,
                        timestamp_ms=record.timestamp_ms,
                        anomaly_type=anom_type,
                        detector_name="CORRELATION",
                        severity=sev,
                        metric_name="composite_fault_index",
                        actual_value=round(fault_index, 2),
                        expected_value=1.0,
                        score=round(fault_index, 2),
                        threshold=self.fault_threshold,
                        description=desc,
                    )
                )

        history.append((v, c, t))
        return anomalies

    def reset_device(self, device_id: str) -> None:
        if device_id in self._histories:
            del self._histories[device_id]
