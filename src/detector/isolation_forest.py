"""Multivariate Isolation Forest comparative benchmark detector using scikit-learn.
Serves as an offline ML baseline to benchmark real-time streaming detectors against.
"""
from __future__ import annotations

import numpy as np
from sklearn.ensemble import IsolationForest

from src.models.schemas import (
    AnomalyRecord,
    AnomalyType,
    SeverityLevel,
    TelemetryRecord,
)


class IsolationForestBenchmarkDetector:
    """Offline / Batch multivariate Isolation Forest model."""

    def __init__(self, contamination: float = 0.05, random_state: int = 42):
        self.contamination = contamination
        self.random_state = random_state
        self.model = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=100,
        )
        self.is_fitted = False
        self.feature_names = ["temperature", "vibration_rms", "vibration_kurtosis", "current", "power_factor"]

    def _extract_features(self, records: list[TelemetryRecord]) -> np.ndarray:
        data = []
        for r in records:
            data.append([
                r.temperature,
                r.vibration_rms,
                r.vibration_kurtosis,
                r.current,
                r.power_factor,
            ])
        return np.array(data, dtype=float)

    def fit(self, normal_records: list[TelemetryRecord]) -> None:
        """Trains the isolation forest on nominal/healthy calibration data."""
        if len(normal_records) < 10:
            raise ValueError("Need at least 10 calibration records to fit Isolation Forest")
        x_train = self._extract_features(normal_records)
        self.model.fit(x_train)
        self.is_fitted = True

    def predict(self, records: list[TelemetryRecord]) -> list[AnomalyRecord]:
        """Predicts anomalies across a batch of telemetry records."""
        if not self.is_fitted:
            # Auto-fit if enough records provided
            if len(records) >= 20:
                self.fit(records[:20])
            else:
                return []

        x_test = self._extract_features(records)
        preds = self.model.predict(x_test)  # -1 for anomaly, 1 for inlier
        scores = self.model.decision_function(x_test)  # lower = more anomalous

        anomalies: list[AnomalyRecord] = []
        for i, (p, s, rec) in enumerate(zip(preds, scores, records)):
            if p == -1:
                severity = SeverityLevel.CRITICAL if s < -0.15 else SeverityLevel.WARNING
                anomalies.append(
                    AnomalyRecord(
                        device_id=rec.device_id,
                        timestamp_ms=rec.timestamp_ms,
                        anomaly_type=AnomalyType.CORRELATED_SEIZURE,
                        detector_name="ISOLATION_FOREST",
                        severity=severity,
                        metric_name="multivariate_embedding",
                        actual_value=round(float(s), 4),
                        expected_value=0.10,
                        score=round(float(-s), 4),
                        threshold=0.0,
                        description=f"Isolation Forest outlier: multivariate anomaly score {s:.3f}",
                    )
                )

        return anomalies
