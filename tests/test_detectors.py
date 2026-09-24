"""Unit tests for Streaming Anomaly Detectors (EWMA, Z-Score, Correlation, Isolation Forest)."""
from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.detector.correlation_detector import MultiSensorCorrelationDetector
from src.detector.ewma_detector import EWMADetector
from src.detector.isolation_forest import IsolationForestBenchmarkDetector
from src.detector.zscore_detector import RobustZScoreDetector
from src.models.schemas import AnomalyType, SensorType, TelemetryRecord


def _make_rec(
    dev: str = "motor_unit_01",
    temp: float = 50.0,
    vib: float = 0.8,
    curr: float = 20.0,
    t_offset: int = 0,
) -> TelemetryRecord:
    return TelemetryRecord(
        device_id=dev,
        timestamp_ms=int(time.time() * 1000) + t_offset,
        sensor_type=SensorType.MOTOR,
        temperature=temp,
        vibration_rms=vib,
        current=curr,
    )


class TestEWMADetector:
    def test_nominal_stream_produces_no_anomalies(self):
        detector = EWMADetector()
        # Feed 15 steady readings
        for i in range(15):
            anoms = detector.process(_make_rec(temp=50.0 + (i % 2) * 0.1, t_offset=i * 100))
            assert len(anoms) == 0

    def test_sudden_step_change_triggers_anomaly(self):
        detector = EWMADetector(threshold_sigma=3.0)
        # Establish baseline
        for i in range(10):
            detector.process(_make_rec(temp=50.0, t_offset=i * 100))

        # Sudden jump from 50 to 95 degrees
        anoms = detector.process(_make_rec(temp=95.0, t_offset=1100))
        assert len(anoms) >= 1
        anom = anoms[0]
        assert anom.anomaly_type == AnomalyType.THERMAL_DRIFT
        assert anom.actual_value == 95.0
        assert anom.score > 3.0


class TestRobustZScoreDetector:
    def test_transient_spike_detected(self):
        detector = RobustZScoreDetector(window_size=20, threshold=3.5)
        # Baseline vibration around 0.8g
        for i in range(15):
            detector.process(_make_rec(vib=0.80 + (i % 3) * 0.02, t_offset=i * 100))

        # Extreme shock spike to 5.2g
        anoms = detector.process(_make_rec(vib=5.20, t_offset=1600))
        assert len(anoms) >= 1
        v_anom = [a for a in anoms if "vibration" in a.metric_name][0]
        assert v_anom.anomaly_type == AnomalyType.VIBRATION_SPIKE
        assert v_anom.score >= 3.5


class TestMultiSensorCorrelationDetector:
    def test_correlated_seizure_flagged(self):
        detector = MultiSensorCorrelationDetector(fault_threshold=3.0)
        # Establish baseline
        for i in range(12):
            detector.process(_make_rec(temp=50.0, vib=0.8, curr=20.0, t_offset=i * 100))

        # Simultaneous jump across all 3 domains
        anoms = detector.process(_make_rec(temp=85.0, vib=4.5, curr=45.0, t_offset=1300))
        assert len(anoms) >= 1
        assert anoms[0].anomaly_type in (AnomalyType.CORRELATED_SEIZURE, AnomalyType.BEARING_FATIGUE)
        assert anoms[0].score >= 3.0


class TestIsolationForestBenchmarkDetector:
    def test_train_and_predict_outlier(self):
        import random
        rng = random.Random(42)
        detector = IsolationForestBenchmarkDetector(contamination=0.10, random_state=42)

        # Generate normal batch with standard empirical sensor variance
        train_records = [
            _make_rec(
                temp=50.0 + rng.gauss(0, 0.4),
                vib=0.80 + abs(rng.gauss(0, 0.05)),
                curr=20.0 + rng.gauss(0, 0.4),
                t_offset=i * 10,
            )
            for i in range(80)
        ]
        detector.fit(train_records)
        assert detector.is_fitted is True

        # Test normal vs extreme outlier
        test_records = [
            _make_rec(temp=50.1, vib=0.82, curr=20.1, t_offset=1000),
            _make_rec(temp=120.0, vib=8.5, curr=95.0, t_offset=1010),  # Extreme outlier
        ]
        anoms = detector.predict(test_records)
        assert len(anoms) >= 1
        assert any(a.actual_value < 0.0 for a in anoms)  # decision function < 0
