"""Unified End-to-End Stream Processing Pipeline.
Orchestrates Authentication -> Stream Buffering -> Multi-Detector Anomaly Engine -> Deduplication -> Alerting.
"""
from __future__ import annotations

import collections
import time
from typing import Dict, List, Optional

from src.alerting.deduplicator import AlertDeduplicator
from src.alerting.dispatcher import AlertDispatcher
from src.detector.correlation_detector import MultiSensorCorrelationDetector
from src.detector.ewma_detector import EWMADetector
from src.detector.isolation_forest import IsolationForestBenchmarkDetector
from src.detector.zscore_detector import RobustZScoreDetector
from src.ingestion.authenticator import DeviceAuthenticator
from src.models.schemas import (
    AlertEvent,
    AnomalyRecord,
    AnomalyType,
    PipelineMetrics,
    TelemetryRecord,
)
from src.streaming.redis_stream import RedisStreamEngine


class StreamProcessor:
    """Central industrial IoT processing engine."""

    def __init__(
        self,
        stream_engine: Optional[RedisStreamEngine] = None,
        authenticator: Optional[DeviceAuthenticator] = None,
        deduplicator: Optional[AlertDeduplicator] = None,
        dispatcher: Optional[AlertDispatcher] = None,
        consumer_group: str = "anomaly-detectors",
        consumer_id: str = "worker_node_01",
        isolation_forest: Optional[IsolationForestBenchmarkDetector] = None,
        enable_isolation_forest: bool = False,
    ):
        self.stream = stream_engine or RedisStreamEngine()
        self.auth = authenticator or DeviceAuthenticator()
        self.dedup = deduplicator or AlertDeduplicator(cooldown_seconds=60)
        self.dispatcher = dispatcher or AlertDispatcher()

        self.consumer_group = consumer_group
        self.consumer_id = consumer_id
        self.stream.create_consumer_group(consumer_group)

        # Real-time online streaming detectors (sub-millisecond evaluation)
        self.ewma = EWMADetector()
        self.zscore = RobustZScoreDetector()
        self.correlation = MultiSensorCorrelationDetector()

        # Optional multivariate Isolation Forest baseline model
        self.isolation_forest = isolation_forest
        self.enable_isolation_forest = enable_isolation_forest or (isolation_forest is not None)

        # Operational metrics tracking
        self.events_ingested: int = 0
        self.events_processed: int = 0
        self.events_dropped: int = 0
        self.anomalies_detected: int = 0
        self._latencies: collections.deque[float] = collections.deque(maxlen=1000)
        self._start_time = time.perf_counter()

        # Recent anomaly log per device
        self._recent_anomalies: collections.deque[AnomalyRecord] = collections.deque(maxlen=200)
        self._live_telemetry_cache: Dict[str, collections.deque[TelemetryRecord]] = {}

    def classify_fault(self, anomalies: List[AnomalyRecord]) -> str:
        """Maps a collection of detector anomalies to a synthesized machinery failure mode."""
        if not anomalies:
            return "NOMINAL"

        types = {a.anomaly_type for a in anomalies}
        metrics = {a.metric_name for a in anomalies}

        has_vib = any(
            a.anomaly_type in (AnomalyType.VIBRATION_SPIKE, AnomalyType.BEARING_FATIGUE)
            or "vibration" in a.metric_name
            for a in anomalies
        )
        has_curr = any(
            a.anomaly_type in (AnomalyType.CURRENT_SURGE, AnomalyType.PHASE_IMBALANCE)
            or "current" in a.metric_name
            for a in anomalies
        )
        has_temp = any(
            a.anomaly_type == AnomalyType.THERMAL_DRIFT
            or "temperature" in a.metric_name
            for a in anomalies
        )

        # Multi-sensor electromechanical seizure: simultaneous severe vibration AND current surge
        if (has_vib and has_curr) or AnomalyType.CORRELATED_SEIZURE in types or "composite_fault_index" in metrics:
            if has_vib and has_curr:
                return "CORRELATED_SEIZURE"
            if AnomalyType.CORRELATED_SEIZURE in types:
                return "CORRELATED_SEIZURE"

        # Bearing fatigue characterized by high kurtosis & vibration RMS without current surge
        if AnomalyType.BEARING_FATIGUE in types or "vibration_kurtosis" in metrics or (has_vib and not has_curr):
            return "BEARING_FATIGUE"

        # Current surge / phase imbalance without severe vibration
        if AnomalyType.PHASE_IMBALANCE in types or AnomalyType.CURRENT_SURGE in types or "current" in metrics or (has_curr and not has_vib):
            return "PHASE_IMBALANCE"

        # Thermal runaway / progressive heating without mechanical/electrical surges
        if AnomalyType.THERMAL_DRIFT in types or "temperature" in metrics or has_temp:
            return "THERMAL_RUNAWAY"

        return anomalies[0].anomaly_type.value

    def ingest_record(self, record: TelemetryRecord) -> str:
        """Authenticates and appends telemetry reading to the stream buffer."""
        self.auth.authenticate(record)
        self.events_ingested += 1

        # Cache for live dashboard charts (last 100 per device)
        if record.device_id not in self._live_telemetry_cache:
            self._live_telemetry_cache[record.device_id] = collections.deque(maxlen=100)
        self._live_telemetry_cache[record.device_id].append(record)

        msg_id = self.stream.add(record)
        return msg_id

    def ingest_batch(self, records: List[TelemetryRecord]) -> List[str]:
        """Ingests a collection of readings."""
        msg_ids = []
        for r in records:
            msg_ids.append(self.ingest_record(r))
        return msg_ids

    def run_detectors(self, record: TelemetryRecord) -> List[AnomalyRecord]:
        """Runs the streaming detector ensemble on a single record."""
        anomalies: List[AnomalyRecord] = []
        anomalies.extend(self.ewma.process(record))
        anomalies.extend(self.zscore.process(record))
        anomalies.extend(self.correlation.process(record))

        if self.enable_isolation_forest and self.isolation_forest and self.isolation_forest.is_fitted:
            anomalies.extend(self.isolation_forest.predict([record]))

        return anomalies

    def process_pending_stream(self, count: int = 50) -> List[AlertEvent]:
        """Consumes buffered records from Redis Streams, runs detectors, and dispatches alerts."""
        entries = self.stream.read_group(self.consumer_group, self.consumer_id, count=count)
        alerts_emitted: List[AlertEvent] = []

        for msg_id, record in entries:
            t0 = time.perf_counter()

            # 1. Run multi-detector ensemble
            anomalies = self.run_detectors(record)

            if anomalies:
                self.anomalies_detected += len(anomalies)
                self._recent_anomalies.extend(anomalies)

                # 2. Deduplicate and group into alert
                alert = self.dedup.group_anomalies_into_alert(record.device_id, anomalies)
                if alert:
                    self.dispatcher.dispatch(alert)
                    alerts_emitted.append(alert)

            # 3. Acknowledge stream message
            self.stream.ack(self.consumer_group, msg_id)
            self.events_processed += 1

            latency_ms = (time.perf_counter() - t0) * 1000.0
            self._latencies.append(latency_ms)

        return alerts_emitted

    def process_record_sync(self, record: TelemetryRecord) -> List[AnomalyRecord]:
        """Direct synchronous execution for testing and immediate API responses."""
        self.ingest_record(record)
        anomalies = self.run_detectors(record)

        if anomalies:
            self.anomalies_detected += len(anomalies)
            self._recent_anomalies.extend(anomalies)
            alert = self.dedup.group_anomalies_into_alert(record.device_id, anomalies)
            if alert:
                self.dispatcher.dispatch(alert)

        self.events_processed += 1
        return anomalies

    def get_live_device_telemetry(self, device_id: str) -> List[TelemetryRecord]:
        """Returns recent time-series window for live chart visualization."""
        if device_id in self._live_telemetry_cache:
            return list(self._live_telemetry_cache[device_id])
        return []

    def get_recent_anomalies(self, limit: int = 50) -> List[AnomalyRecord]:
        """Returns most recent anomalies across all devices."""
        return list(reversed(list(self._recent_anomalies)))[:limit]

    def get_metrics(self) -> PipelineMetrics:
        """Calculates throughput, percentile latencies, and buffer metrics."""
        elapsed = max(time.perf_counter() - self._start_time, 0.001)
        throughput = self.events_processed / elapsed

        buf_metrics = self.stream.get_metrics()
        self.events_dropped = buf_metrics.dropped_count

        lat_list = sorted(self._latencies) if self._latencies else [0.0]
        n = len(lat_list)
        avg_lat = sum(lat_list) / n
        p95 = lat_list[int(n * 0.95)] if n > 0 else 0.0
        p99 = lat_list[int(n * 0.99)] if n > 0 else 0.0

        return PipelineMetrics(
            events_ingested=self.events_ingested,
            events_processed=self.events_processed,
            events_dropped=self.events_dropped,
            anomalies_detected=self.anomalies_detected,
            alerts_dispatched=self.dispatcher.dispatched_count,
            dlq_count=self.dispatcher.dlq_count,
            throughput_eps=round(throughput, 2),
            avg_latency_ms=round(avg_lat, 3),
            p95_latency_ms=round(p95, 3),
            p99_latency_ms=round(p99, 3),
            buffer=buf_metrics,
        )
