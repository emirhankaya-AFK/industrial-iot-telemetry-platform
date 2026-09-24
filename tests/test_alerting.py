"""Unit tests for Alert Deduplication, Cooldown, Dispatcher, and DLQ."""
from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.alerting.deduplicator import AlertDeduplicator
from src.alerting.dispatcher import AlertDispatcher
from src.models.schemas import AlertEvent, AnomalyRecord, AnomalyType, SeverityLevel


def _make_anomaly(
    dev: str = "motor_01",
    sev: SeverityLevel = SeverityLevel.WARNING,
    anom_type: AnomalyType = AnomalyType.THERMAL_DRIFT,
) -> AnomalyRecord:
    return AnomalyRecord(
        device_id=dev,
        timestamp_ms=int(time.time() * 1000),
        anomaly_type=anom_type,
        detector_name="EWMA",
        severity=sev,
        metric_name="temperature",
        actual_value=92.0,
        expected_value=55.0,
        score=4.2,
        threshold=3.2,
        description="High thermal runaway test",
    )


class TestAlertDeduplicator:
    def test_cooldown_suppresses_rapid_fire_alerts(self):
        dedup = AlertDeduplicator(cooldown_seconds=60)
        anom = _make_anomaly()

        # First alert accepted
        assert dedup.should_alert(anom) is True

        # Immediate second identical anomaly suppressed
        assert dedup.should_alert(anom) is False
        assert dedup.suppressed_alert_count == 1

    def test_severity_escalation_bypasses_cooldown(self):
        dedup = AlertDeduplicator(cooldown_seconds=60)
        warn_anom = _make_anomaly(sev=SeverityLevel.WARNING)
        crit_anom = _make_anomaly(sev=SeverityLevel.CRITICAL)

        # First WARNING accepted
        assert dedup.should_alert(warn_anom) is True

        # Second WARNING suppressed
        assert dedup.should_alert(warn_anom) is False

        # Escalation to CRITICAL should bypass cooldown!
        assert dedup.should_alert(crit_anom) is True


class TestAlertDispatcher:
    def test_dispatch_with_successful_sender(self):
        dispatched_items = []

        def mock_sender(alert: AlertEvent) -> bool:
            dispatched_items.append(alert)
            return True

        dispatcher = AlertDispatcher(custom_sender=mock_sender)
        anom = _make_anomaly()
        alert = AlertEvent(device_id="motor_01", severity=SeverityLevel.WARNING, title="Test", details="Test", anomalies=[anom])

        assert dispatcher.dispatch(alert) is True
        assert len(dispatched_items) == 1
        assert dispatcher.dispatched_count == 1
        assert dispatcher.dlq_count == 0

    def test_retries_and_dead_letter_queue_on_failure(self):
        attempts_seen = [0]

        def failing_sender(alert: AlertEvent) -> bool:
            attempts_seen[0] += 1
            raise ConnectionError("Destination server down")

        dispatcher = AlertDispatcher(
            custom_sender=failing_sender,
            max_retries=2,
            initial_backoff_sec=0.01,
            backoff_multiplier=1.0,
        )
        anom = _make_anomaly()
        alert = AlertEvent(device_id="motor_01", severity=SeverityLevel.CRITICAL, title="Fail Alert", details="Fail", anomalies=[anom])

        success = dispatcher.dispatch(alert)
        assert success is False
        assert attempts_seen[0] == 2
        assert dispatcher.dlq_count == 1

        dlq_records = dispatcher.get_dlq_records()
        assert len(dlq_records) == 1
        assert dlq_records[0].alert.alert_id == alert.alert_id

    def test_operator_acknowledgment(self):
        dispatcher = AlertDispatcher()
        alert = AlertEvent(device_id="pump_03", severity=SeverityLevel.WARNING, title="Warn", details="Warn")
        dispatcher.dispatch(alert)

        assert alert.acknowledged is False
        assert dispatcher.acknowledge_alert(alert.alert_id, operator_name="Emirhan") is True

        recent = dispatcher.get_recent_alerts()
        assert recent[0].acknowledged is True
        assert recent[0].acknowledged_by == "Emirhan"
