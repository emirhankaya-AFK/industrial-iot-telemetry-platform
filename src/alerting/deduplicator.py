"""Alert deduplication and cooldown engine to prevent operator alert storms."""
from __future__ import annotations

import time
from typing import Dict, List, Optional

from src.models.schemas import AlertEvent, AnomalyRecord, SeverityLevel


class AlertDeduplicator:
    """Filters and aggregates anomaly streams into actionable, rate-limited alert events."""

    def __init__(self, cooldown_seconds: int = 60):
        self.cooldown_seconds = cooldown_seconds
        # Key: (device_id, anomaly_type) -> (last_alert_time_ms, last_severity)
        self._last_alert: Dict[tuple[str, str], tuple[int, SeverityLevel]] = {}
        self._suppressed_count: int = 0

    def should_alert(self, anomaly: AnomalyRecord) -> bool:
        """Determines if an anomaly warrants emitting a new alert or should be debounced."""
        key = (anomaly.device_id, anomaly.anomaly_type.value)
        now_ms = int(time.time() * 1000)
        cooldown_ms = self.cooldown_seconds * 1000

        if key not in self._last_alert:
            self._last_alert[key] = (now_ms, anomaly.severity)
            return True

        last_time, last_sev = self._last_alert[key]
        elapsed = now_ms - last_time

        # Severity escalation override: if condition worsened to CRITICAL, bypass cooldown!
        if last_sev != SeverityLevel.CRITICAL and anomaly.severity == SeverityLevel.CRITICAL:
            self._last_alert[key] = (now_ms, anomaly.severity)
            return True

        # Standard cooldown debounce
        if elapsed >= cooldown_ms:
            self._last_alert[key] = (now_ms, anomaly.severity)
            return True

        self._suppressed_count += 1
        return False

    def group_anomalies_into_alert(self, device_id: str, anomalies: List[AnomalyRecord]) -> Optional[AlertEvent]:
        """Bundles one or more contemporaneous anomalies for a device into a consolidated AlertEvent."""
        eligible = [a for a in anomalies if self.should_alert(a)]
        if not eligible:
            return None

        # Determine highest severity
        has_critical = any(a.severity == SeverityLevel.CRITICAL for a in eligible)
        has_warning = any(a.severity == SeverityLevel.WARNING for a in eligible)
        overall_sev = (
            SeverityLevel.CRITICAL if has_critical
            else SeverityLevel.WARNING if has_warning
            else SeverityLevel.INFO
        )

        anom_types = list({a.anomaly_type.value for a in eligible})
        title = f"[{overall_sev.value}] Equipment Incident on {device_id}: {', '.join(anom_types)}"
        details = "; ".join(a.description for a in eligible)

        return AlertEvent(
            device_id=device_id,
            severity=overall_sev,
            title=title,
            details=details,
            anomalies=eligible,
        )

    @property
    def suppressed_alert_count(self) -> int:
        return self._suppressed_count

    def clear(self) -> None:
        self._last_alert.clear()
        self._suppressed_count = 0
