"""Webhook dispatcher with exponential backoff retries and Dead-Letter Queue (DLQ)."""
from __future__ import annotations

import logging
import time
from typing import Callable, List, Optional

import httpx

from src.models.schemas import AlertEvent

logger = logging.getLogger(__name__)


class DeadLetterRecord:
    """Record of a permanently failed alert dispatch."""
    def __init__(self, alert: AlertEvent, error: str, attempts: int, timestamp_ms: int):
        self.alert = alert
        self.error = error
        self.attempts = attempts
        self.timestamp_ms = timestamp_ms


class AlertDispatcher:
    """Delivers alerts via webhooks with retries, DLQ routing, and operator ACK log."""

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        max_retries: int = 3,
        initial_backoff_sec: float = 0.5,
        backoff_multiplier: float = 2.0,
        custom_sender: Optional[Callable[[AlertEvent], bool]] = None,
    ):
        self.webhook_url = webhook_url
        self.max_retries = max_retries
        self.initial_backoff_sec = initial_backoff_sec
        self.backoff_multiplier = backoff_multiplier
        self.custom_sender = custom_sender

        # In-memory incident log and DLQ
        self._alerts_log: List[AlertEvent] = []
        self._dlq: List[DeadLetterRecord] = []
        self._dispatched_count: int = 0

    def dispatch(self, alert: AlertEvent) -> bool:
        """Dispatches alert to webhook/custom sender with exponential backoff."""
        self._alerts_log.append(alert)

        # If custom sender provided (used in tests/simulations)
        if self.custom_sender:
            return self._dispatch_with_callback(alert, self.custom_sender)

        # If webhook URL configured
        if self.webhook_url:
            return self._dispatch_http_webhook(alert)

        # If neither, log and mark dispatched
        self._dispatched_count += 1
        return True

    def _dispatch_with_callback(self, alert: AlertEvent, sender: Callable[[AlertEvent], bool]) -> bool:
        attempt = 0
        backoff = self.initial_backoff_sec

        while attempt < self.max_retries:
            attempt += 1
            try:
                success = sender(alert)
                if success:
                    self._dispatched_count += 1
                    return True
            except Exception as e:
                logger.warning("Alert dispatch attempt %d failed: %s", attempt, e)

            if attempt < self.max_retries:
                time.sleep(backoff)
                backoff *= self.backoff_multiplier

        # Route to DLQ on permanent failure
        self._route_to_dlq(alert, f"Callback failed after {self.max_retries} attempts", attempt)
        return False

    def _dispatch_http_webhook(self, alert: AlertEvent) -> bool:
        attempt = 0
        backoff = self.initial_backoff_sec

        with httpx.Client(timeout=3.0) as client:
            while attempt < self.max_retries:
                attempt += 1
                try:
                    resp = client.post(
                        self.webhook_url,
                        json=alert.model_dump(),
                        headers={"Content-Type": "application/json"},
                    )
                    if resp.status_code in (200, 201, 202, 204):
                        self._dispatched_count += 1
                        return True
                    else:
                        logger.warning("Webhook returned HTTP %d on attempt %d", resp.status_code, attempt)
                except Exception as e:
                    logger.warning("Webhook network error on attempt %d: %s", attempt, e)

                if attempt < self.max_retries:
                    time.sleep(backoff)
                    backoff *= self.backoff_multiplier

        self._route_to_dlq(alert, f"HTTP POST to {self.webhook_url} failed after {self.max_retries} attempts", attempt)
        return False

    def _route_to_dlq(self, alert: AlertEvent, error: str, attempts: int) -> None:
        logger.error("Routing alert %s to Dead-Letter Queue (DLQ): %s", alert.alert_id, error)
        now_ms = int(time.time() * 1000)
        self._dlq.append(DeadLetterRecord(alert, error, attempts, now_ms))

    def acknowledge_alert(self, alert_id: str, operator_name: str = "Operator_01") -> bool:
        """Marks an alert incident as acknowledged."""
        now_ms = int(time.time() * 1000)
        for alt in self._alerts_log:
            if alt.alert_id == alert_id:
                alt.acknowledged = True
                alt.acknowledged_at_ms = now_ms
                alt.acknowledged_by = operator_name
                return True
        return False

    def get_recent_alerts(self, limit: int = 50) -> List[AlertEvent]:
        """Returns recent incident alerts."""
        return list(reversed(self._alerts_log[-limit:]))

    def get_dlq_records(self) -> List[DeadLetterRecord]:
        """Returns all Dead-Letter Queue records."""
        return list(self._dlq)

    @property
    def dispatched_count(self) -> int:
        return self._dispatched_count

    @property
    def dlq_count(self) -> int:
        return len(self._dlq)

    def clear(self) -> None:
        self._alerts_log.clear()
        self._dlq.clear()
        self._dispatched_count = 0
