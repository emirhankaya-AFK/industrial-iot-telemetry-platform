"""Deterministic in-memory stream buffer mimicking Redis Streams semantics.
Provides consumer groups, message acknowledgment (ACK), pending entries list (PEL),
and bounded backpressure with drop accounting without external Redis dependency.
"""
from __future__ import annotations

import collections
import time
from typing import Dict, List, Optional, Tuple

from src.models.schemas import BufferMetrics, TelemetryRecord


class StreamMessage:
    """A message stored inside the stream buffer."""
    def __init__(self, msg_id: str, record: TelemetryRecord, timestamp_ms: int):
        self.msg_id = msg_id
        self.record = record
        self.timestamp_ms = timestamp_ms


class MemoryStreamEngine:
    """In-memory streaming buffer with Redis Streams semantics."""

    def __init__(self, stream_name: str = "telemetry:stream", max_len: int = 50_000):
        self.stream_name = stream_name
        self.max_len = max_len
        self._messages: collections.deque[StreamMessage] = collections.deque()
        self._counter: int = 0
        self._dropped_count: int = 0

        # Consumer groups: {group_name: {last_delivered_idx: int, pending: {msg_id: (consumer, timestamp, record)}}}
        self._groups: Dict[str, dict] = {}

    def create_consumer_group(self, group_name: str) -> bool:
        """Creates a named consumer group."""
        if group_name not in self._groups:
            self._groups[group_name] = {
                "delivered_ids": set(),
                "pending": {},  # msg_id -> (consumer_name, delivery_time_ms, msg)
            }
            return True
        return False

    def add(self, record: TelemetryRecord) -> str:
        """Appends a TelemetryRecord to the stream (XADD). Drops oldest if max_len exceeded."""
        now_ms = int(time.time() * 1000)
        self._counter += 1
        msg_id = f"{now_ms}-{self._counter}"

        # Backpressure drop handling
        if len(self._messages) >= self.max_len:
            self._messages.popleft()
            self._dropped_count += 1

        msg = StreamMessage(msg_id=msg_id, record=record, timestamp_ms=now_ms)
        self._messages.append(msg)
        return msg_id

    def read_group(
        self,
        group_name: str,
        consumer_name: str,
        count: int = 10,
    ) -> List[Tuple[str, TelemetryRecord]]:
        """Reads unacknowledged messages for a consumer group (XREADGROUP)."""
        if group_name not in self._groups:
            self.create_consumer_group(group_name)

        group_meta = self._groups[group_name]
        delivered_ids: set = group_meta["delivered_ids"]
        pending: dict = group_meta["pending"]

        now_ms = int(time.time() * 1000)
        results: List[Tuple[str, TelemetryRecord]] = []

        for msg in list(self._messages):
            if len(results) >= count:
                break
            if msg.msg_id not in delivered_ids:
                delivered_ids.add(msg.msg_id)
                pending[msg.msg_id] = (consumer_name, now_ms, msg)
                results.append((msg.msg_id, msg.record))

        return results

    def ack(self, group_name: str, msg_id: str) -> bool:
        """Acknowledges successful processing of a message (XACK)."""
        if group_name in self._groups:
            pending = self._groups[group_name]["pending"]
            if msg_id in pending:
                del pending[msg_id]
                return True
        return False

    def get_pending_entries(self, group_name: str) -> List[dict]:
        """Inspects Pending Entries List (PEL) for unacknowledged messages."""
        if group_name not in self._groups:
            return []
        pending = self._groups[group_name]["pending"]
        now_ms = int(time.time() * 1000)
        return [
            {
                "msg_id": mid,
                "consumer": consumer,
                "idle_time_ms": now_ms - t_delivered,
            }
            for mid, (consumer, t_delivered, _) in pending.items()
        ]

    def claim_stale(
        self,
        group_name: str,
        new_consumer: str,
        min_idle_time_ms: int = 5000,
    ) -> List[Tuple[str, TelemetryRecord]]:
        """Reclaims un-ACKed messages from crashed consumers (XCLAIM)."""
        if group_name not in self._groups:
            return []
        now_ms = int(time.time() * 1000)
        pending = self._groups[group_name]["pending"]
        reclaimed = []

        for mid, (old_cons, t_del, msg) in list(pending.items()):
            if (now_ms - t_del) >= min_idle_time_ms:
                pending[mid] = (new_consumer, now_ms, msg)
                reclaimed.append((mid, msg.record))

        return reclaimed

    def get_metrics(self) -> BufferMetrics:
        """Returns buffer health and capacity metrics."""
        return BufferMetrics(
            stream_name=self.stream_name,
            backlog_count=len(self._messages),
            dropped_count=self._dropped_count,
            max_len=self.max_len,
            consumer_groups=list(self._groups.keys()),
        )

    def clear(self) -> None:
        """Empties the stream buffer."""
        self._messages.clear()
        for g in self._groups.values():
            g["delivered_ids"].clear()
            g["pending"].clear()
