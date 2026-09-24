"""Unit tests for Redis Streams & Memory Stream Buffering with Backpressure."""
from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.models.schemas import SensorType, TelemetryRecord
from src.streaming.memory_stream import MemoryStreamEngine
from src.streaming.redis_stream import RedisStreamEngine


def _make_dummy_record(i: int) -> TelemetryRecord:
    return TelemetryRecord(
        device_id=f"device_{i % 3}",
        timestamp_ms=int(time.time() * 1000) + i,
        sensor_type=SensorType.MOTOR,
        temperature=50.0 + (i % 5),
        vibration_rms=0.8,
        current=20.0,
    )


class TestMemoryStreamEngine:
    def test_add_and_metrics(self):
        engine = MemoryStreamEngine(stream_name="test_stream", max_len=100)
        rec = _make_dummy_record(1)
        msg_id = engine.add(rec)
        assert msg_id is not None
        assert "-" in msg_id

        m = engine.get_metrics()
        assert m.backlog_count == 1
        assert m.dropped_count == 0

    def test_backpressure_drop_handling(self):
        max_len = 5
        engine = MemoryStreamEngine(stream_name="test_stream", max_len=max_len)
        for i in range(12):
            engine.add(_make_dummy_record(i))

        m = engine.get_metrics()
        assert m.backlog_count == max_len
        assert m.dropped_count == 7  # 12 - 5 = 7 dropped

    def test_consumer_group_read_and_ack(self):
        engine = MemoryStreamEngine(stream_name="test_stream", max_len=100)
        group_name = "test_grp"
        engine.create_consumer_group(group_name)

        # Add 3 messages
        for i in range(3):
            engine.add(_make_dummy_record(i))

        # Consumer 1 reads
        readings = engine.read_group(group_name, consumer_name="worker_01", count=2)
        assert len(readings) == 2
        mid1, rec1 = readings[0]

        # Check PEL
        pel = engine.get_pending_entries(group_name)
        assert len(pel) == 2

        # Acknowledge first
        assert engine.ack(group_name, mid1) is True
        pel_after = engine.get_pending_entries(group_name)
        assert len(pel_after) == 1

    def test_pel_claim_stale_recovery(self):
        engine = MemoryStreamEngine(stream_name="test_stream", max_len=100)
        group_name = "recovery_grp"
        engine.create_consumer_group(group_name)

        engine.add(_make_dummy_record(1))
        # Dead worker read it
        readings = engine.read_group(group_name, consumer_name="crashed_worker", count=1)
        assert len(readings) == 1

        # Reclaim with 0ms idle time
        reclaimed = engine.claim_stale(group_name, new_consumer="rescue_worker", min_idle_time_ms=0)
        assert len(reclaimed) == 1
        assert reclaimed[0][0] == readings[0][0]


class TestRedisStreamEngineFallback:
    def test_redis_fallback_when_server_absent(self):
        # Invalid port to force fallback to memory engine
        engine = RedisStreamEngine(redis_url="redis://localhost:9999/0", use_fallback_if_unavailable=True)
        assert engine.is_connected_to_redis is False

        # Should operate smoothly via fallback
        engine.create_consumer_group("grp1")
        msg_id = engine.add(_make_dummy_record(1))
        assert msg_id is not None

        entries = engine.read_group("grp1", "worker_01", count=1)
        assert len(entries) == 1
        assert engine.ack("grp1", entries[0][0]) is True
