"""Integration tests for live Redis Streams and MQTT Mosquitto broker services.
Executes real XREADGROUP, XACK, XCLAIM, and MQTT publish/subscribe flows when services are online,
with graceful fallback/skip when running in isolated offline environments.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import paho.mqtt.publish as publish
import pytest
import redis

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.ingestion.mqtt_subscriber import MQTTTelemetrySubscriber
from src.models.schemas import SensorType, TelemetryRecord
from src.streaming.redis_stream import RedisStreamEngine


def _make_sample_record(device_id: str = "motor_live_01", offset_s: int = 0) -> TelemetryRecord:
    now_ms = int(time.time() * 1000) + offset_s * 1000
    return TelemetryRecord(
        device_id=device_id,
        timestamp_ms=now_ms,
        sensor_type=SensorType.MOTOR,
        temperature=56.2,
        vibration_rms=1.12,
        vibration_kurtosis=3.25,
        current=25.4,
        voltage=400.0,
        power_factor=0.89,
        frequency=50.0,
    )


def is_live_redis_available(url: str) -> bool:
    try:
        r = redis.Redis.from_url(url, socket_timeout=1.0)
        return bool(r.ping())
    except Exception:
        return False


def is_live_mqtt_available(host: str, port: int) -> bool:
    import socket
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except Exception:
        return False


class TestLiveRedisStreamsIntegration:
    """Verifies true XREADGROUP, XACK, and XCLAIM against a live Redis server."""

    @pytest.fixture(autouse=True)
    def setup_engine(self):
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self.redis_url = redis_url
        self.is_available = is_live_redis_available(redis_url)
        self.engine = RedisStreamEngine(redis_url=redis_url, stream_name="test:live:telemetry:stream")

    def test_live_redis_consumer_group_and_xclaim_recovery(self):
        if not self.is_available:
            pytest.skip(f"Live Redis not available at {self.redis_url}; tested via fallback.")

        assert self.engine.is_connected_to_redis is True
        group_name = "test_grp_live"
        stream_name = "test:live:telemetry:stream"

        # Clean prior state in Redis
        r = redis.Redis.from_url(self.redis_url, decode_responses=True)
        r.delete(stream_name)

        # 1. Create consumer group in live Redis
        created = self.engine.create_consumer_group(group_name)
        assert created is True

        # 2. Add records via XADD
        msg_ids = []
        for i in range(5):
            rec = _make_sample_record(f"motor_{i}")
            mid = self.engine.add(rec)
            assert mid is not None
            msg_ids.append(mid)

        # 3. Read group via XREADGROUP
        pulled = self.engine.read_group(group_name, consumer_name="crashed_consumer", count=5)
        assert len(pulled) == 5

        # 4. Check PEL (Pending Entries List) in live Redis
        pending_count = self.engine.get_pending_count(group_name)
        assert pending_count == 5

        # 5. Simulate crash: Reclaim unacknowledged messages via XCLAIM
        reclaimed = self.engine.claim_stale(
            group_name=group_name,
            new_consumer="standby_consumer",
            min_idle_time_ms=0,
            count=5,
        )
        assert len(reclaimed) == 5

        # 6. Acknowledge messages via XACK
        for mid, _ in reclaimed:
            ack_ok = self.engine.ack(group_name, mid)
            assert ack_ok is True

        # 7. Verify PEL is cleared
        pending_after = self.engine.get_pending_count(group_name)
        assert pending_after == 0

        # Teardown
        r.delete(stream_name)


class TestLiveMQTTIntegration:
    """Verifies true MQTT message publishing and subscriber intake."""

    def test_live_mqtt_publish_and_subscriber_ingestion(self):
        mqtt_host = os.environ.get("MQTT_HOST", "localhost")
        mqtt_port = int(os.environ.get("MQTT_PORT", 1883))

        if not is_live_mqtt_available(mqtt_host, mqtt_port):
            pytest.skip(f"Live MQTT broker not reachable at {mqtt_host}:{mqtt_port}; skipping live subscriber test.")

        received_records = []

        def on_record(record: TelemetryRecord):
            received_records.append(record)

        subscriber = MQTTTelemetrySubscriber(
            broker_host=mqtt_host,
            broker_port=mqtt_port,
            topic="industrial/telemetry/test",
            on_record_received=on_record,
        )
        subscriber.start()

        try:
            time.sleep(0.5)
            # Publish a valid JSON telemetry payload to MQTT broker
            sample_rec = _make_sample_record("mqtt_test_device_42")
            json_payload = sample_rec.model_dump_json()

            publish.single(
                topic="industrial/telemetry/test",
                payload=json_payload,
                hostname=mqtt_host,
                port=mqtt_port,
            )

            # Wait for subscriber to receive packet
            timeout = 3.0
            t0 = time.time()
            while time.time() - t0 < timeout:
                if received_records:
                    break
                time.sleep(0.1)

            assert len(received_records) >= 1
            assert received_records[0].device_id == "mqtt_test_device_42"
            assert abs(received_records[0].temperature - 56.2) < 1e-2

        finally:
            subscriber.stop()
