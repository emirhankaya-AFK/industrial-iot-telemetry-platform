"""MQTT Telemetry Subscriber for industrial edge sensor feeds."""
from __future__ import annotations

import logging
from typing import Callable, Optional

try:
    import paho.mqtt.client as mqtt
    HAS_PAHO = True
except ImportError:
    mqtt = None
    HAS_PAHO = False

from src.models.protobuf_bridge import decode_json_telemetry, decode_protobuf_telemetry
from src.models.schemas import TelemetryRecord

logger = logging.getLogger(__name__)


class MQTTTelemetrySubscriber:
    """Async/Threaded MQTT client subscribing to industrial telemetry streams."""

    def __init__(
        self,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        topic: str = "industrial/telemetry/#",
        client_id: str = "edge_gateway_subscriber_01",
        on_record_received: Optional[Callable[[TelemetryRecord], None]] = None,
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.client_id = client_id
        self.on_record_received = on_record_received
        self._is_running = False

        if HAS_PAHO and mqtt is not None:
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id,
            )
            self._client.on_connect = self._on_connect
            self._client.on_message = self._on_message
        else:
            self._client = None

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            logger.info("Connected to MQTT Broker at %s:%d, subscribing to %s", self.broker_host, self.broker_port, self.topic)
            self._client.subscribe(self.topic, qos=1)
        else:
            logger.warning("Failed to connect to MQTT broker, return code %d", rc)

    def _on_message(self, client, userdata, msg):
        try:
            payload = msg.payload
            # Auto-detect binary protobuf vs JSON
            if payload.startswith(b"{"):
                record = decode_json_telemetry(payload)
            else:
                try:
                    record = decode_protobuf_telemetry(payload)
                except Exception:
                    record = decode_json_telemetry(payload)

            if self.on_record_received:
                self.on_record_received(record)
        except Exception as e:
            logger.error("Error processing MQTT telemetry message from %s: %s", msg.topic, e)

    def start(self) -> bool:
        """Starts MQTT listener background thread. Returns False if broker connection fails."""
        if not self._client:
            logger.info("paho-mqtt client not available. Gateway running in HTTP-only mode.")
            return False
        try:
            self._client.connect(self.broker_host, self.broker_port, keepalive=60)
            self._client.loop_start()
            self._is_running = True
            return True
        except Exception as e:
            logger.warning("Could not connect to MQTT broker (%s:%d): %s. Gateway running in HTTP-only mode.", self.broker_host, self.broker_port, e)
            self._is_running = False
            return False

    def stop(self) -> None:
        """Stops MQTT listener thread."""
        if self._is_running and self._client:
            self._client.loop_stop()
            self._client.disconnect()
            self._is_running = False

    @property
    def is_connected(self) -> bool:
        return self._is_running
