"""Production Redis Streams client with consumer groups, PEL recovery, and graceful memory fallback."""
from __future__ import annotations

import json
import logging
from typing import List, Optional, Tuple

import redis

from src.models.schemas import BufferMetrics, TelemetryRecord
from src.streaming.memory_stream import MemoryStreamEngine

logger = logging.getLogger(__name__)


class RedisStreamEngine:
    """Redis Streams buffer client with fallback to MemoryStreamEngine."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        stream_name: str = "telemetry:stream",
        max_len: int = 50_000,
        use_fallback_if_unavailable: bool = True,
    ):
        self.redis_url = redis_url
        self.stream_name = stream_name
        self.max_len = max_len
        self.use_fallback = use_fallback_if_unavailable

        self._redis: Optional[redis.Redis] = None
        self._memory_engine = MemoryStreamEngine(stream_name=stream_name, max_len=max_len)
        self._is_live_redis = False
        self._dropped_count = 0

        self._connect()

    def _connect(self) -> None:
        try:
            r = redis.Redis.from_url(self.redis_url, decode_responses=True, socket_timeout=1.5)
            r.ping()
            self._redis = r
            self._is_live_redis = True
            logger.info("Connected successfully to Redis Streams at %s", self.redis_url)
        except Exception as e:
            self._is_live_redis = False
            self._redis = None
            if self.use_fallback:
                logger.info("External Redis not detected (%s). Active: MemoryStreamEngine.", e)
            else:
                raise ConnectionError(f"Cannot connect to Redis at {self.redis_url}: {e}")

    @property
    def is_connected_to_redis(self) -> bool:
        return self._is_live_redis

    def create_consumer_group(self, group_name: str) -> bool:
        if not self._is_live_redis or self._redis is None:
            return self._memory_engine.create_consumer_group(group_name)

        try:
            self._redis.xgroup_create(self.stream_name, group_name, id="0", mkstream=True)
            return True
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                return True
            logger.warning("Error creating Redis consumer group: %s", e)
            return False

    def add(self, record: TelemetryRecord) -> str:
        if not self._is_live_redis or self._redis is None:
            return self._memory_engine.add(record)

        try:
            data = {"payload": record.model_dump_json()}
            msg_id = self._redis.xadd(
                self.stream_name,
                fields=data,
                maxlen=self.max_len,
                approximate=True,
            )
            return str(msg_id)
        except Exception as e:
            logger.warning("Redis XADD failed (%s), writing to fallback memory buffer", e)
            return self._memory_engine.add(record)

    def read_group(
        self,
        group_name: str,
        consumer_name: str,
        count: int = 10,
    ) -> List[Tuple[str, TelemetryRecord]]:
        if not self._is_live_redis or self._redis is None:
            return self._memory_engine.read_group(group_name, consumer_name, count=count)

        try:
            raw = self._redis.xreadgroup(
                group_name,
                consumer_name,
                streams={self.stream_name: ">"},
                count=count,
                block=50,
            )
            records: List[Tuple[str, TelemetryRecord]] = []
            if raw:
                for _, entries in raw:
                    for msg_id, fields in entries:
                        rec = TelemetryRecord.model_validate_json(fields["payload"])
                        records.append((str(msg_id), rec))
            return records
        except Exception as e:
            logger.error("Redis XREADGROUP error: %s", e)
            return self._memory_engine.read_group(group_name, consumer_name, count=count)

    def ack(self, group_name: str, msg_id: str) -> bool:
        if not self._is_live_redis or self._redis is None:
            return self._memory_engine.ack(group_name, msg_id)

        try:
            res = self._redis.xack(self.stream_name, group_name, msg_id)
            return res > 0
        except Exception as e:
            logger.error("Redis XACK error: %s", e)
            return False

    def get_pending_count(self, group_name: str) -> int:
        if not self._is_live_redis or self._redis is None:
            return self._memory_engine.get_pending_count(group_name)
        return len(self.get_pending_entries(group_name))

    def get_pending_entries(self, group_name: str) -> List[dict]:
        if not self._is_live_redis or self._redis is None:
            return self._memory_engine.get_pending_entries(group_name)

        try:
            pel = self._redis.xpending_range(self.stream_name, group_name, min="-", max="+", count=50)
            return [
                {
                    "msg_id": item["message_id"],
                    "consumer": item["consumer"],
                    "idle_time_ms": item["idle_time"],
                }
                for item in pel
            ]
        except Exception:
            return []

    def claim_stale(
        self,
        group_name: str,
        new_consumer: str,
        min_idle_time_ms: int = 5000,
        min_idle_ms: Optional[int] = None,
        count: Optional[int] = None,
    ) -> List[Tuple[str, TelemetryRecord]]:
        idle_threshold = min_idle_ms if min_idle_ms is not None else min_idle_time_ms
        if not self._is_live_redis or self._redis is None:
            return self._memory_engine.claim_stale(
                group_name,
                new_consumer,
                min_idle_time_ms=idle_threshold,
                count=count,
            )

        try:
            pel = self.get_pending_entries(group_name)
            stale_ids = [item["msg_id"] for item in pel if item["idle_time_ms"] >= idle_threshold]
            if count is not None:
                stale_ids = stale_ids[:count]
            if not stale_ids:
                return []

            claimed = self._redis.xclaim(
                self.stream_name,
                group_name,
                new_consumer,
                min_idle_time=idle_threshold,
                message_ids=stale_ids,
            )
            reclaimed: List[Tuple[str, TelemetryRecord]] = []
            for msg_id, fields in claimed:
                rec = TelemetryRecord.model_validate_json(fields["payload"])
                reclaimed.append((str(msg_id), rec))
            return reclaimed
        except Exception as e:
            logger.error("Redis XCLAIM error: %s", e)
            return []

    def get_metrics(self) -> BufferMetrics:
        if not self._is_live_redis or self._redis is None:
            return self._memory_engine.get_metrics()

        try:
            info = self._redis.xinfo_stream(self.stream_name)
            groups = [g["name"] for g in self._redis.xinfo_groups(self.stream_name)]
            return BufferMetrics(
                stream_name=self.stream_name,
                backlog_count=info.get("length", 0),
                dropped_count=self._dropped_count,
                max_len=self.max_len,
                consumer_groups=groups,
            )
        except Exception:
            return self._memory_engine.get_metrics()
