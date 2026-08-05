"""Redis-backed dedupe shared by all replicas. LocalBackend exists for unit tests."""

from __future__ import annotations

import logging
import time
from typing import Protocol

import redis

from app.config import settings

log = logging.getLogger(__name__)

# How long a processed event ID is remembered.
TTL_SECONDS = 24 * 3600

KEY_PREFIX = "ingest:event:"


class DedupeBackend(Protocol):
    def seen(self, event_id: str) -> bool: ...

    def mark(self, event_id: str) -> None: ...


class LocalBackend:
    """Event IDs kept in a dict in process memory, with the same TTL as Redis."""

    def __init__(self, ttl: float = TTL_SECONDS) -> None:
        self._ttl = ttl
        self._expires: dict[str, float] = {}

    def seen(self, event_id: str) -> bool:
        expires = self._expires.get(event_id)
        if expires is None:
            return False
        if expires < time.monotonic():
            del self._expires[event_id]
            return False
        return True

    def mark(self, event_id: str) -> None:
        self._expires[event_id] = time.monotonic() + self._ttl


class RedisBackend:
    """Event IDs kept in Redis as keys that expire after the TTL."""

    def __init__(self, url: str, ttl: int = TTL_SECONDS) -> None:
        self._ttl = ttl
        self._client = redis.Redis.from_url(url, socket_connect_timeout=1.0, socket_timeout=1.0)

    def seen(self, event_id: str) -> bool:
        return bool(self._client.exists(KEY_PREFIX + event_id))

    def mark(self, event_id: str) -> None:
        self._client.set(KEY_PREFIX + event_id, 1, ex=self._ttl)


def get_backend() -> DedupeBackend:
    if settings.redis_url:
        log.debug("dedupe backend: redis")
        return RedisBackend(settings.redis_url)
    log.debug("dedupe backend: local")
    return LocalBackend()
