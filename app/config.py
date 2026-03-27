"""Settings read from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigError(RuntimeError):
    """A required setting is missing or invalid."""


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql://ingest:ingest@postgres:5432/ingest"
    upstream_url: str = "http://upstream:8080"

    # Postgres connection.
    db_connect_timeout: int = 5
    db_connect_retry_interval: float = 2.0

    log_level: str = "INFO"


def load_settings() -> Settings:
    return Settings(
        database_url=os.environ.get("DATABASE_URL") or Settings.database_url,
        upstream_url=os.environ.get("UPSTREAM_URL", Settings.upstream_url).rstrip("/"),
        db_connect_timeout=_int("DB_CONNECT_TIMEOUT", Settings.db_connect_timeout),
        db_connect_retry_interval=_float("DB_CONNECT_RETRY_INTERVAL", Settings.db_connect_retry_interval),
        log_level=os.environ.get("LOG_LEVEL", Settings.log_level).upper(),
    )


settings = load_settings()
