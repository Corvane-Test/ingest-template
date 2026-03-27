"""Postgres access: one shared connection and a statement helper."""

from __future__ import annotations

import logging
import time
from contextlib import suppress
from typing import Any

import psycopg2
from psycopg2.extensions import connection as Connection

from app.config import settings

log = logging.getLogger(__name__)

INSERT_EVENT = """
INSERT INTO events (event_id, customer, payload, enriched)
VALUES (%s, %s, %s, %s)
"""

COUNT_EVENTS = "SELECT count(*) FROM events"

_conn: Connection


def _open() -> Connection:
    conn = psycopg2.connect(settings.database_url, connect_timeout=settings.db_connect_timeout)
    conn.autocommit = True
    return conn


def connect() -> None:
    """Open the shared connection, waiting until the database accepts it."""
    global _conn
    while True:
        try:
            _conn = _open()
        except psycopg2.OperationalError as exc:
            log.warning(
                "database not ready, retrying",
                extra={"error": str(exc).strip(), "retry_in_s": settings.db_connect_retry_interval},
            )
            time.sleep(settings.db_connect_retry_interval)
        else:
            log.info("database connected")
            return


def _reconnect() -> None:
    global _conn
    with suppress(psycopg2.Error):
        _conn.close()
    _conn = _open()


def close() -> None:
    with suppress(psycopg2.Error):
        _conn.close()


def execute(sql: str, params: tuple[Any, ...] | None = None, *, critical: bool = False) -> int:
    """Run a statement and return the affected row count.

    Connection errors are retried once after a reconnect.
    Statement errors are raised to the caller.
    """
    for _ in range(2):
        try:
            with _conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount
        except psycopg2.OperationalError:
            _reconnect()
        except psycopg2.Error as exc:
            log.debug("statement failed pgcode=%s", exc.pgcode)
            if critical:
                raise
            return 0
    return 0


def fetch_value(sql: str, params: tuple[Any, ...] | None = None, *, critical: bool = True) -> Any:
    """Run a query and return the first column of its first row.

    Same connection handling as ``execute``.
    """
    for _ in range(2):
        try:
            with _conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return row[0] if row else None
        except psycopg2.OperationalError:
            _reconnect()
        except psycopg2.Error as exc:
            log.debug("statement failed pgcode=%s", exc.pgcode)
            if critical:
                raise
            return None
    return None
