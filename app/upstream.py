"""HTTP client for the RateGrid rating API: POST /enrich and POST /ack.

A call that does not return a 2xx raises UpstreamError.
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.config import settings

log = logging.getLogger(__name__)

ENRICH = "enrich"
ACK = "ack"


class UpstreamError(Exception):
    """An upstream call did not succeed."""

    def __init__(self, endpoint: str, message: str) -> None:
        super().__init__(f"{endpoint}: {message}")
        self.endpoint = endpoint


# 5s timeout, 3 retries with backoff
_session = requests.Session()
_session.timeout = settings.upstream_timeout  # seconds
_retry = Retry(total=3, backoff_factor=0.2, status_forcelist=[502, 503, 504])
_session.mount("http://", HTTPAdapter(max_retries=_retry))


def _post(endpoint: str, body: dict[str, Any]) -> requests.Response:
    url = f"{settings.upstream_url}/{endpoint}"
    try:
        response = _session.post(url, json=body)
    except requests.RequestException as exc:
        log.debug("upstream call failed endpoint=%s error=%s", endpoint, type(exc).__name__)
        raise UpstreamError(endpoint, f"{type(exc).__name__}: {exc}") from exc
    if not response.ok:
        log.debug("upstream call failed endpoint=%s status=%s", endpoint, response.status_code)
        raise UpstreamError(endpoint, f"HTTP {response.status_code}")
    return response


def enrich(event_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Price the batch and reserve quota. Return the enrichment fields."""
    response = _post(ENRICH, {"event_id": event_id, "payload": payload})
    return response.json()


def ack(event_id: str) -> None:
    """Commit the reservation made by enrich."""
    _post(ACK, {"event_id": event_id})


def close() -> None:
    _session.close()
