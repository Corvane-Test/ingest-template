"""FastAPI app: POST /events, GET /events/count, GET /healthz.

POST /events enriches an event with RateGrid, stores it, and acks it with
RateGrid.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Literal

import psycopg2
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app import db, upstream
from app.config import settings
from app.normalize import json_default, normalize
from app.upstream import UpstreamError

log = logging.getLogger("app.main")

logging.basicConfig(level=settings.log_level)


class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=64)
    customer: str = Field(max_length=256)
    payload: dict[str, Any]


class EventOut(BaseModel):
    status: Literal["stored", "duplicate"]


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    log.info("starting upstream_url=%s log_level=%s", settings.upstream_url, settings.log_level)
    db.connect()
    try:
        yield
    finally:
        db.close()
        upstream.close()
        log.info("shutdown complete")


app = FastAPI(title="ingest", version="1.6.0", lifespan=lifespan)


@app.post("/events", response_model=EventOut)
async def ingest_event(event: EventIn) -> EventOut:
    enriched = upstream.enrich(event.event_id, event.payload)
    normalized = normalize(json.dumps(event.payload, sort_keys=True))
    db.execute(
        db.INSERT_EVENT,
        (event.event_id, event.customer, json.dumps(normalized, default=json_default), json.dumps(enriched)),
    )
    upstream.ack(event.event_id)

    return EventOut(status="stored")


@app.get("/events/count")
async def count_events() -> dict[str, int]:
    return {"count": int(db.fetch_value(db.COUNT_EVENTS, critical=True))}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(UpstreamError)
async def on_upstream_error(_: Request, exc: UpstreamError) -> JSONResponse:
    log.debug("event failed: %s", exc)
    return JSONResponse({"detail": f"upstream {exc.endpoint} call failed"}, status_code=502)


@app.exception_handler(psycopg2.Error)
async def on_database_error(_: Request, exc: psycopg2.Error) -> JSONResponse:
    log.debug("event failed: %s", type(exc).__name__)
    if isinstance(exc, psycopg2.OperationalError | psycopg2.InterfaceError):
        return JSONResponse(
            {"detail": "storage unavailable, retry later"}, status_code=503, headers={"Retry-After": "1"}
        )
    return JSONResponse({"detail": "event could not be stored"}, status_code=500)
