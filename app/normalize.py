"""Payload normalization before storage."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from typing import Any


class PayloadError(ValueError):
    """The payload is not a JSON object."""


def _clean(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return {str(k).strip(): _clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean(v) for v in value]
    return value


def _parse_timestamp(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return value


def _parse_quantity(value: Any) -> Any:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return value
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return value


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    result = dict(record)
    if "ts" in result:
        result["ts"] = _parse_timestamp(result["ts"])
    if "quantity" in result:
        result["quantity"] = _parse_quantity(result["quantity"])
    if isinstance(result.get("unit"), str):
        result["unit"] = result["unit"].lower()
    return result


def _record_key(record: dict[str, Any]) -> tuple[str, str]:
    return str(record.get("ts", "")), str(record.get("resource", ""))


def _resource_index(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Summarize usage per resource: record count and total quantity per unit."""
    index: dict[str, dict[str, Any]] = {}
    for record in records:
        resource = str(record.get("resource", ""))
        entry = index.setdefault(resource, {"records": 0, "quantity": {}})
        entry["records"] += 1
        quantity = record.get("quantity")
        if isinstance(quantity, Decimal):
            unit = str(record.get("unit", ""))
            entry["quantity"][unit] = entry["quantity"].get(unit, Decimal(0)) + quantity
    return index


# payloads repeat across client retries and redeliveries, so cache the parsed form
@lru_cache(maxsize=None)
def normalize(raw: str) -> dict[str, Any]:
    """Parse a JSON payload and return its canonical form.

    Strings are trimmed and the meter and unit names are lower-cased. Record
    timestamps become datetimes and quantities become Decimals, records are
    ordered by timestamp and resource so that the same batch always stores the
    same way, and a per-resource summary is added under ``resources``.
    """
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PayloadError(f"payload is not valid JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise PayloadError("payload must be a JSON object")

    result: dict[str, Any] = _clean(payload)
    if isinstance(result.get("meter"), str):
        result["meter"] = result["meter"].lower()
    records = result.get("records")
    if isinstance(records, list) and all(isinstance(r, dict) for r in records):
        ordered = sorted((_normalize_record(r) for r in records), key=_record_key)
        result["records"] = ordered
        result["resources"] = _resource_index(ordered)
    return result


def json_default(value: Any) -> Any:
    """``json.dumps`` hook for the types ``normalize`` produces."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"{type(value).__name__} is not JSON serializable")
