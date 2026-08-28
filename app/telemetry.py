"""Custom metrics.

The SDK, exporters and auto-instrumentation are set up by
``opentelemetry-instrument`` from OTEL_* environment variables. Without it the
counters below are no-ops.
"""

from __future__ import annotations

from opentelemetry import metrics

meter = metrics.get_meter("ingest")

events_received = meter.create_counter(
    "events_received_total",
    description="Events accepted for storage.",
)
events_stored = meter.create_counter(
    "events_stored_total",
    description="Events durably stored.",
)
