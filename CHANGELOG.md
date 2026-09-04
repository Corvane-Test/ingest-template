# Changelog

## 1.6.0

- OpenTelemetry metrics and traces through `opentelemetry-instrument`, exported
  over OTLP to the `lgtm` stack.
- Custom counters `events_received_total` and `events_stored_total`.
- Ingest overview dashboard, provisioned as the Grafana home dashboard.

## 1.5.0

- Dedupe moved to Redis.
- `CACHE_URL` added to the Pod manifest.

## 1.4.0

- Upstream timeouts and retries.
- New setting `UPSTREAM_TIMEOUT`, 5 seconds by default.

## 1.3.0

- Unique index on `event_id`.
- The insert uses `ON CONFLICT DO NOTHING`.

## 1.2.0

- Payload normalization: trimmed strings, lower-cased meter and unit names,
  parsed timestamps and quantities, records ordered by timestamp and resource,
  and a per-resource summary under `resources`.
- Liveness probe on `/healthz`.

## 1.1.0

- `GET /events/count` returns the number of stored rows.
- `make deploy` rebuilds the image and replaces the running pod.

## 1.0.0

- `POST /events` prices each usage batch with RateGrid (`/enrich`), stores it in
  Postgres, and commits it with RateGrid (`/ack`).
- `GET /healthz`.
- Local k3d cluster with `make up`, `make load`, `make logs`, and `make down`.
