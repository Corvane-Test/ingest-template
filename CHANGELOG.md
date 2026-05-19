# Changelog

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
