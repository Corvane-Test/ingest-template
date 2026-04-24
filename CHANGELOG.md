# Changelog

## 1.1.0

- `GET /events/count` returns the number of stored rows.
- `make deploy` rebuilds the image and replaces the running pod.

## 1.0.0

- `POST /events` prices each usage batch with RateGrid (`/enrich`), stores it in
  Postgres, and commits it with RateGrid (`/ack`).
- `GET /healthz`.
- Local k3d cluster with `make up`, `make load`, `make logs`, and `make down`.
