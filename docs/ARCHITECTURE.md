# Architecture

## Context

Corvane bills customers by usage. Each product service reports billable usage to
`ingest` in batches, and delivers each batch at least once: a producer retries a
batch after a timeout or a 5xx, and occasionally redelivers a batch it already
delivered. Every batch carries an `event_id`, its idempotency key.

`ingest` prices each batch with RateGrid, the third-party rating provider,
stores it in the `events` table, and commits it with RateGrid. Invoicing reads
the `events` table, and finance reconciles it against what RateGrid has
committed. A lost row is usage missing from an invoice, a duplicate row is
double billing, and a stored row that RateGrid never committed is a
reconciliation mismatch.

```
product services --usage batches--> ingest (FastAPI)
                                      |--> RateGrid      POST /enrich: price and reserve
                                      |--> Postgres      events ledger
                                      |--> RateGrid      POST /ack: commit
                                      |--> Redis         dedupe store
                                      '--OTLP--> lgtm (Collector, Prometheus, Tempo, Loki, Grafana)
```

## Components

| Component | Runs as | Details |
| --- | --- | --- |
| ingest | Pod `ingest`, NodePort Service `ingest` (30080 to container port 8000) | Python 3.12, FastAPI on uvicorn with one worker, `requests` for RateGrid, `psycopg2` for Postgres, OpenTelemetry auto-instrumentation |
| Postgres | Deployment and Service `postgres`, port 5432 | Postgres 16. The schema is loaded from the `postgres-init` ConfigMap on first start. |
| Redis | Platform-managed, `redis.platform.svc:6379` | Shared dedupe store. Holds processed event IDs for 24 hours. |
| RateGrid sandbox | Deployment and Service `upstream`, port 8080 | RateGrid's sandbox image, provided by the platform team. It implements the same API as production RateGrid. NodePort Service `upstream-stats` (30081) exposes it to the host. |
| lgtm | Deployment and Service `lgtm` | `grafana/otel-lgtm` with OTLP on 4317 (gRPC) and 4318 (HTTP) and Grafana on 3000. NodePort Service `grafana` (30000) exposes Grafana to the host. |

Everything runs in the `default` namespace of the k3d cluster `ingest-lab`. The
host reaches ingest on `localhost:8080`, Grafana on `localhost:3000`, and the
RateGrid sandbox on `localhost:8081`.

## RateGrid

RateGrid is a third-party API. We call it and cannot change it. Calls to RateGrid
pass through an API gateway in front of the RateGrid API, which retries failed
calls before an error reaches `ingest`.

| Endpoint | Request | Response |
| --- | --- | --- |
| `POST /enrich` | `{"event_id", "payload"}` | 200 `{"event_id", "unit_price", "currency", "plan", "reservation_id"}`. Prices the batch and reserves quota. |
| `POST /ack` | `{"event_id"}` | 204. Commits the reservation. |
| `GET /stats` | | 200 `{"acked_ids", "acked", "enrich", "ack"}`: the committed event IDs and per-endpoint call counters |
| `POST /reset` | | 204. Clears the stats. |

Both `/enrich` and `/ack` are idempotent per `event_id`: a repeated call returns
the same price, plan, and reservation. `GET /stats` and `POST /reset` are
sandbox endpoints that do not exist in production.

The client is `app/upstream.py`. Every call that does not return a 2xx raises
`UpstreamError`, which the app turns into a 502 naming the endpoint.

## Request flow

`POST /events` is an `async def` handler in `app/main.py`.

1. Validate the body: `event_id` 1 to 64 characters, `customer` up to 256
   characters, `payload` an object. Invalid input returns 422.
2. `dedupe.seen(event_id)`. If true, return 200 with `{"status": "duplicate"}`.
3. `upstream.enrich(event_id, payload)`. A non-2xx returns 502.
4. `normalize(json.dumps(payload, sort_keys=True))`.
5. Increment `events_received_total`.
6. `db.execute(INSERT_EVENT, ...)` with the normalized payload and the RateGrid
   response.
7. Increment `events_stored_total`.
8. `upstream.ack(event_id)`. A non-2xx returns 502.
9. `dedupe.mark(event_id)`.
10. Return 200 with `{"status": "stored"}`.

`GET /events/count` returns the row count of `events`. `GET /healthz` returns
200 with `{"status": "ok"}`.

The service holds one Postgres connection with autocommit on. `db.execute`
reconnects and retries once when the connection drops. At startup the app waits
until Postgres accepts a connection.

## Data model

The schema lives in the `postgres-init` ConfigMap in `k8s/postgres.yaml`.

```sql
CREATE TABLE IF NOT EXISTS events (
    id          bigserial PRIMARY KEY,
    event_id    text NOT NULL,
    customer    varchar(256) NOT NULL,
    payload     jsonb NOT NULL,
    enriched    jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_id)
);
```

The insert, in `app/db.py`:

```sql
INSERT INTO events (event_id, customer, payload, enriched)
VALUES (%s, %s, %s, %s)
ON CONFLICT DO NOTHING
```

`payload` holds the normalized batch and `enriched` holds the RateGrid `/enrich`
response.

## Normalization

`app/normalize.py` turns a payload into a canonical form so that the same batch
always stores the same way. Strings are trimmed, meter and unit names are
lower-cased, record timestamps are parsed as datetimes and quantities as
decimals, records are ordered by timestamp and resource, and a per-resource
summary of record count and total quantity is added under `resources`.
`normalize` is memoized with `functools.lru_cache`.

## Dedupe

`app/dedupe.py` defines two backends with the same interface, `seen` and `mark`.
`RedisBackend` stores each processed event ID as a key with a 24-hour TTL.
`LocalBackend` keeps the same data in a dict. `get_backend()` picks the backend
at startup.

## Observability

- The container runs `opentelemetry-instrument uvicorn app.main:app`. The SDK is
  configured from the `OTEL_*` variables in the Pod manifest.
- Auto-instrumentation produces FastAPI server spans and HTTP server metrics,
  `requests` client spans and metrics, `psycopg2` spans, and process memory and
  CPU metrics.
- `app/telemetry.py` adds `events_received_total` and `events_stored_total`,
  both with a `customer` attribute.
- Metrics are exported every 15 seconds. Traces go to Tempo. Application logs are
  plain text on stdout.
- Grafana's home dashboard is **Ingest overview**, loaded from
  `dashboards/ingest-overview.json` through the `grafana-dashboard-ingest`
  ConfigMap that `make up` creates.

## Deployment

`make up` builds the image and applies `k8s/`. `make deploy` rebuilds and
imports the image, deletes the `ingest` pod, and applies `k8s/ingest-pod.yaml`
again. The Pod has a liveness probe on `/healthz` and limits of 256Mi memory and
500m CPU.
