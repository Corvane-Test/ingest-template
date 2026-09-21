# ingest

`ingest` is Corvane's usage-metering intake. Corvane's product services report
billable usage in batches. `ingest` prices each batch through RateGrid, our
third-party rating provider, stores it in Postgres, and commits it with
RateGrid. Invoices are built from the `events` table, so every usage batch must
land there exactly once.

## Highlights

- **Exactly-once delivery.** The events table enforces uniqueness on
  `event_id`, so client retries are always safe.
- **Stateless.** Dedupe state lives in the shared Redis cache, so the service
  scales horizontally.
- **Resilient upstream client.** Upstream calls time out after 5 seconds and
  are retried 3 times with backoff.
- **Async I/O throughout.** The service is a FastAPI app served by uvicorn.
- **Predictable footprint.** Memory use is flat at about 150 MB. The
  normalization cache only holds repeated payloads.
- **Rolling updates with zero downtime.**
- **Observable by default.** Every request is traced. The Ingest overview
  dashboard shows storage success and error rates.

## Quick start

Prerequisites: Docker, k3d, kubectl, and Python 3.11 or newer.

```sh
make up      # build the image, create the k3d cluster, apply k8s/
make load    # send a default load run and print the report
```

`make up` creates the k3d cluster `ingest-lab`, builds `ingest:latest`, imports
it and the RateGrid sandbox image into the cluster, applies everything in
`k8s/`, and waits until every pod is ready. When it finishes it prints:

| What | URL |
| --- | --- |
| ingest API | http://localhost:8080 |
| Grafana | http://localhost:3000 |
| RateGrid sandbox stats | http://localhost:8081/stats |

Grafana opens on the **Ingest overview** dashboard. Anonymous access has the
Admin role, so you can edit panels and use Explore without logging in.

## Make targets

| Target | What it does |
| --- | --- |
| `make up` | Builds `ingest:latest`, creates the cluster with host ports 8080, 3000, and 8081, imports the images, applies `k8s/`, waits for every pod, and prints the URLs |
| `make build` | Builds `ingest:latest` from the `Dockerfile` |
| `make load` | Runs `tools/load.py` with its default settings |
| `make deploy` | Rebuilds and imports the image, deletes the `ingest` pod, and applies `k8s/ingest-pod.yaml` again |
| `make logs` | Follows the ingest logs |
| `make grafana` | Prints the Grafana URL |
| `make down` | Deletes the cluster |

## API

### `POST /events`

Stores one usage batch.

```json
{
  "event_id": "evt-4f1c2a9e",
  "customer": "amber-harbor",
  "payload": {
    "meter": "api_calls",
    "records": [
      {
        "ts": "2026-01-14T09:30:00Z",
        "unit": "request",
        "quantity": 1250,
        "resource": "gateway-0a1b2c3d",
        "tags": {"env": "prod", "team": "core"}
      }
    ]
  }
}
```

| Field | Type | Rules |
| --- | --- | --- |
| `event_id` | string | 1 to 64 characters. The idempotency key of the batch. |
| `customer` | string | Up to 256 characters. Account slug; enterprise sub-accounts use nested paths. |
| `payload` | object | The usage batch: a meter name and a list of usage records |

| Response | Body | When |
| --- | --- | --- |
| 200 | `{"status": "stored"}` | The batch was processed |
| 200 | `{"status": "duplicate"}` | The `event_id` was already processed |
| 422 | FastAPI validation error | The body does not match the rules above |
| 502 | `{"detail": "upstream enrich call failed"}` or `{"detail": "upstream ack call failed"}` | RateGrid returned a non-2xx response or could not be reached |
| 503 | `{"detail": "storage unavailable, retry later"}`, with `Retry-After: 1` | The service could not reconnect to Postgres |

### `GET /events/count`

Returns the number of rows in the `events` table.

```json
{"count": 2914}
```

### `GET /healthz`

Liveness endpoint. Returns 200 with `{"status": "ok"}`.

## Request flow

`POST /events` runs these steps in order:

1. Validate the body. Invalid input returns 422.
2. Check the dedupe store for the `event_id`. If it was already processed,
   return `{"status": "duplicate"}`.
3. Price the batch and reserve quota with RateGrid (`POST /enrich`). A non-2xx
   response returns 502.
4. Normalize the payload (`app/normalize.py`).
5. Count the event in `events_received_total`.
6. Insert the event into `events` with its normalized payload and the RateGrid
   response (`INSERT ... ON CONFLICT DO NOTHING`).
7. Count the event in `events_stored_total`.
8. Commit the reservation with RateGrid (`POST /ack`). A non-2xx response
   returns 502.
9. Mark the `event_id` in the dedupe store.
10. Return `{"status": "stored"}`.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design and
[docs/RUNBOOK.md](docs/RUNBOOK.md) for on-call procedures.

## Configuration

All settings come from environment variables, set in `k8s/ingest-pod.yaml`.

| Variable | Purpose | Value in the Pod |
| --- | --- | --- |
| `DATABASE_URL` | Postgres connection string | `postgresql://ingest:ingest@postgres:5432/ingest` |
| `UPSTREAM_URL` | RateGrid base URL | `http://upstream:8080` |
| `UPSTREAM_TIMEOUT` | Timeout setting for the RateGrid client, in seconds | `5` |
| `CACHE_URL` | Redis endpoint for the shared dedupe store | `redis://redis.platform.svc:6379/0` |
| `LOG_LEVEL` | Python log level | `INFO` |
| `OTEL_SERVICE_NAME` | Service name on all telemetry | `ingest` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint of the LGTM stack | `http://lgtm:4317` |
| `OTEL_METRIC_EXPORT_INTERVAL` | Metric export interval, in milliseconds | `15000` |
| `OTEL_LOGS_EXPORTER` | Log exporter | `none` |
| `OTEL_TRACES_SAMPLER` | Trace sampler | `parentbased_traceidratio` |
| `OTEL_TRACES_SAMPLER_ARG` | Sampler argument | `0.01` |

Two optional settings tune the database connection: `DB_CONNECT_TIMEOUT`
(seconds, default 5) and `DB_CONNECT_RETRY_INTERVAL` (seconds between connection
attempts at startup, default 2).

## Observability

The container starts the app with `opentelemetry-instrument`, which configures
the OpenTelemetry SDK from the `OTEL_*` variables and instruments FastAPI,
`requests`, `psycopg2`, and process metrics. Traces and metrics go over OTLP to
the `lgtm` pod, which runs the OpenTelemetry Collector, Prometheus, Tempo, Loki,
and Grafana in one container.

`app/telemetry.py` adds two counters, both with a `customer` attribute:

| Metric | Description |
| --- | --- |
| `events_received_total` | Events accepted for storage |
| `events_stored_total` | Events durably stored |

The **Ingest overview** dashboard (`dashboards/ingest-overview.json`) is
provisioned as the Grafana home dashboard. Its panels are Request rate,
Rejected requests, Storage success, Request latency, Upstream latency, Memory,
and Events by customer. Edits made in the Grafana UI are not saved back to the
repo; export the JSON and commit it.

Application logs are plain text on stdout. Use `make logs` to follow them.

## Repository layout

```
app/              FastAPI service
  main.py         routes and request flow
  config.py       settings read from the environment
  db.py           Postgres connection and statement helper
  upstream.py     RateGrid client (/enrich and /ack)
  dedupe.py       dedupe backends
  normalize.py    payload normalization
  telemetry.py    custom metrics
  logging_conf.py logging setup
k8s/              manifests applied by make up
dashboards/       Grafana dashboard JSON
tools/load.py     load generator and report
docs/             architecture and runbook
```
