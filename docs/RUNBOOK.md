# Runbook

On-call procedures for `ingest`. Commands assume the local k3d cluster
`ingest-lab` and the `default` namespace.

## Where to look

| What | How |
| --- | --- |
| Pod status and restarts | `kubectl get pod ingest` and `kubectl describe pod ingest` |
| Logs | `make logs`, or `kubectl logs ingest --previous` after a restart |
| Dashboard | http://localhost:3000, home dashboard **Ingest overview** |
| Metrics and traces | Grafana Explore, with the Prometheus and Tempo data sources |
| Row count | `curl -s localhost:8080/events/count` |
| RateGrid commits | `curl -s localhost:8081/stats` |
| Database shell | `kubectl exec -it deploy/postgres -- psql -U ingest -d ingest` |

Logs are plain text at `LOG_LEVEL=INFO`. For more detail, set `LOG_LEVEL` to
`DEBUG` in `k8s/ingest-pod.yaml` and run `make deploy`.

## Symptoms

### Pod restarting

Memory pressure from traffic growth. Raise the memory limit.

1. Confirm the restart count with `kubectl get pod ingest`.
2. Raise `resources.limits.memory` in `k8s/ingest-pod.yaml`. The current limit
   is 256Mi.
3. Run `make deploy`.

### Duplicate events (double billing)

Not possible on our side. The client is resending with a new `event_id`.

1. Get the two invoice lines from billing and the customer they belong to.
2. Send both batches back to the producing team so they can fix their retry
   logic.

### Slow requests

Upstream is slow. Nothing to tune here, since calls already time out and retry.

1. Check the **Upstream latency** panel on the Ingest overview dashboard.
2. If RateGrid latency stays high, open a ticket with RateGrid.

### Missing events (usage missing from an invoice)

A failed insert returns a 500, so a missing event was never accepted. Ask the
client to resend.

1. Confirm the `event_id` has no row:
   `SELECT 1 FROM events WHERE event_id = '<event_id>';`
2. Ask the producing team to resend the batch with the same `event_id`.

### Reconciliation mismatch with RateGrid

Finance reports usage that RateGrid committed and the ledger does not hold, or
the other way around.

1. List the committed IDs with `curl -s localhost:8081/stats`. The `acked_ids`
   field holds every event ID RateGrid has committed since the last reset.
2. Compare them with `SELECT event_id FROM events;`.
3. Send the differing event IDs to finance with the customer for each one.

### 502 responses

A RateGrid call returned a non-2xx response or could not be reached. The
response body names the endpoint: `upstream enrich call failed` or
`upstream ack call failed`. Producers retry these batches.

### 503 responses

The service could not reconnect to Postgres. The response carries
`Retry-After: 1`.

1. Check `kubectl get pod -l app=postgres`.
2. Once Postgres is ready, run `make deploy` so `ingest` starts with a fresh
   connection.

## Routine operations

| Task | Command |
| --- | --- |
| Deploy a change | `make deploy` |
| Restart the service | `make deploy` |
| Follow logs | `make logs` |
| Run a load test | `make load` |
| Rebuild the cluster | `make down`, then `make up` |
