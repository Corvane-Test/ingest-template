#!/usr/bin/env python3
"""Load generator and truth report for the ingest service.

This script behaves like a fleet of at-least-once producers.  It sends unique
usage batches to ``POST /events``, retries failures, redelivers some
already-acknowledged batches, and then compares three independent sources of
truth to say what actually happened to every event:

* what the client was told (HTTP 2xx means the event was acknowledged),
* what the database holds (read with ``psql`` inside the Postgres pod),
* what the upstream rating provider committed (``GET /stats`` on the mock).

It also watches ``kubectl get pod -l app=ingest`` for the whole run and
records every container restart together with its termination reason.

Everything the service receives (the set of events, customers and payloads) is
a pure function of ``--seed``.  Timing-dependent effects (retries, which
requests fail) are not.  Only the Python standard library is used.

Metrics, in report order:

    sent        unique events attempted
    acked       unique events that received a 2xx on any attempt
    failed      unique events with no 2xx after every attempt
    rows        rows in the events table
    duplicates  rows minus distinct event_id
    lost        acknowledged event ids with no row
    unacked     acknowledged event ids that upstream /stats never saw
    p50/p95/max client-observed latency of every attempt of every request,
                including retries, timeouts and redeliveries
    restarts    ingest container restarts during the run, by reason
    duration    seconds from the first request to the last response

The script exits 0 whenever it printed a report.  The numbers are the result;
it asserts nothing.  It exits 2 if a precondition (truncate, reset) fails or
the post-run database read fails.
"""

from __future__ import annotations

import argparse
import heapq
import http.client
import json
import math
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_URL = "http://localhost:8080"
DEFAULT_STATS_URL = "http://localhost:8081"
DEFAULT_EVENTS = 3000
DEFAULT_CONCURRENCY = 20
DEFAULT_SEED = 42

CLIENT_TIMEOUT = 5.0  # seconds, applied to connect and to each socket read
MAX_RETRIES = 3  # retries after the first attempt
RETRY_JITTER = (0.2, 1.0)  # seconds, uniform

LONG_CUSTOMER_RATE = 0.03  # share of events with a nested sub-account path
LONG_CUSTOMER_LENGTH = (33, 256)  # characters
SLUG_POOL_SIZE = 2000  # short account slugs

REDELIVERY_RATE = 0.05  # share of events resent after being acknowledged
REDELIVERY_DELAY = (0.5, 2.0)  # seconds, uniform

# Records per usage batch.  One record serializes to about 198 bytes, so 160
# records make a payload of about 31.6 KB and a request body of about 31.7 KB.
# Retune here to change the request body size.
PAYLOAD_RECORDS = 160

MONITOR_INTERVAL = 2.0  # seconds between pod polls
SETTLE_POLL = 1.0  # seconds between row-count polls after the run
SETTLE_STABLE_POLLS = 2  # unchanged polls that mean the service is quiet
SETTLE_MAX = 30.0  # seconds to wait for the service to go quiet
KUBECTL_TIMEOUT = 60.0  # seconds per kubectl call

DB_USER = "ingest"
DB_NAME = "ingest"
POSTGRES_SELECTOR = "app=postgres"
INGEST_SELECTOR = "app=ingest"

BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)
EPOCH_SPAN = 30 * 24 * 3600  # record timestamps fall in a 30 day window

ADJECTIVES = (
    "amber azure bold brisk calm clear crisp daring eager fair "
    "gentle golden grand happy ivory jolly keen lively lucid merry "
    "noble olive open placid proud quick quiet rapid royal sharp "
    "silver smooth solid steady sunny swift tidy true vivid warm "
    "wise young zesty bright"
).split()
NOUNS = (
    "anchor beacon bridge cedar comet delta ember falcon forge garden "
    "harbor island jungle kernel lantern meadow nebula orchard prairie "
    "quarry ridge summit tundra valley willow zephyr atlas bison canyon "
    "dune estuary fjord glacier haven isle junction knoll lagoon mesa "
    "oasis pier reef shore thicket upland vista wharf yard zenith "
    "basin cove grove"
).split()
REGIONS = ("eu-west", "eu-north", "us-east", "us-west", "ap-south", "ap-east", "sa-east")
SITE_PREFIXES = ("plant", "campus", "depot", "lab", "hub", "yard")
SUBUNITS = ("line", "cell", "rack", "bay", "zone", "floor")
DEPARTMENTS = ("ops", "billing", "research", "logistics", "platform", "support", "fleet")

METERS: dict[str, tuple[str, ...]] = {
    "api_calls": ("request", "1k_requests"),
    "compute_seconds": ("second", "vcpu_second"),
    "storage_gb_hours": ("gb_hour",),
    "egress_gb": ("gb",),
    "messages_sent": ("message", "1k_messages"),
    "active_seats": ("seat_day",),
}
RESOURCE_KINDS = ("node", "bucket", "queue", "worker", "gateway", "volume")
TAG_ENVS = ("prod", "staging", "batch")
TAG_TEAMS = ("core", "data", "edge", "ml", "billing", "search")
TAG_SKUS = ("std", "pro", "ent", "metered")

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Event:
    """One usage batch as a producer would send it."""

    index: int
    event_id: str
    customer: str
    body: bytes
    redelivery_delay: float | None  # None when this event is never resent


@dataclass
class Totals:
    """Mutable run state shared by worker threads."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    latencies: list[float] = field(default_factory=list)
    acked: set[str] = field(default_factory=set)
    failed: set[str] = field(default_factory=set)

    def add_latencies(self, values: list[float]) -> None:
        with self.lock:
            self.latencies.extend(values)

    def mark(self, event_id: str, ok: bool) -> None:
        with self.lock:
            (self.acked if ok else self.failed).add(event_id)


@dataclass
class RestartLog:
    """Restarts and pod replacements seen by the monitor thread."""

    reasons: dict[str, int] = field(default_factory=dict)
    pod_replacements: int = 0
    polls: int = 0
    poll_errors: int = 0

    @property
    def total(self) -> int:
        return sum(self.reasons.values())


class PreconditionError(RuntimeError):
    """A step the report depends on failed."""


# ---------------------------------------------------------------------------
# Event generation (pure function of the seed)
# ---------------------------------------------------------------------------


def build_slug_pool(rng: random.Random) -> list[str]:
    """Return SLUG_POOL_SIZE short account slugs (well under 32 characters)."""
    combos = [f"{a}-{n}" for a in ADJECTIVES for n in NOUNS]
    rng.shuffle(combos)
    return combos[:SLUG_POOL_SIZE]


def build_long_customer(rng: random.Random, pool: list[str]) -> str:
    """Build a nested sub-account path of 33 to 256 characters."""
    low, high = LONG_CUSTOMER_LENGTH
    target = rng.randint(low, high)
    parts = [rng.choice(pool), rng.choice(REGIONS)]

    def fragments() -> list[str]:
        return [
            f"{rng.choice(SITE_PREFIXES)}-{rng.randint(1, 99):02d}",
            f"{rng.choice(SUBUNITS)}-{rng.randint(1, 12)}",
            rng.choice(DEPARTMENTS),
            rng.choice(pool),
        ]

    while len("/".join(parts)) < target:
        for fragment in fragments():
            candidate = "/".join(parts + [fragment])
            if len(candidate) > high:
                return "/".join(parts)
            parts.append(fragment)
            if len(candidate) >= target:
                return candidate
    return "/".join(parts)


def build_payload(rng: random.Random, event_id: str) -> dict:
    """Build a usage batch: a meter name and PAYLOAD_RECORDS usage records."""
    meter = rng.choice(sorted(METERS))
    records = []
    for _ in range(PAYLOAD_RECORDS):
        ts = BASE_TIME + timedelta(seconds=rng.randrange(EPOCH_SPAN))
        records.append(
            {
                "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "unit": rng.choice(METERS[meter]),
                "quantity": round(rng.uniform(0.01, 5000.0), 3),
                "resource": f"{rng.choice(RESOURCE_KINDS)}-{rng.getrandbits(32):08x}",
                "tags": {
                    "env": rng.choice(TAG_ENVS),
                    "team": rng.choice(TAG_TEAMS),
                    "sku": rng.choice(TAG_SKUS),
                    "batch": event_id,
                },
            }
        )
    return {"meter": meter, "records": records}


def pick_indices(rng: random.Random, count: int, rate: float) -> set[int]:
    """Choose round(count * rate) distinct event indices."""
    return set(rng.sample(range(count), round(count * rate)))


def generate_events(count: int, seed: int) -> list[Event]:
    """Generate every event up front; the result depends only on the arguments."""
    rng = random.Random(seed)
    pool = build_slug_pool(rng)
    long_indices = pick_indices(rng, count, LONG_CUSTOMER_RATE)
    redelivered = pick_indices(rng, count, REDELIVERY_RATE)
    events = []
    for index in range(count):
        event_id = f"evt-{index:06d}-{rng.getrandbits(48):012x}"
        if index in long_indices:
            customer = build_long_customer(rng, pool)
        else:
            customer = rng.choice(pool)
        payload = build_payload(rng, event_id)
        delay = rng.uniform(*REDELIVERY_DELAY) if index in redelivered else None
        body = json.dumps(
            {"event_id": event_id, "customer": customer, "payload": payload}
        ).encode()
        events.append(Event(index, event_id, customer, body, delay))
    return events


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------


def post_once(host: str, port: int, path: str, body: bytes) -> int | None:
    """Send one request.  Return the status code, or None on timeout or error."""
    conn = http.client.HTTPConnection(host, port, timeout=CLIENT_TIMEOUT)
    try:
        conn.request("POST", path, body, {"Content-Type": "application/json"})
        response = conn.getresponse()
        response.read()
        return response.status
    except (OSError, http.client.HTTPException):
        return None
    finally:
        conn.close()


def is_retryable(status: int | None) -> bool:
    return status is None or status >= 500


def deliver(
    event: Event, target: urllib.parse.ParseResult, seed: int
) -> tuple[bool, list[float]]:
    """Send an event with retries.  Return (acknowledged, latency of each attempt)."""
    jitter = random.Random(f"{seed}:{event.index}:jitter")
    latencies: list[float] = []
    host, port = target.hostname or "localhost", target.port or 80
    for attempt in range(MAX_RETRIES + 1):
        started = time.monotonic()
        status = post_once(host, port, "/events", event.body)
        latencies.append(time.monotonic() - started)
        if status is not None and 200 <= status < 300:
            return True, latencies
        if not is_retryable(status) or attempt == MAX_RETRIES:
            break
        time.sleep(jitter.uniform(*RETRY_JITTER))
    return False, latencies


class WorkQueue:
    """Hands out new events, plus redeliveries once they come due."""

    def __init__(self, count: int) -> None:
        self._cond = threading.Condition()
        self._count = count
        self._next_new = 0
        self._in_flight_new = 0
        self._redeliveries: list[tuple[float, int]] = []

    def take(self) -> tuple[str, int] | None:
        """Return ("new" | "redeliver", index), or None when all work is done."""
        with self._cond:
            while True:
                now = time.monotonic()
                if self._redeliveries and self._redeliveries[0][0] <= now:
                    return "redeliver", heapq.heappop(self._redeliveries)[1]
                if self._next_new < self._count:
                    index = self._next_new
                    self._next_new += 1
                    self._in_flight_new += 1
                    return "new", index
                if self._redeliveries:
                    self._cond.wait(self._redeliveries[0][0] - now)
                elif self._in_flight_new:
                    self._cond.wait()
                else:
                    return None

    def schedule_redelivery(self, index: int, delay: float) -> None:
        with self._cond:
            heapq.heappush(self._redeliveries, (time.monotonic() + delay, index))
            self._cond.notify_all()

    def finish_new(self) -> None:
        with self._cond:
            self._in_flight_new -= 1
            self._cond.notify_all()


def worker(
    queue: WorkQueue,
    events: list[Event],
    totals: Totals,
    target: urllib.parse.ParseResult,
    seed: int,
) -> None:
    """Pull work until the queue is drained."""
    while (task := queue.take()) is not None:
        kind, index = task
        event = events[index]
        ok, latencies = deliver(event, target, seed)
        totals.add_latencies(latencies)
        if kind == "new":
            totals.mark(event.event_id, ok)
            if ok and event.redelivery_delay is not None:
                queue.schedule_redelivery(index, event.redelivery_delay)
            queue.finish_new()


def run_load(
    events: list[Event], concurrency: int, target: urllib.parse.ParseResult, seed: int
) -> tuple[Totals, float]:
    """Run all events through `concurrency` workers.  Return totals and duration."""
    totals = Totals()
    queue = WorkQueue(len(events))
    threads = [
        threading.Thread(target=worker, args=(queue, events, totals, target, seed))
        for _ in range(concurrency)
    ]
    started = time.monotonic()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return totals, time.monotonic() - started


# ---------------------------------------------------------------------------
# kubectl, database and upstream access
# ---------------------------------------------------------------------------


def kubectl(*args: str) -> str:
    """Run kubectl and return stdout; raise PreconditionError on failure."""
    try:
        done = subprocess.run(
            ["kubectl", *args],
            capture_output=True,
            text=True,
            timeout=KUBECTL_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PreconditionError(f"kubectl {' '.join(args[:2])} failed: {exc}") from exc
    if done.returncode != 0:
        raise PreconditionError(
            f"kubectl {' '.join(args[:2])} exited {done.returncode}: {done.stderr.strip()}"
        )
    return done.stdout


def postgres_pod() -> str:
    name = kubectl(
        "get", "pod", "-l", POSTGRES_SELECTOR, "-o", "jsonpath={.items[0].metadata.name}"
    ).strip()
    if not name:
        raise PreconditionError(f"no pod matches label {POSTGRES_SELECTOR}")
    return name


def psql(pod: str, sql: str) -> str:
    """Run one SQL statement inside the Postgres pod; return unaligned output."""
    return kubectl(
        "exec", pod, "--", "psql", "-U", DB_USER, "-d", DB_NAME, "-At", "-c", sql
    )


def http_json(url: str, method: str = "GET") -> dict:
    request = urllib.request.Request(url, method=method, data=b"" if method == "POST" else None)
    with urllib.request.urlopen(request, timeout=10) as response:
        raw = response.read()
    return json.loads(raw) if raw.strip() else {}


def prepare(pod: str, stats_url: str) -> None:
    """Truncate the events table and reset upstream stats, or raise."""
    try:
        psql(pod, "TRUNCATE events")
    except PreconditionError as exc:
        raise PreconditionError(f"cannot truncate the events table: {exc}") from exc
    try:
        http_json(f"{stats_url}/reset", "POST")
    except (OSError, urllib.error.URLError, ValueError) as exc:
        raise PreconditionError(f"cannot reset upstream stats at {stats_url}: {exc}") from exc


def read_database(pod: str) -> tuple[int, int, set[str]]:
    """Return (rows, distinct event ids, set of stored event ids)."""
    counts = psql(pod, "SELECT count(*), count(DISTINCT event_id) FROM events").strip()
    rows_text, distinct_text = counts.split("|")
    ids = psql(pod, "SELECT DISTINCT event_id FROM events").split()
    return int(rows_text), int(distinct_text), set(ids)


def read_upstream_acks(stats_url: str) -> set[str]:
    try:
        stats = http_json(f"{stats_url}/stats")
    except (OSError, urllib.error.URLError, ValueError) as exc:
        raise PreconditionError(f"cannot read upstream stats at {stats_url}: {exc}") from exc
    return set(stats.get("acked_ids") or [])


def settle(url: str) -> None:
    """Wait until /events/count stops changing, so abandoned requests can finish."""
    deadline = time.monotonic() + SETTLE_MAX
    last, stable = None, 0
    while time.monotonic() < deadline and stable < SETTLE_STABLE_POLLS:
        try:
            count = http_json(f"{url}/events/count").get("count")
        except (OSError, urllib.error.URLError, ValueError):
            return
        stable = stable + 1 if count == last else 0
        last = count
        time.sleep(SETTLE_POLL)


# ---------------------------------------------------------------------------
# Restart monitor
# ---------------------------------------------------------------------------


def termination_reason(status: dict) -> str:
    terminated = (status.get("lastState") or {}).get("terminated") or {}
    return terminated.get("reason") or "Unknown"


class RestartMonitor:
    """Polls ingest pods and records restart count increases and replacements."""

    def __init__(self) -> None:
        self.log = RestartLog()
        self._seen: dict[str, str] = {}  # pod uid -> pod name
        self._counts: dict[tuple[str, str], int] = {}  # (uid, container) -> restartCount
        self._first = True
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self) -> None:
        self.poll()  # baseline before any load is sent
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()
        self.poll()  # catch a restart that happened after the last tick

    def _loop(self) -> None:
        while not self._stop.wait(MONITOR_INTERVAL):
            self.poll()

    def poll(self) -> None:
        try:
            listing = json.loads(kubectl("get", "pod", "-l", INGEST_SELECTOR, "-o", "json"))
        except (PreconditionError, ValueError):
            self.log.poll_errors += 1
            return
        self.log.polls += 1
        self._observe(listing.get("items") or [])

    def _observe(self, pods: list[dict]) -> None:
        present = {pod["metadata"]["uid"]: pod for pod in pods if "uid" in pod.get("metadata", {})}
        for uid in [u for u in self._seen if u not in present]:
            del self._seen[uid]
            self.log.pod_replacements += 1
        for uid, pod in present.items():
            self._seen[uid] = pod["metadata"].get("name", uid)
            for status in pod.get("status", {}).get("containerStatuses") or []:
                self._record(uid, status)
        self._first = False

    def _record(self, uid: str, status: dict) -> None:
        key = (uid, status.get("name", ""))
        current = int(status.get("restartCount", 0))
        if key not in self._counts:
            # A pod present at the first poll starts from its current count.
            # A pod first seen later was created during the run, so it starts at 0.
            self._counts[key] = current if self._first else 0
        increase = current - self._counts[key]
        if increase > 0:
            reason = termination_reason(status)
            self.log.reasons[reason] = self.log.reasons.get(reason, 0) + increase
            self._counts[key] = current


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def percentile(sorted_values: list[float], fraction: float) -> float:
    """Nearest-rank percentile of an already sorted list."""
    if not sorted_values:
        return 0.0
    rank = min(len(sorted_values), max(1, math.ceil(len(sorted_values) * fraction)))
    return sorted_values[rank - 1]


def build_report(
    sent: int,
    totals: Totals,
    stored: tuple[int, int, set[str]],
    upstream_acked: set[str],
    restarts: RestartLog,
    duration: float,
) -> dict:
    rows, distinct, stored_ids = stored
    latencies = sorted(totals.latencies)
    return {
        "sent": sent,
        "acked": len(totals.acked),
        "failed": len(totals.failed),
        "rows": rows,
        "duplicates": rows - distinct,
        "lost": len(totals.acked - stored_ids),
        "unacked": len(totals.acked - upstream_acked),
        "p50": round(percentile(latencies, 0.50), 3),
        "p95": round(percentile(latencies, 0.95), 3),
        "max": round(latencies[-1], 3) if latencies else 0.0,
        "restarts": {"total": restarts.total, "reasons": dict(sorted(restarts.reasons.items()))},
        "duration": round(duration, 1),
        "throughput": round(sent / duration, 1) if duration > 0 else 0.0,
        "pod_replacements": restarts.pod_replacements,
    }


def format_report(report: dict) -> str:
    reasons = " ".join(f"{k}={v}" for k, v in report["restarts"]["reasons"].items())
    restarts = f"{report['restarts']['total']}" + (f"  {reasons}" if reasons else "")
    lines = [
        ("sent", report["sent"]),
        ("acked", report["acked"]),
        ("failed", report["failed"]),
        ("rows", report["rows"]),
        ("duplicates", report["duplicates"]),
        ("lost", report["lost"]),
        ("unacked", report["unacked"]),
        ("p50", f"{report['p50']:.3f} s"),
        ("p95", f"{report['p95']:.3f} s"),
        ("max", f"{report['max']:.3f} s"),
        ("restarts", restarts),
        ("duration", f"{report['duration']:.1f} s"),
        ("throughput", f"{report['throughput']:.1f} events/s"),
        ("pod_replacements", report["pod_replacements"]),
    ]
    return "\n".join(f"{name:<18}{value}" for name, value in lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load generator and truth report for ingest.")
    parser.add_argument("--events", type=int, default=DEFAULT_EVENTS, help="unique events to send")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="parallel senders")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="seed for the event set")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    parser.add_argument("--url", default=DEFAULT_URL, help="ingest base URL")
    parser.add_argument("--stats-url", default=DEFAULT_STATS_URL, help="upstream stats base URL")
    args = parser.parse_args(argv)
    if args.events < 1 or args.concurrency < 1:
        parser.error("--events and --concurrency must be at least 1")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    target = urllib.parse.urlparse(args.url)
    try:
        pod = postgres_pod()
        prepare(pod, args.stats_url)
    except PreconditionError as exc:
        print(f"load: precondition failed: {exc}", file=sys.stderr)
        return 2

    events = generate_events(args.events, args.seed)
    monitor = RestartMonitor()
    monitor.start()
    totals, duration = run_load(events, args.concurrency, target, args.seed)
    settle(args.url)
    monitor.stop()

    try:
        report = build_report(
            len(events), totals, read_database(pod), read_upstream_acks(args.stats_url),
            monitor.log, duration,
        )
    except (PreconditionError, ValueError) as exc:
        print(f"load: cannot read results after the run: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2) if args.json else format_report(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
