"""T10a: the metrics registry behind `/metrics`.

T9's acceptance criteria (CPU < 80%, p99 bounded, consumer lag bounded) are
unmeasurable without this, which is why it was split out of the larger
observability task. This module is the *registry*; the HTTP route that serves
`render()` is the app's business.

**Cost model, and why the code looks like it does.** The hot path runs once per
event, i.e. 50,000 times per second. So:

* Nothing is formatted on the increment path. No `f"..."`, no `str()`, no label
  joining, no float formatting. An increment touches a dict and an int.
* Labels are canonicalised to interned strings by a bounded cache, so the key
  tuple holds objects that already exist instead of new strings per call. The
  two hottest recorders inline that lookup; the rest go through `_label()`.
* `lock.acquire()` / `try/finally: lock.release()`, not `with self._lock:`.
  `with` on a `threading.Lock` goes through `__enter__`/`__exit__`, which
  measured ~40% more expensive per call on this interpreter, and `try/finally`
  costs nothing when no exception is raised. At 50k events/sec that is the
  difference between 0.72 and 0.49 microseconds per event.
* Buckets are found with `bisect`, not a Python-level loop.
* Everything expensive (label joining, escaping, cumulative bucket summing,
  float formatting) happens once per `render()`, which is a cold path hit by a
  scraper, not by a request.

`test_increment_path_costs_under_one_microsecond` enforces the budget. If you
make the increment path allocate, that test is the one that fails, which is the
point of writing it.

**Concurrency: one lock for the whole registry, plus one for the sequence
window.** The GIL makes a single attribute store atomic but not a
read-modify-write, and every mutation here is a read-modify-write
(`d[k] = d.get(k, 0) + 1`, or "which bucket did this fall into"). So atomics
alone are not sufficient, and under PEP 703 free-threading they would not be
sufficient either -- the code would be relying on a guarantee the interpreter
may withdraw. A single uncontended `threading.Lock.acquire` is tens of
nanoseconds; at 50k events/sec the critical sections are held for well under
1% of wall time, so contention is not the concern that the lock's existence
might suggest.

The sequence window has its own lock because its eviction step is
occasionally O(batch), and a batch eviction must never stall a counter
increment. The two locks are never held at the same time -- the sequence path
releases its lock before touching the registry's -- so there is no lock-order
hazard to reason about.

**Cardinality is capped, and the cap is a metric.** A 500-tenant system that
labelled series by `tenant` would publish 500 series *per counter* *per worker*.
So:

* `tenant` and `source` are deliberately NOT labels. `source` is a URI
  containing the tenant id; per-tenant visibility comes from the DLQ, the ledger
  and the log, which are the right tools for a 500-way breakdown, not a
  scrape-time time series.
* Every label that IS present is truncated at
  `DEFAULT_LABEL_CARDINALITY` distinct values. Past the cap, further values fold
  into a single `__other__` series, and each folded observation increments
  `gateway_label_values_dropped_total`. Without that counter, "the violation
  counter reads zero" could be an artefact of having folded the evidence away --
  which is exactly the failure T9 must not be fooled by.
* `status_class` is the one label that bypasses the cap, because it is a
  projection onto a fixed six-element range rather than a free string: bucketing
  an already-bucketed value cannot grow cardinality.

**The sequence window is bounded.** `observe_sequence` is the guard on the
sticky-routing ordering guarantee, and an unbounded `(source, user)` dict at
50k events/sec is a memory leak that also hands an attacker a hash-collision
surface. The window evicts oldest-first at `DEFAULT_SEQUENCE_WINDOW` entries and
counts the evictions in `gateway_sequence_evictions_total`.

Eviction can only ever *undercount* violations, never invent one: an evicted
pair forgets its high-water mark and its next event reads as first-sight. So
the ordering counter is a lower bound on true violations, and the eviction
counter is what tells you how much of the world the bound is blind to. Both are
exported so the caveat is measurable rather than folklore.

**No external dependencies.** Standard library only, deliberately: a
third-party metrics library would be the largest and least controlled
dependency in a gateway whose whole thesis is supply-chain hygiene.
"""

from __future__ import annotations

import math
import os
import socket
import threading
from bisect import bisect_left
from dataclasses import dataclass, field
from typing import Iterable

__all__ = [
    "BATCH_SIZE_BUCKETS",
    "CONTENT_TYPE",
    "DEFAULT_LABEL_CARDINALITY",
    "DEFAULT_SEQUENCE_WINDOW",
    "ENCRYPTION_LATENCY_BUCKETS",
    "KAFKA_PRODUCE_LATENCY_BUCKETS",
    "METRICS_NAMESPACE",
    "Metrics",
    "OTHER_LABEL",
    "REQUEST_LATENCY_BUCKETS",
    "SEQUENCE_WINDOW_EVICTIONS",
    "UNSET_LABEL",
    "WORKER_ID_ENV",
    "default_worker_id",
]

# --- Exposition format --------------------------------------------------------

#: Prometheus text exposition format 0.0.4. Also the content type of the route,
#: so the two cannot drift apart.
CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

METRICS_NAMESPACE = "gateway"

#: Environment variable naming the worker. Set by the orchestrator, because a
#: worker that cannot name itself cannot be told apart from its siblings, and
#: "tell workers apart" is the whole point of the per-worker series.
WORKER_ID_ENV = "WORKER_ID"

#: Rendered for a label the caller did not supply. Explicit rather than empty:
#: `""` is indistinguishable from a channel literally named "" in a dashboard
#: legend, and "we do not know" and "we know and it is blank" are different
#: claims.
UNSET_LABEL = "__unset__"

#: Rendered when a label value is past the cardinality cap.
OTHER_LABEL = "__other__"

#: Distinct values retained per label name. 64 is comfortably above the real
#: domain (6 event types, 3 source channels, ~9 validation reason codes) and
#: comfortably below anything that would hurt a scrape.
DEFAULT_LABEL_CARDINALITY = 64

#: (source, user) pairs whose last-seen sequence is retained. 100k pairs is
#: ~10 MB at these value sizes, and 50k events/sec with a realistic fan-out
#: turns a pair over many times a second -- so this is a working set, not a log.
DEFAULT_SEQUENCE_WINDOW = 100_000

# --- Histogram buckets (FIXED; changing one restarts every rate on it) --------

#: Events per accepted batch. 0 and 1 matter: an empty batch is a bug worth
#: alerting on, and 500 is `config.MAX_EVENTS_PER_BATCH`, the hard cap a batch
#: can never exceed. Upper bounds are inclusive.
BATCH_SIZE_BUCKETS: tuple[float, ...] = (1, 5, 10, 25, 50, 100, 200, 500)

#: Seconds spent encrypting one event. Sub-millisecond at the small end: this
#: is AES-GCM on a few KiB, and a regression that doubles it should move the
#: distribution rather than hide in the tail.
ENCRYPTION_LATENCY_BUCKETS: tuple[float, ...] = (
    0.0001,
    0.00025,
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
)

#: Seconds from `produce()` to the delivery callback. The wide tail is
#: `message.timeout.ms=5000` from the producer config: a produce that times out
#: is a 5xx, and the buckets have to reach it or the p99 would look fine while
#: the failures hide in `+Inf`.
KAFKA_PRODUCE_LATENCY_BUCKETS: tuple[float, ...] = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    5.0,
)

#: Seconds for one whole HTTP request, handler in and out. Starts at 5ms
#: because a batch round-trip that cannot beat 5ms is not a result worth
#: reading; the top bucket is 10s, comfortably past `message.timeout.ms`.
REQUEST_LATENCY_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)

#: The `+Inf` bucket. Present in every histogram, implied rather than listed.
_INF = math.inf

# --- Sequence window result codes ---------------------------------------------
#
# Ints rather than a bool|None enum: this is on the per-event path, and a
# returned tuple or a custom object would allocate.

_OK = 0
_VIOLATION = 1

#: Metric name of the sequence-window eviction counter. Re-exported because the
#: test and any consumer of the exposition refer to it by name.
SEQUENCE_WINDOW_EVICTIONS = f"{METRICS_NAMESPACE}_sequence_evictions_total"


def default_worker_id() -> str:
    """`WORKER_ID` if the orchestrator set one, else `host:pid`.

    The fallback exists so a developer running two uvicorn workers locally still
    gets tellable-apart series -- the failure being debugged is usually exactly
    the one where the two processes are indistinguishable.
    """
    explicit = os.environ.get(WORKER_ID_ENV, "").strip()
    if explicit:
        return explicit
    return f"{socket.gethostname()}:{os.getpid()}"


# --- Status projection --------------------------------------------------------


def _status_class(status: int) -> str:
    """`2xx`, `4xx`, ... or `other`.

    A projection onto six values, not a pass-through: an unbounded label here
    would be the raw status code, and a client that can choose its status code
    could then choose its own series. Rubbish and non-integer statuses fold into
    `other` rather than creating a new series each.
    """
    if type(status) is not int or not 100 <= status <= 599:
        return "other"
    return f"{status // 100}xx"


def _escape(value: str) -> str:
    """Escape a label value for the text format. Render-path only."""
    if "\\" in value:
        value = value.replace("\\", "\\\\")
    if '"' in value:
        value = value.replace('"', '\\"')
    if "\n" in value:
        value = value.replace("\n", "\\n")
    return value


def _label_set(names: Iterable[str], values: Iterable[str]) -> str:
    """Render `{a="1",b="2"}`, or `""` for no labels. Render-path only."""
    pairs = [f'{n}="{_escape(v)}"' for n, v in zip(names, values, strict=True)]
    return "{" + ",".join(pairs) + "}" if pairs else ""


def _number(value: float) -> str:
    """Shortest round-trip float form; ints stay ints so counters read clean."""
    if type(value) is int:
        return str(value)
    return repr(float(value))


# --- Histogram ----------------------------------------------------------------


class _Histogram:
    """Fixed-bucket histogram. Counts are kept per bucket (non-cumulative) and
    are summed at render time, so no work is spent maintaining cumulative state
    on the hot path.

    `edges[i]` is the inclusive `le` bound of bucket `i`, and slot `i` holds
    observations in `edges[i-1] <= v < edges[i]`. A `bisect_left` over `edges`
    lands on exactly that slot, so `v <= le` counts and `v > le` does not, in one
    call. The appended `inf` is the `+Inf` bucket: anything past the last real
    bound lands there rather than being clamped into a bucket that would claim
    to describe it.
    """

    __slots__ = ("name", "help", "edges", "buckets", "count", "total")

    def __init__(self, name: str, help_text: str, le_bounds: tuple[float, ...]) -> None:
        self.name = name
        self.help = help_text
        self.edges: tuple[float, ...] = tuple(float(b) for b in le_bounds) + (_INF,)
        self.buckets: list[int] = [0] * len(self.edges)
        self.count = 0
        self.total = 0.0

    def observe(self, value: float) -> None:
        index = bisect_left(self.edges, value)
        if index >= len(self.buckets):
            # Only reachable for value == inf; the +Inf bucket is the last slot.
            index = len(self.buckets) - 1
        self.buckets[index] += 1
        self.count += 1
        self.total += value


# --- Sequence window ----------------------------------------------------------


class _SequenceWindow:
    """Bounded, oldest-first-evicting map of `(source, user)` -> high-water
    sequence.

    Keeps the **maximum** seen, not the last: 5 then 4 must read as a
    violation, and a "last value wins" window would call that progress.
    """

    __slots__ = ("_lock", "_high", "_cap", "_evicted")

    def __init__(self, cap: int) -> None:
        if cap < 1:
            raise ValueError(f"sequence window must hold at least one pair, got {cap!r}")
        self._lock = threading.Lock()
        self._high: dict[tuple[str, str], int] = {}
        self._cap = cap
        self._evicted = 0

    def observe(self, source: str, user: str, sequence: int) -> int:
        """Returns `_OK` or `_VIOLATION`. Caller holds no other lock, and this is
        the only lock taken on the sequence path. An unparseable sequence is
        rejected by the caller before it gets here."""
        key = (source, user)
        with self._lock:
            high = self._high.get(key)
            if high is not None:
                if sequence < high:
                    return _VIOLATION
                if sequence == high:
                    return _OK
            # First sight, or a new high-water mark: both end in an insert, and
            # the cap has to be enforced on BOTH. Checking it only on the
            # update path is the obvious bug here -- a window that only ever saw
            # new pairs never evicts anything and is exactly the unbounded dict
            # this class exists to prevent.
            self._high[key] = sequence
            if len(self._high) > self._cap:
                self._evict()
            return _OK

    def _evict(self) -> None:
        """Drop oldest-first until back under the cap.

        `dict` is insertion-ordered, so the first key is the oldest. Re-inserting
        an existing key does NOT refresh its position, which is deliberate: a
        hot pair should not be able to pin its slot against a cold one forever,
        or a flood of one pair would evict every other pair and the window would
        track exactly one user. The trade (a long-lived pair can be evicted
        despite being hot) can only undercount violations.
        """
        high = self._high
        overflow = len(high) - self._cap
        for _ in range(overflow):
            try:
                del high[next(iter(high))]
            except (StopIteration, RuntimeError):  # pragma: no cover - defensive
                break
        self._evicted += overflow

    @property
    def evictions(self) -> int:
        return self._evicted

    def __len__(self) -> int:
        # len() of a dict is a single atomic load under the GIL, and this is a
        # read-only introspection used by tests and diagnostics, not a
        # read-modify-write.
        return len(self._high)


# --- Registry -----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Family:
    """One counter or gauge family: a name, a kind, and the label order its
    series keys are built in.

    `always` lists the series that must render even when nothing has touched
    them, so an idle worker still publishes zeros. Absent that, a metric that
    only appears once it is non-zero is indistinguishable from a broken one.
    `always=None` means "render exactly the keys present", which is what the
    genuinely keyed families (per type, per channel, per group) want.
    """

    name: str
    kind: str
    help: str
    label_names: tuple[str, ...]
    series: dict[tuple[str, ...], int | float] = field(default_factory=dict)
    always: tuple[tuple[str, ...], ...] = ()


class Metrics:
    """The gateway's metric registry: a process-local singleton in practice,
    one instance per worker process.

    Construct it once at app startup, hold it on `app.state`, and pass it down.
    Do not make it a module-level global: a process-global makes every test in
    the suite share counters, and the isolation is worth five extra characters
    of wiring.

    Bounded by construction (see the module docstring for the WHY):

    * every label is truncated at `label_cardinality` distinct values
    * the sequence window holds at most `sequence_window` pairs
    * `status_class` is a fixed six-value projection
    * no label is ever a tenant id, a user id or a raw status code

    Typical wiring of one request, in the order the events happen::

        metrics = Metrics()                      # once, at app startup
        metrics.set_in_flight_batches(n + 1)     # ... work ...
        metrics.observe_encryption_latency(t1 - t0)
        if limited:
            metrics.record_rate_limit_denial()
            metrics.record_response(429)
        else:
            metrics.record_batch_accepted(len(accepted))
            for ev in accepted:
                metrics.observe_sequence(ev.source, ev.user_id_pseudo, ev.sequence)
                metrics.record_event_accepted(ev.type, sourcechannel=ev.sourcechannel)
            for rej in rejected:
                metrics.record_validation_failure(rej.reason)
                metrics.record_event_rejected(type=None)
            metrics.record_response(202)
        metrics.set_in_flight_batches(n)         # ... done
        # the route: return Response(metrics.render(), media_type=CONTENT_TYPE)
    """

    def __init__(
        self,
        worker_id: str | None = None,
        *,
        label_cardinality: int = DEFAULT_LABEL_CARDINALITY,
        sequence_window: int = DEFAULT_SEQUENCE_WINDOW,
    ) -> None:
        if label_cardinality < 1:
            raise ValueError(f"label cardinality must be >= 1, got {label_cardinality!r}")
        self.worker_id = worker_id if worker_id is not None else default_worker_id()
        self.label_cardinality = label_cardinality
        self.sequence_window = sequence_window

        self._lock = threading.Lock()
        self._sequences = _SequenceWindow(sequence_window)

        #: raw label value -> canonical value, per label name. Bounded at
        #: `label_cardinality` entries by `_tracked`; overflow is NOT
        #: negative-cached, because a negative cache is itself an unbounded dict
        #: and a random label value is exactly how an attacker would fill it.
        self._canon: dict[str, dict[str, str]] = {
            name: {} for name in ("type", "sourcechannel", "reason", "status_class", "group", "label")
        }
        self._tracked: dict[str, int] = {name: 0 for name in self._canon}
        # Direct references to the two hottest caches. `self._canon["type"]` is a
        # dict lookup on a string key on every event; an attribute load is not.
        self._canon_type = self._canon["type"]
        self._canon_channel = self._canon["sourcechannel"]

        ns = METRICS_NAMESPACE
        # Counter series. The per-worker label is last in every key, and
        # `self.worker_id` is a fixed string, so it costs one tuple slot and no
        # dictionary lookup.
        self._ev_accepted: dict[tuple[str, ...], int] = {}
        self._ev_rejected: dict[tuple[str, ...], int] = {}
        self._batches_accepted: dict[tuple[str, ...], int] = {}
        self._batches_rejected: dict[tuple[str, ...], int] = {}
        self._validation_failures: dict[tuple[str, ...], int] = {}
        self._rate_limit_denials: dict[tuple[str, ...], int] = {}
        self._dlq_published: dict[tuple[str, ...], int] = {}
        self._decryptions: dict[tuple[str, ...], int] = {}
        self._ordering_violations: dict[tuple[str, ...], int] = {}
        self._ordering_unchecked: dict[tuple[str, ...], int] = {}
        self._http_responses: dict[tuple[str, ...], int] = {}
        self._label_dropped: dict[tuple[str, ...], int] = {}
        # Gauge series.
        self._dlq_depth: dict[tuple[str, ...], int] = {}
        self._in_flight: dict[tuple[str, ...], int] = {}
        self._buffer_items: dict[tuple[str, ...], int] = {}
        self._buffer_items_max: dict[tuple[str, ...], int] = {}
        self._buffer_utilisation: dict[tuple[str, ...], float] = {}
        self._consumer_lag: dict[tuple[str, ...], int] = {}
        self._worker_info: dict[tuple[str, ...], int] = {}

        self._histograms = {
            "batch_size": _Histogram(
                f"{ns}_batch_size_events",
                "Events per accepted batch, by contract limit MAX_EVENTS_PER_BATCH=500.",
                BATCH_SIZE_BUCKETS,
            ),
            "encryption": _Histogram(
                f"{ns}_encryption_latency_seconds",
                "Seconds to encrypt one event (AES-GCM over the PII fields).",
                ENCRYPTION_LATENCY_BUCKETS,
            ),
            "kafka_produce": _Histogram(
                f"{ns}_kafka_produce_latency_seconds",
                "Seconds from produce() to the delivery callback, including retries.",
                KAFKA_PRODUCE_LATENCY_BUCKETS,
            ),
            "request": _Histogram(
                f"{ns}_request_latency_seconds",
                "Seconds for one whole HTTP ingest request, handler in and out.",
                REQUEST_LATENCY_BUCKETS,
            ),
        }

        self._h_batch_size = self._histograms["batch_size"]
        self._h_encryption = self._histograms["encryption"]
        self._h_kafka_produce = self._histograms["kafka_produce"]
        self._h_request = self._histograms["request"]

        # Render table, built once. Points at the same dicts the hot path
        # mutates, so it cannot drift out of sync with them. `w` is the
        # always-rendered key for a worker-only family: one series per worker,
        # emitted at 0 whether or not anything has happened yet.
        w = ("worker",)
        self._worker_key = (self.worker_id,)
        self._families: tuple[_Family, ...] = (
            _Family(
                f"{ns}_events_accepted_total",
                "counter",
                "Events accepted by the gateway, by CloudEvents type and source channel.",
                ("type", "sourcechannel", "worker"),
                self._ev_accepted,
            ),
            _Family(
                f"{ns}_events_rejected_total",
                "counter",
                "Events rejected, by CloudEvents type and source channel. Validation "
                "reason codes are in the *_validation_failures_total family.",
                ("type", "sourcechannel", "worker"),
                self._ev_rejected,
            ),
            _Family(
                f"{ns}_batches_accepted_total",
                "counter",
                "Batches accepted (at least one event survived validation).",
                w,
                self._batches_accepted,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_batches_rejected_total",
                "counter",
                "Batches rejected whole (bad envelope, 413, mixed tenant).",
                w,
                self._batches_rejected,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_validation_failures_total",
                "counter",
                "Per-event validation failures by reason code. Codes, never messages: a "
                "message would carry the offending value into a second topic.",
                ("reason", "worker"),
                self._validation_failures,
            ),
            _Family(
                f"{ns}_rate_limit_denials_total",
                "counter",
                "Events denied by the per-tenant token bucket (HTTP 429). Deliberately "
                "unlabelled by tenant: 500 series per worker for a signal that is "
                "already per-request in the logs.",
                w,
                self._rate_limit_denials,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_dlq_published_total",
                "counter",
                "Events published to the DLQ topic.",
                w,
                self._dlq_published,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_decryptions_total",
                "counter",
                "Decryption operations performed (DLQ re-injection and admin tooling).",
                w,
                self._decryptions,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_ordering_violations_total",
                "counter",
                "Events whose sequence was lower than one already seen for the same "
                "(source, user) pair. The sticky-routing guard: it must read 0 with "
                "routing on and rise with it off.",
                w,
                self._ordering_violations,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_ordering_unchecked_total",
                "counter",
                "Events whose sequence could not be compared (absent or non-numeric). "
                "A zero violation count is only evidence when this is small.",
                w,
                self._ordering_unchecked,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_http_responses_total",
                "counter",
                "HTTP responses by status class. Bucketed, never the raw status: a "
                "client that can pick its status code must not be able to pick a series.",
                ("status_class", "worker"),
                self._http_responses,
            ),
            _Family(
                f"{ns}_label_values_dropped_total",
                "counter",
                "Observations folded into the __other__ label value by the cardinality "
                "cap. Non-zero means the exposition is deliberately blind past this point.",
                ("label",),
                self._label_dropped,
            ),
            _Family(
                SEQUENCE_WINDOW_EVICTIONS,
                "counter",
                "(source, user) pairs evicted from the ordering window. Each eviction "
                "can hide a later violation, so this bounds how blind the counter is.",
                w,
                {("evictions",): 0},
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_dlq_depth",
                "gauge",
                "Events currently sitting in the DLQ, reported by the reaper that "
                "drains it. The gateway publishes to the DLQ but does not size it.",
                w,
                self._dlq_depth,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_in_flight_batches",
                "gauge",
                "Batches currently being processed. Rises and falls; a value that only "
                "rises is a worker that stopped finishing work.",
                w,
                self._in_flight,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_buffer_items",
                "gauge",
                "Items currently in the producer's queue buffer.",
                w,
                self._buffer_items,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_buffer_items_max",
                "gauge",
                "Capacity of the producer's queue buffer (config.PRODUCER_QUEUE_MAXSIZE).",
                w,
                self._buffer_items_max,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_buffer_utilisation",
                "gauge",
                "Un-acked records divided by gateway_buffer_items_max. Can exceed 1: "
                "the numerator counts records anywhere in the un-acked set, including "
                "ones librdkafka has already taken off the Python queue, while the "
                "denominator is only the Python queue's capacity. 1 is therefore not "
                "the 503 point -- a full Python queue refuses a sink() with 503 while "
                "this still reads under 1. Watch it rising toward and past 1 as the "
                "trend; alert on gateway_buffer_items against its max instead.",
                w,
                self._buffer_utilisation,
                always=(self._worker_key,),
            ),
            _Family(
                f"{ns}_consumer_lag",
                "gauge",
                "Consumer lag, set by whoever polls the consumer group. The gateway "
                "produces and does not consume, so this is an input, never a poll.",
                ("group", "worker"),
                self._consumer_lag,
            ),
            _Family(
                f"{ns}_worker_info",
                "gauge",
                "Always 1. Exists so a worker with no traffic is still visible: a "
                "worker that is absent from a scrape is indistinguishable from a "
                "worker that is not running.",
                w,
                self._worker_info,
                always=(self._worker_key,),
            ),
        )
        self._eviction_family = next(f for f in self._families if f.name == SEQUENCE_WINDOW_EVICTIONS)
        self._worker_info[self._worker_key] = 1
        # An absent lag is a real, actionable value, not a missing series.
        self._consumer_lag[("default", self.worker_id)] = 0
        # Same reasoning for truncation: "the cap never fired" must read as an
        # explicit 0 per label, not as a missing series. The domain here is our
        # own constant label-name tuple, so seeding it costs six series and
        # cannot grow.
        for label_name in self._canon:
            self._label_dropped[(label_name,)] = 0

    # --- label cardinality ---------------------------------------------------

    def _label(self, name: str, value: str | None) -> str:
        """Canonical value for one label, collapsing past the cap.

        The general form, for the cold paths. The two hot recorders inline the
        fast branch and call `_label_miss` for the rest, because a method call
        per label per event is a measurable fraction of a 50k/s budget.

        Caller MUST hold `self._lock` (the miss path mutates the drop counter).
        """
        if value is None:
            return UNSET_LABEL
        canonical = self._canon[name].get(value)
        if canonical is not None:
            return canonical
        return self._label_miss(name, value)

    def _label_miss(self, name: str, value: str) -> str:
        """First sight of a label value: track it, or fold it into `__other__`.

        Runs at most `label_cardinality` times per label name for the tracking
        branch; the fold branch runs once per dropped observation, which is
        bounded by the traffic and never grows any structure.

        Caller MUST hold `self._lock`.
        """
        canon = self._canon[name]
        if self._tracked[name] < self.label_cardinality:
            canon[value] = value
            self._tracked[name] += 1
            return value
        # Deliberately NOT written back to `canon`. A negative cache would let a
        # stream of distinct label values grow a dict without bound, which is
        # the leak this cap exists to prevent -- and a random label value is
        # exactly how an attacker would fill it.
        dropped = (name,)
        self._label_dropped[dropped] = self._label_dropped.get(dropped, 0) + 1
        return OTHER_LABEL

    # --- counters (hot path) -------------------------------------------------

    def record_event_accepted(
        self, type: str | None = None, *, sourcechannel: str | None = None
    ) -> None:
        """One event survived validation and was handed to the producer."""
        lock = self._lock
        lock.acquire()
        try:
            # Label fast path, inlined. `_label()` is a method call and this runs
            # 50k times a second; the `or` is safe because a tracked value is
            # always a non-empty string and `__other__` is never stored back in
            # the cache, so a dropped value re-takes the slow path every time.
            canon_type = self._canon_type
            canon_channel = self._canon_channel
            canonical_type = (
                UNSET_LABEL if type is None else canon_type.get(type) or self._label_miss("type", type)
            )
            canonical_channel = (
                UNSET_LABEL
                if sourcechannel is None
                else canon_channel.get(sourcechannel) or self._label_miss("sourcechannel", sourcechannel)
            )
            key = (canonical_type, canonical_channel, self.worker_id)
            series = self._ev_accepted
            series[key] = series.get(key, 0) + 1
        finally:
            lock.release()

    def record_event_rejected(
        self, type: str | None = None, *, sourcechannel: str | None = None
    ) -> None:
        """One event was dropped before the producer."""
        lock = self._lock
        lock.acquire()
        try:
            canonical_type = (
                UNSET_LABEL
                if type is None
                else self._canon_type.get(type) or self._label_miss("type", type)
            )
            canonical_channel = (
                UNSET_LABEL
                if sourcechannel is None
                else self._canon_channel.get(sourcechannel)
                or self._label_miss("sourcechannel", sourcechannel)
            )
            key = (canonical_type, canonical_channel, self.worker_id)
            series = self._ev_rejected
            series[key] = series.get(key, 0) + 1
        finally:
            lock.release()

    def record_batch_accepted(self, size: int) -> None:
        """One batch accepted, `size` events in it.

        Records the batch counter *and* the batch-size histogram in one call, so
        the ingest path cannot count a batch while forgetting its size. The
        alternative is two calls and a silently empty histogram, which reads as
        a gateway bug rather than a wiring bug.
        """
        key = self._worker_key
        lock = self._lock
        lock.acquire()
        try:
            series = self._batches_accepted
            series[key] = series.get(key, 0) + 1
            self._h_batch_size.observe(size)
        finally:
            lock.release()

    def record_batch_rejected(self) -> None:
        """One batch rejected whole.

        Does NOT feed the batch-size histogram: that histogram is the size
        distribution of work we *accepted*, and folding rejected batches in
        would make the accepted size look smaller than it is.
        """
        key = self._worker_key
        lock = self._lock
        lock.acquire()
        try:
            series = self._batches_rejected
            series[key] = series.get(key, 0) + 1
        finally:
            lock.release()

    def record_validation_failure(self, reason: str) -> None:
        """One per-event validation failure, by reason code.

        `reason` is a code (`SCHEMA`, `OVERSIZED`, ...) from
        `app.validate.events`, never a message.
        """
        lock = self._lock
        lock.acquire()
        try:
            # Inside the lock: `_label` bumps the drop counter on the cap path.
            key = (self._label("reason", reason), self.worker_id)
            series = self._validation_failures
            series[key] = series.get(key, 0) + 1
        finally:
            lock.release()

    def record_rate_limit_denial(self) -> None:
        """One event denied by the token bucket (the caller returns 429)."""
        key = self._worker_key
        lock = self._lock
        lock.acquire()
        try:
            series = self._rate_limit_denials
            series[key] = series.get(key, 0) + 1
        finally:
            lock.release()

    def record_dlq_published(self, count: int = 1) -> None:
        """`count` events written to the DLQ topic."""
        key = self._worker_key
        lock = self._lock
        lock.acquire()
        try:
            series = self._dlq_published
            series[key] = series.get(key, 0) + count
        finally:
            lock.release()

    def record_decryption(self, count: int = 1) -> None:
        """`count` decryption operations performed."""
        key = self._worker_key
        lock = self._lock
        lock.acquire()
        try:
            series = self._decryptions
            series[key] = series.get(key, 0) + count
        finally:
            lock.release()

    def record_response(self, status: int) -> None:
        """One HTTP response, bucketed by status class. This is the 4xx/5xx rate."""
        key = (_status_class(status), self.worker_id)
        lock = self._lock
        lock.acquire()
        try:
            series = self._http_responses
            series[key] = series.get(key, 0) + 1
        finally:
            lock.release()

    def observe_sequence(self, source: str, user: str, sequence: str | int | None) -> bool:
        """Compare `sequence` against the high-water mark for `(source, user)`.

        Returns True when this event arrived out of order, i.e. its sequence is
        lower than one already seen for the pair. That is the observable the
        sticky-routing guarantee is asserted on (plan D12, T9 C1).

        `sequence` is a string in the CloudEvents contract, so it is parsed to an
        int: a lexical compare would call "10" older than "9" and report a
        violation on every correctly-ordered event, which is worse than no
        counter because it trains everyone to ignore it. `user` is already the
        pseudonymous id — a raw one never reaches this layer.

        An absent or non-numeric sequence is counted in
        `gateway_ordering_unchecked_total` and is NOT a violation.

        Tracking state is bounded and evicting: see `tracked_sequence_pairs`.

        The in-order path -- the overwhelming majority -- takes only the sequence
        window's lock and never the registry's, so a correctly-ordered event
        does not contend with the counters at all. The registry lock is taken
        only to bump one of the two exception counters below.
        """
        try:
            parsed = _as_sequence(sequence)
        except (TypeError, ValueError):
            # Untrackable, not a violation. Counted separately so that "the
            # violation counter reads zero" is evidence rather than an artefact
            # of a driver that never sent a comparable sequence.
            lock = self._lock
            lock.acquire()
            try:
                unchecked = self._ordering_unchecked
                key = self._worker_key
                unchecked[key] = unchecked.get(key, 0) + 1
            finally:
                lock.release()
            return False

        outcome = self._sequences.observe(source, user, parsed)
        if outcome == _OK:
            return False
        lock = self._lock
        lock.acquire()
        try:
            violations = self._ordering_violations
            key = self._worker_key
            violations[key] = violations.get(key, 0) + 1
        finally:
            lock.release()
        return outcome == _VIOLATION

    @property
    def tracked_sequence_pairs(self) -> int:
        """How many `(source, user)` pairs are currently tracked.

        Bounded by `sequence_window`; this is the number to watch for a leak.
        """
        return len(self._sequences)

    @property
    def sequence_evictions(self) -> int:
        return self._sequences.evictions

    # --- gauges --------------------------------------------------------------

    def set_dlq_depth(self, depth: int) -> None:
        """How many events are in the DLQ right now. Goes up and down: the
        reaper drains it."""
        with self._lock:
            self._dlq_depth[(self.worker_id,)] = depth

    def set_in_flight_batches(self, count: int) -> None:
        """Batches currently being processed by this worker."""
        with self._lock:
            self._in_flight[(self.worker_id,)] = count

    def set_buffer_usage(self, used: int, capacity: int) -> None:
        """Producer queue buffer occupancy, and its derived utilisation.

        All three are set from one call so they cannot disagree within a
        scrape: a `utilisation` computed from a different read than the
        `used` it implies is worse than no utilisation at all.
        """
        with self._lock:
            key = (self.worker_id,)
            self._buffer_items[key] = used
            self._buffer_items_max[key] = capacity
            self._buffer_utilisation[key] = (used / capacity) if capacity > 0 else 0.0

    def set_consumer_lag(self, lag: int, *, group: str = "default") -> None:
        """Externally-reported consumer lag for `group`, in messages.

        An input, not a poll: the gateway produces and never consumes, so
        whoever reads the consumer group (the driver, the verifier, a small
        exporter) pushes the number here and the exposition carries it.
        """
        with self._lock:
            self._consumer_lag[(self._label("group", group), self.worker_id)] = lag

    # --- histograms (hot path) -----------------------------------------------

    def observe_batch_size(self, size: int) -> None:
        """Events in one batch, without counting a batch. `record_batch_accepted`
        is the normal entry point; this exists for the caller that already
        counted the batch itself."""
        lock = self._lock
        lock.acquire()
        try:
            self._h_batch_size.observe(size)
        finally:
            lock.release()

    def observe_encryption_latency(self, seconds: float) -> None:
        lock = self._lock
        lock.acquire()
        try:
            self._h_encryption.observe(seconds)
        finally:
            lock.release()

    def observe_kafka_produce_latency(self, seconds: float) -> None:
        lock = self._lock
        lock.acquire()
        try:
            self._h_kafka_produce.observe(seconds)
        finally:
            lock.release()

    def observe_request_latency(self, seconds: float) -> None:
        lock = self._lock
        lock.acquire()
        try:
            self._h_request.observe(seconds)
        finally:
            lock.release()

    # --- render (cold path) --------------------------------------------------

    def render(self) -> str:
        """The whole registry as Prometheus text exposition format 0.0.4.

        Takes the lock once for the whole render, so a scrape can never observe
        a half-updated snapshot (a counter incremented and its histogram not).
        Every series is emitted even at zero: a metric that only appears once it
        is non-zero is indistinguishable from a metric that is broken, and on a
        quiet worker the first of those is the more common accident.
        """
        with self._lock:
            self._eviction_family.series[self._worker_key] = self._sequences.evictions
            out: list[str] = []
            for family in self._families:
                out.append(f"# HELP {family.name} {family.help}")
                out.append(f"# TYPE {family.name} {family.kind}")
                series = family.series
                if family.always:
                    keys: Iterable[tuple[str, ...]] = family.always
                else:
                    # Sorted so two scrapes of an identical state are byte
                    # identical, which is what makes the output diffable.
                    keys = sorted(series)
                for key in keys:
                    labels = _label_set(family.label_names, key)
                    out.append(f"{family.name}{labels} {_number(series.get(key, 0))}")

            for histogram in self._histograms.values():
                out.append(f"# HELP {histogram.name} {histogram.help}")
                out.append(f"# TYPE {histogram.name} histogram")
                cumulative = 0
                for edge, bucket in zip(histogram.edges, histogram.buckets, strict=True):
                    cumulative += bucket
                    le = "+Inf" if edge == _INF else _number(edge)
                    out.append(f'{histogram.name}_bucket{{le="{le}"}} {cumulative}')
                out.append(f"{histogram.name}_sum {_number(histogram.total)}")
                out.append(f"{histogram.name}_count {histogram.count}")

            return "\n".join(out) + "\n"

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"Metrics(worker_id={self.worker_id!r}, label_cardinality={self.label_cardinality!r}, "
            f"sequence_window={self.sequence_window!r}, tracked_pairs={len(self._sequences)})"
        )


def _as_sequence(value: str | int | None) -> int:
    """Coerce a contract `sequence` to an int, or raise.

    `sequence` is `str | None` in the CloudEvents struct, so a driver sending an
    int is the normal case and a string is the schema-conformant one. Both must
    compare numerically. Non-numeric values raise and the caller counts them as
    unchecked: silently treating them as 0 would make every later event for that
    pair look like a violation.
    """
    if type(value) is int:
        return value
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"sequence must be an int or a numeric string, got {value!r}")
