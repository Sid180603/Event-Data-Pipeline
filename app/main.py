"""T3b: the application factory -- the composition root, and nothing else.

Every collaborator below is **injected**. This module owns no state, opens no
connection at import time, and contains no business logic: the pipeline's
decisions are made in `app.ingest`, the producer's in `app.kafka`, the counters'
in `app.metrics`. What lives here is the wiring, and there are three things in
it worth arguing for.

**One tenant set, three registries.** `TenantRegistry` (T4),
`TenantKeyRegistry` (T6) and `BucketRegistry` (T7) are each built from
`credentials`, once, below. They could each be handed their own list and would
each work perfectly in isolation -- which is the problem: a tenant that auth
accepts and the limiter has never heard of is charged the `default` budget, and
a tenant the limiter knows but has no key for is an exception on the hot path.
The invariant is cheap to hold and impossible to see from the code, so it is
also asserted in `app/test_app.py`.

**The sink is wrapped, not replaced.** `build_ingest_router` takes a sink and
nothing else that sees individual events, so per-event metrics
(`record_event_accepted`, `observe_sequence`) have to come from a wrapper
around it. The wrapper decodes a three-field projection of each produced record
rather than the whole CloudEvent: ~0.9 us against ~1.4 us, and it is 50,000
times a second.

**Nothing is constructed at import time.** There is deliberately no module-level
`app = create_app()`: uvicorn is pointed at `app.main:build_app_from_env
--factory`, so importing this module reads no environment variable, builds no
tenant keys and opens no socket. A test asserts that in a subprocess.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import msgspec
from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response

from app.auth import Credential, TenantRegistry, TokenParseBudget, TokenVerifier
from app.config import TOPIC_DLQ, Settings
from app.crypto.registry import TenantKeyRegistry
from app.ingest import (
    DecryptAudit,
    Sink,
    build_decrypt_router,
    build_ingest_router,
)
from app.kafka.producer import KafkaSink
from app.metrics import CONTENT_TYPE, Metrics
from app.ratelimit.bucket import Clock, TokenBucket
from app.ratelimit.registry import BucketRegistry, TenantLimits

if TYPE_CHECKING:  # pragma: no cover - typing only
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

log = logging.getLogger("app.main")

__all__ = [
    "DECRYPT_LIMIT",
    "DEFAULT_PARSE_BUDGET",
    "DEFAULT_TENANT_LIMIT",
    "build_app_from_env",
    "create_app",
    "credential_id_for",
    "credentials_from_env",
]

#: Per-tenant budget when the configuration does not say otherwise: 2,000
#: events/sec sustained with a burst of 4,000. Chosen against the 500-tenant
#: catalog and the 50k events/sec target, so a single tenant cannot consume the
#: gateway's whole budget. It is a floor for an unconfigured tenant, never an
#: unlimited one -- see `BucketRegistry`'s docstring.
DEFAULT_TENANT_LIMIT = TenantLimits(rate=2_000.0, burst=4_000.0)

#: The operator bucket is separate on purpose (`app/ingest/decrypt.py`): a tenant
#: must not be able to spend the operator's decryption budget, and decryption
#: is a key operation and a cheap oracle, so it is deliberately slow.
DECRYPT_LIMIT = TenantLimits(rate=10.0, burst=10.0)

#: Unauthenticated malformed tokens per process. Refusal is 401, not 429,
#: because a token we cannot read has no tenant to attribute a per-tenant limit
#: to.
DEFAULT_PARSE_BUDGET = 1_000

#: The two endpoints the request metrics cover. `/metrics` and `/healthz` are
#: deliberately excluded: a 15-second scrape would otherwise be a measurable
#: share of the request rate the dashboard is showing.
_INGEST_PATH = "/v1/ingest"
_DECRYPT_PATH = "/v1/decrypt"
_GATEWAY_PATHS = frozenset({_INGEST_PATH, _DECRYPT_PATH})

#: Statuses that mean "this whole batch was refused" as opposed to "some events
#: in it were". A 429 is not here: it is a load-shed signal with its own
#: counter, and folding it in would double-count it.
_REFUSED_BATCH_STATUSES = frozenset({400, 403, 413})


# --- per-event metric projection ---------------------------------------------


class _CandidateHint(msgspec.Struct):
    user_id_pseudo: str


class _DataHint(msgspec.Struct):
    candidate: _CandidateHint


class _EventHint(msgspec.Struct):
    """Four fields out of a published CloudEvent, for the metrics only.

    Not `CloudEvent` itself: this runs once per event on the request path, and
    decoding three scalar fields out of the JSON is measurably cheaper than
    materialising the whole envelope (including the ciphertexts, which are the
    most expensive part to skip past). Unknown fields are ignored rather than
    refused, which is what makes the projection safe against a contract that
    grows a field.
    """

    source: str
    type: str
    data: _DataHint
    sequence: str | None = None
    sourcechannel: str | None = None


_HINT_DECODER = msgspec.json.Decoder(type=_EventHint)


class _BatchResult(msgspec.Struct):
    """The `202` body, read once so the batch size can be counted.

    `rejected` is a list of `Raw` rather than decoded values: a 500-event batch
    with 500 rejections is a ~30 KB body, and materialising every rejection to
    read one integer out of the same line is work the hot path should not do for
    a metric.
    """

    accepted: int
    rejected: list[msgspec.Raw] = msgspec.field(default_factory=list)


class InstrumentedSink:
    """The gateway's `Sink`: delegates, then reports what was published.

    Counting *after* the delegate is deliberate. If the delegate raises
    `SinkUnavailable` the caller gets a `503` and will resend the same `id`s, so
    a record that never reached the sink must not appear in
    `gateway_events_accepted_total` -- a counter that counted work we then
    refused to accept is how a dashboard starts lying.
    """

    __slots__ = ("inner", "_dlq_topic", "_metrics")

    def __init__(self, inner: Sink, metrics: Metrics, *, dlq_topic: str = TOPIC_DLQ) -> None:
        self.inner = inner
        self._metrics = metrics
        self._dlq_topic = dlq_topic

    def sink(self, topic: str, key: str, value: bytes) -> None:
        self.inner.sink(topic, key, value)
        if topic == self._dlq_topic:
            # The pipeline routes the DLQ through `sink_dlq`; this branch exists so
            # a caller that names the topic directly is counted too, rather than
            # being a hole in the counter.
            self._metrics.record_dlq_published()
            return
        try:
            hint = _HINT_DECODER.decode(value)
        except msgspec.DecodeError:  # pragma: no cover - the gateway encoded it
            # Our own encoding, so this cannot happen without a bug. Logging and
            # carrying on is still right: the record is already in the sink, and
            # failing the request here would turn a metrics defect into a 500.
            log.error("produced record did not match the metric projection")
            return
        self._metrics.record_event_accepted(hint.type, sourcechannel=hint.sourcechannel)
        # The pseudonym, never the raw `user_id`: the ordering window is bounded
        # state keyed by this, and a raw identifier in it would be an identifier
        # in a metrics structure.
        self._metrics.observe_sequence(
            hint.source, hint.data.candidate.user_id_pseudo, hint.sequence
        )

    def sink_dlq(self, key: str, value: bytes) -> None:
        self.inner.sink_dlq(key, value)
        self._metrics.record_dlq_published()

    def close(self) -> None:
        self.inner.close()


# --- request metrics ----------------------------------------------------------


class RequestMetrics:
    """Pure-ASGI middleware that turns one request into five observations.

    ASGI rather than `@app.middleware("http")` for two concrete reasons: that
    wrapper costs a task group per request, and it hands back a streaming
    response, which would mean reassembling a body just to read `accepted` out
    of it -- and reassembling it would mean holding a second copy of a decrypt
    response, which is plaintext PII. Here the body is captured only after
    `http.response.start` has already said `202`, which only `/v1/ingest` returns.

    Status is what is counted, not a verdict: by the time middleware sees it, the
    decision was made by code that owns it.
    """

    def __init__(self, app: ASGIApp, metrics: Metrics, *, clock: Clock = time.monotonic) -> None:
        self.app = app
        self._metrics = metrics
        self._clock = clock
        self._lock = threading.Lock()
        self._in_flight = 0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path") not in _GATEWAY_PATHS:
            await self.app(scope, receive, send)
            return

        metrics = self._metrics
        started = self._clock()
        status = 500
        capture = False
        body = bytearray()

        self._enter()
        try:

            async def send_wrapper(message: Message) -> None:
                nonlocal status, capture
                if message["type"] == "http.response.start":
                    status = message["status"]
                    capture = status == 202
                elif capture and message["type"] == "http.response.body":
                    body.extend(message.get("body", b""))
                await send(message)

            await self.app(scope, receive, send_wrapper)
        finally:
            self._exit()
            elapsed = self._clock() - started
            metrics.record_response(status)
            metrics.observe_request_latency(elapsed)
            if scope.get("path") == _INGEST_PATH:
                _count_outcome(metrics, status, bytes(body))

    def _enter(self) -> None:
        # A lock because this is a read-modify-write on a shared int, and the
        # rest of `app.metrics` is written to that rule under free-threading too.
        lock = self._lock
        lock.acquire()
        try:
            self._in_flight += 1
            self._metrics.set_in_flight_batches(self._in_flight)
        finally:
            lock.release()

    def _exit(self) -> None:
        lock = self._lock
        lock.acquire()
        try:
            self._in_flight -= 1
            self._metrics.set_in_flight_batches(self._in_flight)
        finally:
            lock.release()


def _count_outcome(metrics: Metrics, status: int, body: bytes) -> None:
    """Fold one ingest response into the right counter."""
    if status == 202:
        try:
            result = msgspec.json.decode(body, type=_BatchResult)
        except msgspec.DecodeError:  # pragma: no cover - our own encoder
            return
        if result.accepted:
            # Counter and histogram in one call, so a batch cannot be counted
            # while its size is forgotten.
            metrics.record_batch_accepted(result.accepted)
        else:
            # A 202 that carried nothing publishable is still a batch, and 0 is
            # a bucket worth having: an empty batch is a bug worth alerting on.
            metrics.observe_batch_size(0)
        return
    if status == 429:
        metrics.record_rate_limit_denial()
    elif status in _REFUSED_BATCH_STATUSES:
        metrics.record_batch_rejected()


# --- the audit sink -----------------------------------------------------------


def log_audit(record: DecryptAudit) -> None:
    """Where a decrypt audit record goes when the app is not told otherwise.

    WHO and WHICH `(source, id)`, never the value: an audit trail that outlives
    the request is exactly the place a decrypted value must never land. A real
    deployment points `audit` at a durable log; this is the honest default, and
    it is the reason `audit` is a parameter at all.
    """
    log.info(
        "decrypt audit: actor=%s outcome=%s site=%s source=%s id=%s field=%s",
        record.actor,
        record.outcome,
        record.career_site_id,
        record.source,
        record.event_id,
        record.field,
    )


# --- the factory --------------------------------------------------------------


def create_app(
    *,
    credentials: Sequence[Credential],
    settings: Settings,
    operator_key: bytes,
    operator_id: str = "operator",
    sink: Sink | None = None,
    metrics: Metrics | None = None,
    tenant_limits: Mapping[str, TenantLimits] | None = None,
    default_limit: TenantLimits = DEFAULT_TENANT_LIMIT,
    decrypt_limit: TenantLimits = DECRYPT_LIMIT,
    parse_budget_limit: int = DEFAULT_PARSE_BUDGET,
    audit: Callable[[DecryptAudit], None] | None = None,
) -> FastAPI:
    """Assemble the gateway. Everything is a parameter; nothing is a global.

    `sink=None` builds a real `KafkaSink` from `settings.kafka_bootstrap_servers`
    -- at *call* time, which is what keeps `import app.main` inert. Every test in
    the suite passes a `FakeSink`, so the suite needs no broker.
    """
    if not operator_key:
        # `app.ingest.decrypt` refuses to build a router without this too, but a
        # gateway that starts and then 401s every operator is a different failure
        # to diagnose than one that refuses to start. Say which one it is.
        raise ValueError(
            "operator_key is empty: the gateway cannot audit decrypts without it "
            "(set OPERATOR_KEY; it is the X-Operator-Key credential, not a tenant JWT)"
        )
    if not credentials:
        raise ValueError(
            "no credentials configured: a gateway with no tenants must not start, "
            "because every request would be a 403"
        )

    # The one tenant set. Three registries, one source (see the module docstring).
    tenant_ids = frozenset(cred.career_site_id for cred in credentials)
    ordered_tenants = tuple(sorted(tenant_ids))

    metrics = metrics if metrics is not None else Metrics()
    tenants = TenantRegistry(credentials)
    # Built once per process: the public key is parsed and the algorithm is
    # pinned here, so neither happens per request. Fails at startup on an empty
    # or unusable key, which is a configuration error with an obvious message --
    # rather than a 401 on every request from a gateway that looks healthy.
    verifier = TokenVerifier(
        settings.jwt_public_key_pem, settings.jwt_algorithm, settings.jwt_audience
    )
    keys = TenantKeyRegistry(settings, ordered_tenants)
    limits = dict(tenant_limits or {})
    # A configured tenant always has an entry, even when `tenant_limits` omitted
    # it: the registry then reports every configured tenant, which is what the
    # sticky-routing precondition in `RegistrySummary` is checked against.
    for career_site_id in ordered_tenants:
        limits.setdefault(career_site_id, default_limit)
    buckets = BucketRegistry(limits, default=default_limit)

    if sink is None:
        sink = KafkaSink(
            bootstrap_servers=settings.kafka_bootstrap_servers, metrics=metrics
        )
    published = InstrumentedSink(sink, metrics)
    operator_bucket = TokenBucket(rate=decrypt_limit.rate, burst=decrypt_limit.burst)
    audit_sink = audit if audit is not None else log_audit

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        yield
        # SIGTERM lands here. The producer drains on its own thread, so the drain
        # is awaited rather than performed: the point is that the process stays
        # alive until the queue is empty, not that it blocks the loop doing it.
        await run_in_threadpool(published.close)

    app = FastAPI(title="career event gateway", lifespan=lifespan, docs_url=None, redoc_url=None)

    # On `app.state` because a scraper, a test, or an operator tool needs to read
    # the same instances the routes use. Holding them as closures instead would
    # make "what is this process actually enforcing?" unanswerable.
    app.state.metrics = metrics
    app.state.sink = published
    app.state.tenants = tenants
    app.state.keys = keys
    app.state.buckets = buckets
    app.state.verifier = verifier
    app.state.operator_bucket = operator_bucket

    app.include_router(
        build_ingest_router(
            verifier=verifier,
            tenants=tenants,
            keys=keys,
            buckets=buckets,
            sink=published,
            parse_budget=TokenParseBudget(parse_budget_limit),
        )
    )
    app.include_router(
        build_decrypt_router(
            keys=keys,
            operator_key=operator_key,
            operator_id=operator_id,
            bucket=operator_bucket,
            audit=audit_sink,
        )
    )

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint() -> Response:
        return Response(metrics.render(), media_type=CONTENT_TYPE)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> Response:
        # Liveness only. It does not ask the producer whether the broker is up:
        # a gateway that answers "no" when Kafka is down gets restarted by its
        # orchestrator, which is a much worse outage than a gateway that is
        # waiting for a broker.
        return JSONResponse({"status": "ok", "worker": metrics.worker_id})

    app.add_middleware(RequestMetrics, metrics=metrics)
    return app


# --- environment wiring -------------------------------------------------------


def credential_id_for(career_site_id: str, channel: str) -> str:
    """The credential a tenant's clients on `channel` sign with.

    Derived rather than configured, because it has to be derivable by the driver
    too: a token has to name a credential the gateway has registered, and two
    independently maintained tables of ids is how that pairing starts drifting.
    """
    return f"cred_{career_site_id}_{channel.lower()}"


def credentials_from_env(environ: Mapping[str, str] | None = None) -> tuple[Credential, ...]:
    """The tenant list, from configuration. One list, three registries.

    `GATEWAY_CREDENTIALS` (explicit `credential_id:career_site_id:CHANNEL`
    triples) wins when it is set, because that is the escape hatch for a driver
    that mints its own ids. Otherwise `GATEWAY_TENANTS` (a comma-separated list
    of ids) is expanded to one credential per source channel.
    """
    env = os.environ if environ is None else environ
    explicit = env.get("GATEWAY_CREDENTIALS", "").strip()
    if explicit:
        credentials = []
        for entry in explicit.split(","):
            parts = entry.strip().split(":")
            if len(parts) != 3:
                raise ValueError(
                    f"GATEWAY_CREDENTIALS entry {entry!r} must be "
                    "credential_id:career_site_id:CHANNEL"
                )
            credentials.append(
                Credential(
                    credential_id=parts[0], career_site_id=parts[1], source_channel=parts[2]
                )
            )
        return tuple(credentials)

    tenants = [entry.strip() for entry in env.get("GATEWAY_TENANTS", "").split(",") if entry.strip()]
    if not tenants:
        raise ValueError(
            "no tenants configured: set GATEWAY_TENANTS (comma-separated career_site_ids) "
            "or GATEWAY_CREDENTIALS (credential_id:career_site_id:CHANNEL)"
        )
    return tuple(
        Credential(
            credential_id=credential_id_for(career_site_id, channel),
            career_site_id=career_site_id,
            source_channel=channel,
        )
        for career_site_id in tenants
        for channel in ("WEB_APP", "MOBILE_APP", "THIRD_PARTY_SERVICE")
    )


def build_app_from_env() -> FastAPI:
    """Entry point for `uvicorn app.main:build_app_from_env --factory`.

    A function, not a module-level `app`, so importing this module has no
    effects; the process pays for construction once, here.
    """
    return create_app(
        credentials=credentials_from_env(),
        settings=Settings(),
        operator_key=os.getenv("OPERATOR_KEY", "").encode(),
        operator_id=os.getenv("OPERATOR_ID", "operator"),
    )
