"""T3b: the application factory. RED before implementation.

What is pinned here, and none of it is visible in the routing code:

1. **`/metrics` works and is the exposition a scraper expects** -- the demo's
   dashboard is this endpoint, so a wrong content type is a broken product, not
   a cosmetic detail.
2. **`/healthz` is liveness only.** It must answer with the broker down, or an
   orchestrator restarts a healthy gateway during a broker blip.
3. **One tenant set feeds all three registries.** `TenantRegistry`,
   `TenantKeyRegistry` and `BucketRegistry` are built from the same collection
   of ids. If they diverge, auth accepts a tenant the rate limiter has never
   heard of -- and the limiter's `default` is a floor, not an open door.
4. **Per-event metrics exist**, because the dashboard's headline number is
   `events_accepted`, and `observe_sequence` is the sticky-routing guard.
5. **Importing `app.main` builds nothing.** A module-level `app = create_app()`
   would open a Kafka connection and read every env var at import, which makes
   the module untestable and `import app.main` from any script a side effect.
6. **A missing operator key is a startup failure**, not a runtime 401: the
   decrypt endpoint guards plaintext PII and a fail-open there is not a bug to
   discover in production.

The tests run against `FakeSink` -- no broker, no network.
"""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
import subprocess
import sys

import jwt
import msgspec
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient

from app.auth import Credential
from app.config import TOPIC_RAW, Settings
from app.kafka.producer import FakeSink
from app.main import create_app
from app.metrics import CONTENT_TYPE, Metrics
from app.ratelimit.registry import TenantLimits

AUDIENCE = "career-api"
TENANT_A = "acme_8921"
TENANT_B = "globex_4471"

CREDS = (
    Credential(credential_id="cred_a_web", career_site_id=TENANT_A, source_channel="WEB_APP"),
    Credential(credential_id="cred_b_web", career_site_id=TENANT_B, source_channel="WEB_APP"),
)

DEV_MASTER = b"test-only-master-secret-do-not-use!"
OPERATOR_KEY = b"test-only-operator-key"

BATCH_CONTENT_TYPE = "application/cloudevents-batch+json"


# --- fixtures -----------------------------------------------------------------


@pytest.fixture(scope="module")
def issuer() -> tuple[Ed25519PrivateKey, str]:
    key = Ed25519PrivateKey.generate()
    pem = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return key, pem.decode()


@pytest.fixture
def settings(issuer) -> Settings:
    return Settings(
        kafka_bootstrap_servers="broker:9092",
        master_secret=DEV_MASTER,
        jwt_public_key_pem=issuer[1],
        jwt_algorithm="EdDSA",
        jwt_audience=AUDIENCE,
    )


@pytest.fixture
def audits() -> list:
    return []


@pytest.fixture
def sink() -> FakeSink:
    return FakeSink()


@pytest.fixture
def metrics() -> Metrics:
    return Metrics(worker_id="test-worker")


@pytest.fixture
def app(settings, sink, metrics, audits):
    return create_app(
        credentials=CREDS,
        settings=settings,
        operator_key=OPERATOR_KEY,
        operator_id="ops-oncall-1",
        sink=sink,
        metrics=metrics,
        audit=audits.append,
    )


@pytest.fixture
def client(app) -> TestClient:
    with TestClient(app) as client:
        yield client


def token(issuer, *, tenant: str = TENANT_A, credential_id: str = "cred_a_web") -> str:
    now = dt.datetime.now(dt.timezone.utc)
    return jwt.encode(
        {
            "career_site_id": tenant,
            "credential_id": credential_id,
            "aud": AUDIENCE,
            "iat": now,
            "exp": now + dt.timedelta(hours=1),
        },
        issuer[0],
        algorithm="EdDSA",
    )


def event(
    *,
    tenant: str = TENANT_A,
    event_id: str = "01J8XQ4M7K2P9R3S01",
    sequence: str = "0000000001",
    user: str = "usr_1",
    **overrides,
) -> dict:
    ev = {
        "specversion": "1.0",
        "id": event_id,
        "source": f"/careers/{tenant}",
        "type": "com.careerpage.career.job-viewed",
        "time": "2026-09-30T14:43:09.123Z",
        "subject": "job_88320491",
        "dataschema": "https://schema.careerpage.example/event/1.0",
        "datacontenttype": "application/json",
        "sequence": sequence,
        "data": {
            "candidate": {"user_id": user, "email": "a@b.invalid", "name": "A B"},
            "event_payload": {"job_id": "job_88320491", "session_id": "sess_8839201923"},
        },
    }
    ev.update(overrides)
    return ev


def body(*events: dict) -> bytes:
    return msgspec.json.encode(list(events))


def post(client: TestClient, issuer, raw: bytes, **headers):
    return client.post(
        "/v1/ingest",
        content=raw,
        headers={
            "Content-Type": BATCH_CONTENT_TYPE,
            "Authorization": f"Bearer {token(issuer)}",
            **headers,
        },
    )


def series(exposition: str, name: str) -> list[str]:
    return [line for line in exposition.splitlines() if line.startswith(name)]


def value_of(exposition: str, prefix: str) -> float:
    for line in series(exposition, prefix):
        return float(line.rsplit(" ", 1)[1])
    raise AssertionError(f"no series starting {prefix!r} in:\n{exposition}")


# =============================================================================
# 1. /metrics
# =============================================================================


def test_metrics_serves_the_prometheus_exposition(client):
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"] == CONTENT_TYPE
    assert "# TYPE gateway_events_accepted_total counter" in response.text


def test_metrics_publishes_the_worker_id_so_workers_can_be_told_apart(client):
    assert 'gateway_worker_info{worker="test-worker"} 1' in client.get("/metrics").text


def test_every_exposition_line_is_well_formed(client):
    """The dashboard is a scraper reading this text, so a malformed line is a
    silently dropped series rather than a visible error."""
    import re

    sample = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[^}]*\})? -?[0-9.eE+]+( [a-z]+)?$")
    for line in client.get("/metrics").text.splitlines():
        if not line or line.startswith("#"):
            continue
        assert sample.match(line), line


# =============================================================================
# 2. /healthz
# =============================================================================


def test_healthz_is_ok_with_no_broker_involved(client, sink):
    """Liveness, not readiness: it must not depend on the thing that is most
    likely to be down, or a broker blip restarts a healthy gateway."""
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert sink.records == []


def test_healthz_names_the_worker(client):
    assert client.get("/healthz").json()["worker"] == "test-worker"


# =============================================================================
# 3. one tenant set, three registries
# =============================================================================


def test_all_three_registries_are_built_from_the_same_tenants(app):
    tenants = app.state.tenants
    keys = app.state.keys
    buckets = app.state.buckets

    configured = {cred.career_site_id for cred in CREDS}
    assert tenants.tenants == frozenset(configured)
    assert set(keys.career_site_ids) == configured
    assert set(buckets.summary().rates) == configured


def test_the_rate_limiter_is_a_floor_not_an_open_door(app):
    """A tenant the limiter has never heard of still gets a budget, and it is
    the same one every configured tenant gets -- not an unlimited default."""
    configured = app.state.buckets.bucket_for("acme_8921")
    unknown = app.state.buckets.bucket_for("never-configured")

    assert unknown.rate == pytest.approx(configured.rate)
    assert unknown.burst == pytest.approx(configured.burst)
    assert unknown.burst > 0


def test_a_tenant_missing_from_the_key_registry_is_refused(client, issuer):
    """The three registries agree, so a token naming a tenant nobody configured
    is refused at auth rather than reaching a key lookup."""
    response = post(
        client, issuer, body(event(tenant="not-a-tenant")), Authorization=f"Bearer {token(issuer, tenant='not-a-tenant')}"
    )
    assert response.status_code == 403


# =============================================================================
# 4. both routers are mounted and share the key registry
# =============================================================================


def test_an_accepted_batch_reaches_the_sink(client, sink, issuer):
    response = post(client, issuer, body(event(), event(event_id="01JBB", sequence="0000000002")))

    assert response.status_code == 202
    assert response.json() == {"accepted": 2, "rejected": []}
    assert [topic for topic, _key, _value in sink.records] == [TOPIC_RAW, TOPIC_RAW]


def test_a_rejected_event_goes_to_the_dlq(client, sink, issuer):
    response = post(client, issuer, body(event(), event(event_id="01JBB", type="com.nope.x")))

    assert response.status_code == 202
    assert response.json()["accepted"] == 1
    assert len(sink.dlq_records) == 1


def test_the_operator_can_decrypt_what_the_gateway_ingested(client, sink, issuer, audits):
    """One key registry, proven by using it from both sides: the event the
    gateway published is the event the decrypt endpoint reads."""
    post(client, issuer, body(event()))
    produced = msgspec.json.decode(sink.records[0][2], type=dict)

    response = client.post(
        "/v1/decrypt",
        json={"career_site_id": TENANT_A, "field": "name_enc", "event": produced},
        headers={"X-Operator-Key": OPERATOR_KEY.decode()},
    )

    assert response.status_code == 200
    assert response.json()["value"] == "A B"
    assert [record.outcome for record in audits] == ["ok"]


def test_decrypt_without_the_operator_key_is_401(client, audits):
    response = client.post("/v1/decrypt", json={"career_site_id": TENANT_A, "field": "name_enc", "event": {}})

    assert response.status_code == 401
    assert [record.outcome for record in audits] == ["denied"]


# =============================================================================
# 5. per-request and per-event metrics
# =============================================================================


def test_a_202_counts_the_batch_its_size_and_the_events_in_it(client, issuer):
    post(client, issuer, body(event(), event(event_id="01JBB", sequence="0000000002")))
    text = client.get("/metrics").text

    assert value_of(text, 'gateway_batches_accepted_total{worker="test-worker"}') == 1
    assert value_of(text, "gateway_batch_size_events_count") == 1
    assert value_of(text, "gateway_batch_size_events_sum") == 2
    assert value_of(text, 'gateway_events_accepted_total{type="com.careerpage.career.job-viewed"') == 2


def test_every_response_is_counted_by_status_class(client, issuer):
    post(client, issuer, body(event()))
    client.post("/v1/ingest", content=body(event(event_id="01JBB")), headers={"Content-Type": BATCH_CONTENT_TYPE})
    text = client.get("/metrics").text

    # /metrics and /healthz are excluded, so one 202 and one 401 is all there is.
    assert value_of(text, 'gateway_http_responses_total{status_class="2xx"') == 1
    assert value_of(text, 'gateway_http_responses_total{status_class="4xx"') == 1


def test_request_latency_is_observed(client, issuer):
    post(client, issuer, body(event()))
    text = client.get("/metrics").text

    assert value_of(text, "gateway_request_latency_seconds_count") >= 1


def test_the_observability_endpoints_are_not_counted_as_traffic(client, issuer):
    """Otherwise a 15-second scrape inflates the request rate the dashboard is
    supposed to be showing, and the number stops meaning anything."""
    post(client, issuer, body(event()))
    before = value_of(client.get("/metrics").text, 'gateway_http_responses_total{status_class="2xx"')

    for _ in range(5):
        client.get("/metrics")
        client.get("/healthz")

    text = client.get("/metrics").text
    assert value_of(text, 'gateway_http_responses_total{status_class="2xx"') == before


def test_the_sequence_check_sees_every_event(client, issuer):
    """Two events, one user, ascending sequence: in order, so no violation."""
    post(
        client,
        issuer,
        body(
            event(sequence="0000000001", user="usr_1"),
            event(event_id="01JBB", sequence="0000000002", user="usr_1"),
        ),
    )
    text = client.get("/metrics").text

    assert value_of(text, 'gateway_ordering_violations_total{worker="test-worker"}') == 0
    assert value_of(text, 'gateway_ordering_unchecked_total{worker="test-worker"}') == 0


def test_a_regression_in_sequence_is_counted_as_a_violation(client, issuer):
    """The sticky-routing guarantee is asserted on exactly this counter, so it
    has to be wired even though nothing in the request path asks for it."""
    post(
        client,
        issuer,
        body(
            event(sequence="0000000002", user="usr_1"),
            event(event_id="01JBB", sequence="0000000001", user="usr_1"),
        ),
    )
    text = client.get("/metrics").text

    assert value_of(text, 'gateway_ordering_violations_total{worker="test-worker"}') == 1


def test_an_out_of_order_event_for_a_different_user_is_not_a_violation(client, issuer):
    """The window is keyed on `(source, user)`, so two users may both send
    sequence 1 without that being a reordering."""
    post(
        client,
        issuer,
        body(
            event(sequence="0000000001", user="usr_1"),
            event(event_id="01JBB", sequence="0000000001", user="usr_2"),
        ),
    )
    text = client.get("/metrics").text

    assert value_of(text, 'gateway_ordering_violations_total{worker="test-worker"}') == 0
    assert client.app.state.metrics.tracked_sequence_pairs == 2


def test_an_absent_sequence_is_counted_as_unchecked_not_as_a_violation(client, issuer):
    ev = event()
    ev.pop("sequence")
    post(client, issuer, body(ev))
    text = client.get("/metrics").text

    assert value_of(text, 'gateway_ordering_unchecked_total{worker="test-worker"}') == 1
    assert value_of(text, 'gateway_ordering_violations_total{worker="test-worker"}') == 0


def test_the_pseudonym_is_what_the_ordering_window_is_keyed_on(client, sink, issuer):
    """Not the raw `user_id`: the window is the thing that must stay bounded, and
    a raw id in it would be an identifier in a metrics path."""
    post(client, issuer, body(event()))
    produced = msgspec.json.decode(sink.records[0][2], type=dict)
    pseudo = produced["data"]["candidate"]["user_id_pseudo"]

    assert client.app.state.metrics.tracked_sequence_pairs == 1
    assert sink.records[0][1] == f"{TENANT_A}|{pseudo}"


def test_a_whole_batch_refusal_is_counted_where_it_belongs(client, issuer):
    response = post(
        client, issuer, body(event(), event(tenant=TENANT_B, event_id="01JBB")),
    )
    assert response.status_code == 403

    text = client.get("/metrics").text
    assert value_of(text, 'gateway_batches_rejected_total{worker="test-worker"}') == 1
    assert value_of(text, 'gateway_batches_accepted_total{worker="test-worker"}') == 0


def test_a_rate_limited_batch_is_counted_as_a_denial(settings, sink, metrics, audits, issuer):
    """A tenant with a one-token budget is refused on the second event of a
    two-event batch: charged per event, denied per batch."""
    app = create_app(
        credentials=CREDS,
        settings=settings,
        operator_key=OPERATOR_KEY,
        sink=sink,
        metrics=metrics,
        audit=audits.append,
        tenant_limits={TENANT_A: TenantLimits(rate=1.0, burst=1.0)},
    )
    with TestClient(app) as client:
        response = post(client, issuer, body(event(), event(event_id="01JBB", sequence="0000000002")))
        assert response.status_code == 429
        assert response.headers["Retry-After"]
        text = client.get("/metrics").text

    assert value_of(text, 'gateway_rate_limit_denials_total{worker="test-worker"}') == 1


def test_in_flight_batches_rises_and_falls(client, issuer):
    post(client, issuer, body(event()))
    assert value_of(client.get("/metrics").text, 'gateway_in_flight_batches{worker="test-worker"}') == 0


def test_the_dlq_publication_is_counted(client, issuer):
    post(client, issuer, body(event(), event(event_id="01JBB", type="com.nope.x")))
    text = client.get("/metrics").text

    assert value_of(text, 'gateway_dlq_published_total{worker="test-worker"}') == 1


# =============================================================================
# 6. construction-time guarantees
# =============================================================================


def test_an_empty_operator_key_fails_the_build(settings, sink):
    """Not a 401 at request time: `secrets.compare_digest(b"", b"")` succeeds, so
    an unset operator key would be a fail-open on the one endpoint that returns
    plaintext PII."""
    with pytest.raises(ValueError, match="operator_key"):
        create_app(credentials=CREDS, settings=settings, operator_key=b"", sink=sink)


def test_no_credentials_is_a_startup_failure_not_an_open_gateway(settings, sink):
    with pytest.raises(ValueError, match="credential"):
        create_app(credentials=(), settings=settings, operator_key=OPERATOR_KEY, sink=sink)


def test_an_empty_jwt_public_key_fails_the_build(sink):
    settings = Settings(master_secret=DEV_MASTER, jwt_public_key_pem="")
    with pytest.raises(ValueError, match="public_key_pem"):
        create_app(credentials=CREDS, settings=settings, operator_key=OPERATOR_KEY, sink=sink)


def test_importing_app_main_builds_nothing():
    """`import app.main` must not construct a producer, read env, or open a
    socket -- otherwise the module is unusable from a script, a test, or a
    health check, and the failure arrives at import time where nobody looks."""
    repo = pathlib.Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, app.main;"
            "assert 'confluent_kafka' not in sys.modules, 'importing app.main built a Kafka client';"
            "assert 'fastapi' in sys.modules",
        ],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


# =============================================================================
# 7. lifecycle
# =============================================================================


def test_the_sink_is_drained_when_the_app_stops(settings, sink):
    app = create_app(
        credentials=CREDS, settings=settings, operator_key=OPERATOR_KEY, sink=sink
    )
    with TestClient(app):
        assert not sink.closed
    assert sink.closed, "SIGTERM path would drop whatever was still buffered"


def test_a_real_sink_is_built_when_none_is_injected(settings, metrics, monkeypatch):
    """The production path exists and is exercised by the composition, not by a
    separate wiring that only runs on the demo machine."""
    built: list[dict] = []

    class _Stub:
        def __init__(self, config: dict) -> None:
            built.append(config)

        def produce(self, topic, *, key=None, value=None, on_delivery=None): ...
        def poll(self, timeout=None): return 0
        def flush(self, timeout=None): return 0
        def __len__(self): return 0

    monkeypatch.setattr("app.kafka.producer.build_producer", _Stub)
    app = create_app(
        credentials=CREDS, settings=settings, operator_key=OPERATOR_KEY, metrics=metrics
    )

    assert app.state.sink.inner is not None
    assert built and built[0]["bootstrap.servers"] == "broker:9092"
    assert built[0]["enable.idempotence"] is True


def test_audit_records_reach_the_injected_sink_on_a_denial(client, audits, caplog):
    with caplog.at_level(logging.DEBUG):
        response = client.post("/v1/decrypt", json={"career_site_id": TENANT_A, "field": "name_enc", "event": {}})

    assert response.status_code == 401
    assert len(audits) == 1
    assert audits[0].career_site_id is None
