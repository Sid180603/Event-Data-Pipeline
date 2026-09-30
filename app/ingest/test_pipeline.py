"""T5 ingest tests. RED before implementation.

Two things are being pinned here, and one of them is invisible in the code:

1. **The DLQ payload depends on how far an event got.** An event rejected during
   validation was never encrypted, so its DLQ record is the plaintext ingress
   payload -- acceptable *only* because it failed before encryption. An event
   that was encrypted and then failed the post-encryption size check must be
   DLQ'd in its ENCRYPTED form, or the DLQ becomes a second plaintext PII store
   and a replayed record gets double-encrypted. Both paths are tested with a
   canary, because they differ in exactly the way a value regex would miss.

2. **`202` is not a durability receipt** (C4). The handler returns 202 once the
   events are in the producer's in-memory buffer. Tests assert the code says so
   rather than the code being allowed to imply otherwise.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any

import jwt
import msgspec
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.auth import Credential, TenantRegistry, TokenParseBudget, TokenVerifier
from app.auth.errors import Forbidden, Unauthorized
from app.config import (
    MAX_BATCH_BYTES,
    MAX_EVENTS_PER_BATCH,
    MAX_EVENT_BYTES,
    Settings,
    TOPIC_RAW,
)
from app.crypto.facade import protect_candidate
from app.crypto.registry import TenantKeyRegistry
from app.dlq.envelope import DLQ_ERROR_TYPE, DlqEvent, dlq_data_to_ingress
from app.ingest import (
    DURABILITY_ACCEPTED_INTO_BUFFER,
    REASON_NOT_A_BATCH,
    IngestResult,
    NotABatch,
    PayloadTooLarge,
    RateLimited,
    SinkUnavailable,
    build_decrypt_router,
    build_ingest_router,
    ingest_batch,
)
from app.ingest.limits import check_declared_length
from app.ingest.pipeline import Sink
from app.pseudonym.hmac import pseudonymize
from app.ratelimit.registry import BucketRegistry, TenantLimits
from contracts.attributes import SAFE_CONTEXT_ATTRS, derive_kafka_key

# --- constants ---------------------------------------------------------------

AUDIENCE = "career-api"
TENANT_A = "acme_8921"
TENANT_B = "globex_4471"
SOURCE_A = f"/careers/{TENANT_A}"
SOURCE_B = f"/careers/{TENANT_B}"

CREDS = (
    Credential(credential_id="cred_a_web", career_site_id=TENANT_A, source_channel="WEB_APP"),
    Credential(credential_id="cred_b_web", career_site_id=TENANT_B, source_channel="WEB_APP"),
)

DEV_MASTER = b"test-only-master-secret-do-not-use!"

#: Canaries from plan M7. `@` cannot appear in the base64url alphabet, so a
#: literal substring search cannot miss an encoding of the email.
CANARY_EMAIL = "SENTINEL-8f3a@example.invalid"
CANARY_USER = "usr_CANARY_992182741"
CANARY_NAME = "SENTINEL-8f3a-name"
CANARY_PHONE = "SENTINEL-8f3a-phone"

OPERATOR_KEY = b"test-only-operator-key"
OPERATOR_ID = "ops-oncall-1"


# --- fixtures ----------------------------------------------------------------


@pytest.fixture(scope="module")
def issuer() -> tuple[Ed25519PrivateKey, str]:
    key = Ed25519PrivateKey.generate()
    pem = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return key, pem.decode()


@pytest.fixture
def verifier(issuer) -> TokenVerifier:
    return TokenVerifier(public_key_pem=issuer[1], algorithm="EdDSA", audience=AUDIENCE)


@pytest.fixture
def tenants() -> TenantRegistry:
    return TenantRegistry(CREDS)


@pytest.fixture
def keys() -> TenantKeyRegistry:
    settings = Settings(master_secret=DEV_MASTER, key_version=1)
    return TenantKeyRegistry(settings, (TENANT_A, TENANT_B))


@pytest.fixture
def clock() -> list[float]:
    """Injected monotonic clock, so refill is arithmetic rather than a sleep."""
    return [1000.0]


@pytest.fixture
def buckets(clock) -> BucketRegistry:
    """Generous burst, so the shared fixture is not itself a rate limit. The
    rate-limit tests build their own near-empty bucket."""
    generous = TenantLimits(rate=1.0, burst=10_000.0)
    return BucketRegistry(
        {TENANT_A: generous, TENANT_B: generous}, default=generous, clock=lambda: clock[0]
    )


@pytest.fixture
def sink() -> "RecordingSink":
    return RecordingSink()


@pytest.fixture
def parse_budget() -> TokenParseBudget:
    return TokenParseBudget(limit=100)


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


# --- fake sink ---------------------------------------------------------------


class RecordingSink:
    """In-memory stand-in for the Kafka producer (T5a owns the real one)."""

    def __init__(self, *, fail_raw: Exception | None = None, fail_dlq: Exception | None = None) -> None:
        self.sent: list[tuple[str, str, bytes]] = []
        self.dlq: list[tuple[str, bytes]] = []
        self.fail_raw = fail_raw
        self.fail_dlq = fail_dlq

    def sink(self, topic: str, key: str, value: bytes) -> None:
        if self.fail_raw is not None:
            raise self.fail_raw
        self.sent.append((topic, key, value))

    def sink_dlq(self, key: str, value: bytes) -> None:
        if self.fail_dlq is not None:
            raise self.fail_dlq
        self.dlq.append((key, value))

    # --- assertions helpers
    def produced(self) -> list[dict]:
        return [msgspec.json.decode(value, type=dict) for _topic, _key, value in self.sent]

    def dlq_events(self) -> list[DlqEvent]:
        return [msgspec.json.decode(value, type=DlqEvent) for _key, value in self.dlq]


# --- event builders ----------------------------------------------------------


def candidate(**overrides: Any) -> dict:
    base = {
        "user_id_pseudo": CANARY_USER,
        "email": CANARY_EMAIL,
        "name": CANARY_NAME,
        "phone": CANARY_PHONE,
    }
    base.update(overrides)
    return base


def event(
    *,
    tenant: str = TENANT_A,
    event_id: str = "01J8XQ4M7K2P9R3S01",
    sequence: str = "0000000001",
    type: str = "com.careerpage.career.job-viewed",
    partitionkey: str | None = None,
    sourcechannel: str | None = None,
    time: str = "2026-09-30T14:43:09.123Z",
    **cand: Any,
) -> dict:
    ev = {
        "specversion": "1.0",
        "id": event_id,
        "source": f"/careers/{tenant}",
        "type": type,
        "time": time,
        "subject": "job_88320491",
        "dataschema": "https://schema.careerpage.example/event/1.0",
        "datacontenttype": "application/json",
        "sequence": sequence,
        "data": {
            "candidate": candidate(**cand),
            "event_payload": {"job_id": "job_88320491", "session_id": "sess_8839201923"},
        },
    }
    if partitionkey is not None:
        ev["partitionkey"] = partitionkey
    if sourcechannel is not None:
        ev["sourcechannel"] = sourcechannel
    return ev


def body(*events: dict) -> bytes:
    return msgspec.json.encode(list(events))


def many(count: int, *, tenant: str = TENANT_A) -> list[dict]:
    return [event(tenant=tenant, event_id=f"01J8XQ4M7K2P9R3S{i:02d}", sequence=f"{i:010d}") for i in range(count)]


def run(
    raw: bytes,
    *,
    issuer,
    verifier,
    tenants,
    keys,
    buckets,
    sink,
    parse_budget=None,
    token_str: str | None = None,
    x_source_type: str | None = None,
    **kwargs,
) -> IngestResult:
    return ingest_batch(
        raw,
        authorization=f"Bearer {token_str or token(issuer)}",
        x_source_type=x_source_type,
        sink=sink,
        verifier=verifier,
        tenants=tenants,
        keys=keys,
        buckets=buckets,
        parse_budget=parse_budget,
        **kwargs,
    )


# =============================================================================
# 1. composition and the happy path
# =============================================================================


def test_a_valid_batch_is_accepted_and_produced(issuer, verifier, tenants, keys, buckets, sink):
    result = run(
        body(event(event_id="01JAA", sequence="0000000001"), event(event_id="01JBB", sequence="0000000002")),
        issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink,
    )

    assert result.accepted == 2
    assert result.rejected == []
    assert len(sink.sent) == 2
    assert {topic for topic, _key, _value in sink.sent} == {TOPIC_RAW}


def test_the_202_response_shape_is_accepted_and_rejected(issuer, verifier, tenants, keys, buckets, sink):
    raw = body(event(event_id="01JAA"), event(event_id="01JBB", type="com.nope.bad"))
    result = run(raw, issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)

    assert set(result.to_response()) == {"accepted", "rejected"}
    assert result.to_response()["accepted"] == 1
    assert [r["index"] for r in result.to_response()["rejected"]] == [1]


def test_the_pipeline_does_not_import_the_kafka_module():
    """The sink is injected. Importing app.kafka here would merge two owners."""
    import app.ingest.handler as handler
    import app.ingest.pipeline as pipeline

    for module in (pipeline, handler):
        source = open(module.__file__, encoding="utf-8").read()
        assert "app.kafka" not in source
        assert "from app import kafka" not in source


# =============================================================================
# 2. the derived key, and the client hint we do not trust
# =============================================================================


def test_the_kafka_key_is_derived_by_the_gateway(issuer, verifier, tenants, keys, buckets, sink):
    run(
        body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    _topic, key, _value = sink.sent[0]

    pseudo = pseudonymize(keys.keys_for(TENANT_A).mac_key, CANARY_USER)
    assert key == derive_kafka_key(TENANT_A, pseudo)
    assert CANARY_USER not in key
    assert CANARY_EMAIL not in key


def test_the_produced_event_carries_the_derived_partitionkey(issuer, verifier, tenants, keys, buckets, sink):
    run(
        body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    produced = sink.produced()[0]
    pseudo = pseudonymize(keys.keys_for(TENANT_A).mac_key, CANARY_USER)

    assert produced["partitionkey"] == derive_kafka_key(TENANT_A, pseudo)


def test_a_partitionkey_disagreeing_with_the_derived_key_is_403(issuer, verifier, tenants, keys, buckets, sink):
    ev = event(partitionkey="acme_8921|some-other-user-pseudo")
    with pytest.raises(Forbidden):
        run(
            body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
        )
    assert sink.sent == []


def test_a_partitionkey_that_agrees_is_accepted(issuer, verifier, tenants, keys, buckets, sink):
    pseudo = pseudonymize(keys.keys_for(TENANT_A).mac_key, CANARY_USER)
    ev = event(partitionkey=derive_kafka_key(TENANT_A, pseudo))

    result = run(
        body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert result.accepted == 1
    assert sink.sent[0][1] == derive_kafka_key(TENANT_A, pseudo)


def test_a_missing_token_is_401_and_reaches_nothing(verifier, tenants, keys, buckets, sink):
    """Even a batch that is entirely garbage is refused before it is looked at:
    an unauthenticated caller must not learn which of its events were valid."""
    with pytest.raises(Unauthorized):
        ingest_batch(
            body(event(type="com.nope.bad"), event(event_id="01JBB")),
            authorization=None, sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets,
        )
    assert sink.sent == []
    assert sink.dlq == []


def test_the_body_source_disagreeing_with_the_token_is_403(issuer, verifier, tenants, keys, buckets, sink):
    with pytest.raises(Forbidden):
        run(
            body(event(tenant=TENANT_B)),
            issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink,
        )
    assert sink.sent == []


# =============================================================================
# 3. per-event rejection
# =============================================================================


def test_one_bad_event_does_not_fail_a_50_event_batch(issuer, verifier, tenants, keys, buckets, sink):
    events = many(50)
    events[17]["type"] = "com.careerpage.career.not-real"

    result = run(
        body(*events), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )

    assert result.accepted == 49
    assert [r.index for r in result.rejected] == [17]
    assert len(sink.sent) == 49


def test_a_bad_event_among_500_still_leaves_the_rest(issuer, verifier, tenants, keys, buckets, sink):
    events = many(500)
    events[499]["time"] = "30-09-2026 14:43:09"

    result = run(
        body(*events), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert result.accepted == 499
    assert [r.index for r in result.rejected] == [499]


def test_rejection_indices_point_at_the_original_body(issuer, verifier, tenants, keys, buckets, sink):
    """A batch whose first and last events are garbage: indices are 0 and 2, not
    positions within a filtered list."""
    events = [event(event_id="01JAA", type="com.nope.bad"), event(event_id="01JBB"), event(event_id="01JCC", time="nope")]

    result = run(
        body(*events), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert result.accepted == 1
    assert [r.index for r in result.rejected] == [0, 2]


def test_every_event_can_fail_independently(issuer, verifier, tenants, keys, buckets, sink):
    events = [event(event_id="01JAA", type="com.nope.bad"), event(event_id="01JBB", time="nope"), "not-an-object"]

    result = run(
        body(*events), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert result.accepted == 0
    assert [r.index for r in result.rejected] == [0, 1, 2]


def test_an_undecodable_event_does_not_stop_the_batch(issuer, verifier, tenants, keys, buckets, sink):
    result = run(
        body({"not": "an event"}), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert result.accepted == 0
    assert len(result.rejected) == 1


def test_an_unknown_top_level_attribute_is_named_in_the_reason(issuer, verifier, tenants, keys, buckets, sink):
    ev = event()
    ev["career_site_id"] = TENANT_A
    result = run(
        body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert result.accepted == 0
    assert "UNKNOWN_ATTRIBUTE" in result.rejected[0].reason


# =============================================================================
# 4. the DLQ payload is the POST-encryption event
# =============================================================================


def test_a_validation_rejection_dlq_carries_the_unencrypted_ingress_payload(issuer, verifier, tenants, keys, buckets, sink):
    """Path (a). It failed validation, so it was NEVER encrypted. The DLQ record
    is therefore the plaintext ingress payload -- true only because the failure
    happened before the encrypt stage. Pinning it so the property cannot change
    silently."""
    ev = event(event_id="01JAA", type="com.careerpage.career.not-real")
    run(body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)

    assert len(sink.dlq) == 1
    payload = sink.dlq_events()[0].data.original_payload
    assert payload["data"]["candidate"]["email"] == CANARY_EMAIL
    assert payload["data"]["candidate"]["user_id_pseudo"] == CANARY_USER
    assert sink.sent == []


def test_a_post_encryption_rejection_dlq_carries_the_encrypted_event(issuer, verifier, tenants, keys, buckets, sink):
    """Path (b). The event WAS encrypted and then crossed MAX_EVENT_BYTES, so
    the DLQ must carry the ciphertext. A plaintext payload here would make the
    DLQ a second PII store and a replayed record would be double-encrypted."""
    ev = event(event_id="01JAA", name="x" * 900, email="y" * 900, phone="z" * 900)
    result = run(
        body(ev),
        issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink,
        max_event_bytes=2000,
    )

    assert result.accepted == 0
    assert [r.index for r in result.rejected] == [0]
    assert sink.sent == []

    payload = sink.dlq_events()[0].data.original_payload
    candidate_block = payload["data"]["candidate"]
    assert candidate_block["email_enc"] and candidate_block["name_enc"] and candidate_block["phone_enc"]
    for canary in (CANARY_EMAIL, CANARY_NAME, CANARY_PHONE, CANARY_USER):
        assert canary not in msgspec.json.encode(payload).decode()
    assert canaries_absent(payload)


def test_a_dlq_record_is_a_cloudevent_a_replay_worker_can_read(issuer, verifier, tenants, keys, buckets, sink):
    ev = event(event_id="01JAA", type="com.careerpage.career.not-real")
    run(body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)

    dlq_event = sink.dlq_events()[0]
    assert dlq_event.type == DLQ_ERROR_TYPE
    assert dlq_event.source == SOURCE_A
    assert dlq_event.data.error_context.stage
    assert dlq_event.data.error_context.reason


def test_a_dlq_record_can_be_replayed_without_re_encryption(issuer, verifier, tenants, keys, buckets, sink):
    """C3: the replayed event's ciphertexts still authenticate under the tenant
    key, which is only true if the DLQ stored the post-encryption form."""
    ev = event(event_id="01JAA", name="x" * 900, email="y" * 900, phone="z" * 900)
    run(
        body(ev),
        issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink,
        max_event_bytes=2000,
    )

    replayed = dlq_data_to_ingress(sink.dlq_events()[0].data)
    assert (
        keys.decrypt_field(
            TENANT_A,
            replayed.data.candidate.name_enc,
            source=replayed.source,
            event_id=replayed.id,
            event_type=replayed.type,
            field_name="name_enc",
        )
        == "x" * 900
    )


def test_the_dlq_is_keyed_by_tenant_not_by_user(issuer, verifier, tenants, keys, buckets, sink):
    """A rejected event often has no usable user_id_pseudo -- that is frequently
    why it was rejected -- so the DLQ cannot be keyed by the derived user key."""
    ev = event(event_id="01JAA", type="com.careerpage.career.not-real")
    run(body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)

    assert sink.dlq[0][0] == SOURCE_A


def canaries_absent(payload: dict) -> bool:
    blob = msgspec.json.encode(payload)
    return not any(c.encode() in blob for c in (CANARY_EMAIL, CANARY_NAME, CANARY_PHONE, CANARY_USER))


# =============================================================================
# 5/6. tenancy
# =============================================================================


def test_a_batch_mixing_two_tenants_is_403(issuer, verifier, tenants, keys, buckets, sink):
    with pytest.raises(Forbidden):
        run(
            body(event(event_id="01JAA"), event(tenant=TENANT_B, event_id="01JBB")),
            issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink,
        )
    assert sink.sent == []
    assert sink.dlq == []


def test_an_unknown_tenant_is_403(issuer, verifier, tenants, keys, buckets, sink):
    with pytest.raises(Forbidden):
        run(
            body(event(tenant="ghost_0001", event_id="01JAA")),
            issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink,
            token_str=token(issuer, tenant="ghost_0001", credential_id="cred_a_web"),
        )


def test_a_forged_source_type_hint_is_403(issuer, verifier, tenants, keys, buckets, sink):
    with pytest.raises(Forbidden):
        run(
            body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink,
            x_source_type="MOBILE_APP",
        )


def test_the_recorded_channel_comes_from_the_credential_not_the_body(issuer, verifier, tenants, keys, buckets, sink):
    ev = event(sourcechannel="MOBILE_APP")  # the client is wrong about itself
    run(body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)

    assert sink.produced()[0]["sourcechannel"] == "WEB_APP"


# =============================================================================
# 7. rate limiting: charged per event, denied per batch
# =============================================================================


def test_rate_limiting_charges_one_token_per_event(issuer, verifier, tenants, keys, buckets, sink, clock):
    run(
        body(*many(10)), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert buckets.bucket_for(TENANT_A).consumed == 10
    assert buckets.bucket_for(TENANT_A).denied == 0


def test_a_denied_batch_is_429_and_the_whole_batch_is_refused(issuer, verifier, tenants, keys, clock, sink):
    exhausted = BucketRegistry(
        {TENANT_A: TenantLimits(rate=0.001, burst=2.0)}, default=TenantLimits(rate=0.001, burst=2.0),
        clock=lambda: clock[0],
    )
    with pytest.raises(RateLimited) as caught:
        run(
            body(*many(10)), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys,
            buckets=exhausted, sink=sink,
        )
    assert caught.value.retry_after >= 1
    assert sink.sent == []


def test_a_429_writes_no_dlq_entry(issuer, verifier, tenants, keys, clock, sink):
    """A rate-limited batch is not poisoned: it will succeed on retry, so filing
    it in the DLQ would bury real poison under retry noise."""
    exhausted = BucketRegistry(
        {TENANT_A: TenantLimits(rate=0.001, burst=2.0)}, default=TenantLimits(rate=0.001, burst=2.0),
        clock=lambda: clock[0],
    )
    events = many(10)
    events[3]["type"] = "com.careerpage.career.not-real"  # would otherwise be DLQ'd

    with pytest.raises(RateLimited):
        run(
            body(*events), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys,
            buckets=exhausted, sink=sink,
        )
    assert sink.dlq == []


def test_the_token_budget_is_never_refunded_by_a_partial_batch(issuer, verifier, tenants, keys, clock, sink):
    """The bucket is charged as it is spent, and a denial is not a rollback."""
    exhausted = BucketRegistry(
        {TENANT_A: TenantLimits(rate=0.001, burst=2.0)}, default=TenantLimits(rate=0.001, burst=2.0),
        clock=lambda: clock[0],
    )
    with pytest.raises(RateLimited):
        run(
            body(*many(10)), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys,
            buckets=exhausted, sink=sink,
        )
    assert exhausted.bucket_for(TENANT_A).consumed == 2
    assert exhausted.bucket_for(TENANT_A).denied == 1


# =============================================================================
# 8. body limits
# =============================================================================


def test_more_than_500_events_is_413(issuer, verifier, tenants, keys, buckets, sink):
    with pytest.raises(PayloadTooLarge):
        run(
            body(*many(MAX_EVENTS_PER_BATCH + 1)), issuer=issuer, verifier=verifier, tenants=tenants,
            keys=keys, buckets=buckets, sink=sink,
        )
    assert sink.sent == []


def test_exactly_500_events_is_legal(issuer, verifier, tenants, keys, buckets, sink):
    result = run(
        body(*many(MAX_EVENTS_PER_BATCH)), issuer=issuer, verifier=verifier, tenants=tenants,
        keys=keys, buckets=buckets, sink=sink,
    )
    assert result.accepted == MAX_EVENTS_PER_BATCH


def test_a_body_over_four_mib_is_413(issuer, verifier, tenants, keys, buckets, sink):
    events = many(200)
    for ev in events:
        # 200 x ~21 KiB: each event is inside the 48 KiB ingress cap, so only the
        # batch cap can catch it.
        ev["data"]["event_payload"]["client_metadata"] = {"blob": "x" * 21_000}
    with pytest.raises(PayloadTooLarge):
        run(body(*events), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)
    assert sink.sent == []


def test_an_event_over_the_ingress_cap_is_413(issuer, verifier, tenants, keys, buckets, sink):
    ev = event()
    ev["data"]["event_payload"]["client_metadata"] = {"blob": "x" * 200_000}
    with pytest.raises(PayloadTooLarge):
        run(body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)
    assert sink.sent == []


def test_the_declared_length_is_refused_before_the_body_is_read():
    """uvicorn has no default body-size limit, so the handler's only pre-read
    lever is Content-Length. Checked before anything is allocated."""
    check_declared_length(None)
    check_declared_length("1024")
    with pytest.raises(PayloadTooLarge):
        check_declared_length(str(MAX_BATCH_BYTES + 1))
    with pytest.raises(PayloadTooLarge):
        check_declared_length("not-a-number")


def test_a_413_names_the_limit_not_the_body(issuer, verifier, tenants, keys, buckets, sink):
    ev = event()
    ev["data"]["event_payload"]["client_metadata"] = {"blob": CANARY_EMAIL * 10_000}
    with pytest.raises(PayloadTooLarge) as caught:
        run(body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)
    assert CANARY_EMAIL not in str(caught.value)


def test_a_body_that_is_not_a_batch_array_is_a_400(issuer, verifier, tenants, keys, buckets, sink):
    with pytest.raises(NotABatch) as caught:
        run(
            msgspec.json.encode({"not": "a batch"}), issuer=issuer, verifier=verifier, tenants=tenants,
            keys=keys, buckets=buckets, sink=sink,
        )
    assert caught.value.reason == REASON_NOT_A_BATCH


# =============================================================================
# 9. the post-encryption size check
# =============================================================================


def test_an_event_that_crosses_the_limit_only_after_encryption_is_rejected(issuer, verifier, tenants, keys, buckets, sink):
    """G2: base64 expansion plus nonce+tag per field can push an event that fit
    the ingress cap over MAX_EVENT_BYTES. The check runs on the ENCRYPTED event."""
    ev = event(name="x" * 900, email="y" * 900, phone="z" * 900)
    result = run(
        body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink,
        max_event_bytes=2000,
    )
    assert result.accepted == 0
    assert "OVERSIZED" in result.rejected[0].reason
    assert sink.sent == []


def test_the_post_encryption_check_leaves_small_events_alone(issuer, verifier, tenants, keys, buckets, sink):
    result = run(
        body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert result.accepted == 1
    assert len(sink.sent) == 1
    assert sink.dlq == []


def test_the_produced_event_actually_meets_the_published_limit(issuer, verifier, tenants, keys, buckets, sink):
    """The real MAX_EVENT_BYTES, not the injected test one."""
    run(body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)
    for _topic, _key, value in sink.sent:
        assert len(value) <= MAX_EVENT_BYTES


# =============================================================================
# 10. 202 is not a durability receipt
# =============================================================================


def test_the_result_says_accepted_into_buffer_not_durably_stored(issuer, verifier, tenants, keys, buckets, sink):
    result = run(
        body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert result.durability == DURABILITY_ACCEPTED_INTO_BUFFER
    assert "durab" in IngestResult.__doc__.lower()


def test_the_result_does_not_claim_the_events_reached_kafka(issuer, verifier, tenants, keys, buckets, sink):
    result = run(
        body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert "kafka" not in result.to_response()
    assert "durab" not in result.to_response()


# =============================================================================
# 12. the sink
# =============================================================================


def test_a_full_producer_buffer_is_503(issuer, verifier, tenants, keys, buckets, sink):
    sink.fail_raw = SinkUnavailable("producer queue is full")
    with pytest.raises(SinkUnavailable):
        run(
            body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
        )


def test_an_unreachable_broker_is_503(issuer, verifier, tenants, keys, buckets, sink):
    sink.fail_raw = ConnectionError("broker down")
    with pytest.raises(SinkUnavailable):
        run(
            body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
        )


def test_a_sink_raising_something_else_is_not_swallowed(issuer, verifier, tenants, keys, buckets, sink):
    sink.fail_raw = RuntimeError("bug in the producer")
    with pytest.raises(RuntimeError):
        run(
            body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
        )


def test_the_sink_protocol_is_what_the_pipeline_expects():
    assert hasattr(Sink, "sink") and hasattr(Sink, "sink_dlq")


# =============================================================================
# 13/14. encryption hygiene
# =============================================================================


def test_no_canary_reaches_the_produced_record(issuer, verifier, tenants, keys, buckets, sink):
    run(body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)

    wire = b"".join(value for _t, _k, value in sink.sent)
    for canary in (CANARY_EMAIL, CANARY_NAME, CANARY_PHONE, CANARY_USER):
        assert canary.encode() not in wire
    assert b"@example.invalid" not in wire


def test_the_produced_candidate_is_actually_decryptable(issuer, verifier, tenants, keys, buckets, sink):
    run(body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)
    produced = sink.produced()[0]
    name_enc = produced["data"]["candidate"]["name_enc"]

    assert (
        keys.decrypt_field(
            TENANT_A, name_enc, source=produced["source"], event_id=produced["id"],
            event_type=produced["type"], field_name="name_enc",
        )
        == CANARY_NAME
    )


def test_the_published_record_is_the_contract_shape(issuer, verifier, tenants, keys, buckets, sink):
    from contracts.attributes import extra_attributes

    run(body(event()), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)
    produced = sink.produced()[0]
    assert extra_attributes(produced) == set()
    assert produced["keyversion"] == 1


def test_a_client_supplied_ciphertext_is_refused(issuer, verifier, tenants, keys, buckets, sink):
    """The gateway is the only writer of ciphertext. Accepting a client's would
    make a replayed DLQ record double-encrypt itself."""
    ev = event()
    ev["data"]["candidate"]["email_enc"] = "1.Y2lwaGVydGV4dA"
    result = run(
        body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert result.accepted == 0
    assert sink.sent == []


def test_rejection_reasons_never_carry_the_offending_value(issuer, verifier, tenants, keys, buckets, sink):
    ev = event(type=f"{CANARY_EMAIL}.nope")
    result = run(
        body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink
    )
    assert result.accepted == 0
    assert CANARY_EMAIL not in result.rejected[0].reason


def test_the_ingress_allowlist_matches_the_published_one():
    from app.ingest.pipeline import IngressEvent

    assert set(IngressEvent.__struct_fields__) == set(SAFE_CONTEXT_ATTRS) | {"data"}


def test_the_published_examples_are_the_POST_encryption_shape(issuer, verifier, tenants, keys, buckets, sink):
    """A cross-task conflict, pinned so it cannot be discovered at the demo.

    `contracts/examples/*.json` and `driver/fsm.py` both emit `data.candidate`
    ALREADY holding `*_enc` -- they are the *Kafka* shape, not the *request*
    shape. The gateway encrypts, so its request shape is the plaintext one, and
    these are refused. The fix belongs upstream: the ingress schema needs
    publishing in `CONTRACT.md` alongside `event.schema.json`, and the examples
    and the driver corpus need regenerating in it. Until then the driver cannot
    drive load through `/v1/ingest`.
    """
    import json

    from pathlib import Path

    examples = Path(__file__).resolve().parents[2] / "contracts" / "examples"
    raw = (examples / "job-viewed.json").read_bytes()
    result = run(raw, issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)

    assert result.accepted == 0
    assert "SCHEMA" in result.rejected[0].reason
    # The example is unambiguously post-encryption: it carries a `*_enc` field.
    assert "email_enc" in json.loads(raw)[0]["data"]["candidate"]


def test_the_pipeline_body_never_names_a_pii_value(issuer, verifier, tenants, keys, buckets, sink):
    ev = event(event_id="01JAA", type="com.careerpage.career.not-real")
    run(body(ev), issuer=issuer, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink)

    dlq_event = sink.dlq_events()[0]
    reason = dlq_event.data.error_context.reason
    assert CANARY_EMAIL not in reason
    assert "not-real" not in reason


# =============================================================================
# HTTP handler
# =============================================================================


def build_client(*, sink, verifier, tenants, keys, buckets, parse_budget=None) -> TestClient:
    app = FastAPI()
    app.include_router(
        build_ingest_router(
            verifier=verifier, tenants=tenants, keys=keys, buckets=buckets, sink=sink, parse_budget=parse_budget
        )
    )
    return TestClient(app)


def post(client: TestClient, issuer, raw: bytes, *, tenant: str = TENANT_A, **headers):
    return client.post(
        "/v1/ingest",
        content=raw,
        headers={
            "Content-Type": "application/cloudevents-batch+json",
            "Authorization": f"Bearer {token(issuer, tenant=tenant)}",
            **headers,
        },
    )


def test_the_route_returns_202_with_the_contract_body(verifier, tenants, keys, buckets, sink, issuer):
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = post(client, issuer, body(event(event_id="01JAA"), event(event_id="01JBB")))

    assert response.status_code == 202
    assert response.json() == {"accepted": 2, "rejected": []}
    assert len(sink.sent) == 2


def test_a_batch_with_a_bad_event_is_still_202(verifier, tenants, keys, buckets, sink, issuer):
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = post(client, issuer, body(event(event_id="01JAA"), event(event_id="01JBB", type="com.nope.x")))

    assert response.status_code == 202
    payload = response.json()
    assert payload["accepted"] == 1
    assert payload["rejected"][0]["index"] == 1


def test_no_authorization_header_is_401(verifier, tenants, keys, buckets, sink, issuer):
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = client.post("/v1/ingest", content=body(event()), headers={"Content-Type": "application/cloudevents-batch+json"})

    assert response.status_code == 401
    assert sink.sent == []


def test_a_garbage_token_is_401(verifier, tenants, keys, buckets, sink, issuer):
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = client.post(
            "/v1/ingest", content=body(event()),
            headers={"Content-Type": "application/cloudevents-batch+json", "Authorization": "Bearer not-a-jwt"},
        )
    assert response.status_code == 401


def test_a_401_body_does_not_say_which_check_failed(verifier, tenants, keys, buckets, sink, issuer):
    """A 401 that distinguishes bad-signature from bad-audience is a signature
    oracle for a prober."""
    expired = jwt.encode(
        {
            "career_site_id": TENANT_A,
            "credential_id": "cred_a_web",
            "aud": AUDIENCE,
            "exp": dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1),
        },
        issuer[0],
        algorithm="EdDSA",
    )
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = client.post(
            "/v1/ingest", content=body(event()),
            headers={"Content-Type": "application/cloudevents-batch+json", "Authorization": f"Bearer {expired}"},
        )

    assert response.status_code == 401
    assert response.json() == {"reason": "UNAUTHORIZED"}


def test_a_mixed_tenant_batch_is_403(verifier, tenants, keys, buckets, sink, issuer):
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = post(client, issuer, body(event(event_id="01JAA"), event(tenant=TENANT_B, event_id="01JBB")))

    assert response.status_code == 403
    assert sink.sent == []


def test_a_partitionkey_mismatch_is_403(verifier, tenants, keys, buckets, sink, issuer):
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = post(client, issuer, body(event(partitionkey="acme_8921|not-the-derived-key")))

    assert response.status_code == 403


def test_too_many_events_is_413(verifier, tenants, keys, buckets, sink, issuer):
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = post(client, issuer, body(*many(MAX_EVENTS_PER_BATCH + 1)))

    assert response.status_code == 413
    assert sink.sent == []


def test_an_oversized_body_is_413(verifier, tenants, keys, buckets, sink, issuer):
    ev = event()
    ev["data"]["event_payload"]["client_metadata"] = {"blob": "x" * 200_000}
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = post(client, issuer, body(ev))

    assert response.status_code == 413
    assert sink.sent == []


def test_an_oversized_declared_length_is_413_without_reading_the_body(verifier, tenants, keys, buckets, sink, issuer):
    raw = body(event())
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = client.post(
            "/v1/ingest", content=raw,
            headers={
                "Content-Type": "application/cloudevents-batch+json",
                "Authorization": f"Bearer {token(issuer)}",
                "Content-Length": str(MAX_BATCH_BYTES + 1),
            },
        )
    assert response.status_code == 413


def test_a_rate_limited_batch_is_429_with_retry_after(verifier, tenants, keys, clock, sink, issuer):
    exhausted = BucketRegistry(
        {TENANT_A: TenantLimits(rate=0.001, burst=2.0)}, default=TenantLimits(rate=0.001, burst=2.0),
        clock=lambda: clock[0],
    )
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=exhausted) as client:
        response = post(client, issuer, body(*many(10)))

    assert response.status_code == 429
    assert int(response.headers["Retry-After"]) >= 1
    assert sink.sent == []


def test_a_full_buffer_is_503_with_retry_after(verifier, tenants, keys, buckets, sink, issuer):
    sink.fail_raw = SinkUnavailable("producer queue is full")
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = post(client, issuer, body(event()))

    assert response.status_code == 503
    assert "Retry-After" in response.headers


def test_a_body_that_is_not_a_batch_array_is_400(verifier, tenants, keys, buckets, sink, issuer):
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = post(client, issuer, msgspec.json.encode({"not": "a batch"}))

    assert response.status_code == 400
    assert response.json() == {"reason": REASON_NOT_A_BATCH}


def test_the_hot_path_bypasses_fastapis_validation_layer(verifier, tenants, keys, buckets, sink, issuer):
    """No Pydantic request model: an event Pydantic would reject structurally
    still reaches our own per-event rejection, and the answer is 202 -- not the
    422 FastAPI would have produced on its own."""
    ev = event(event_id="01JAA", type="com.careerpage.career.not-real")
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = post(client, issuer, body(ev))

    assert response.status_code == 202
    assert response.json()["rejected"][0]["index"] == 0


def test_the_route_accepts_an_absent_content_type(verifier, tenants, keys, buckets, sink, issuer):
    """A CloudEvents batch is a JSON array; the header is advice, not a gate."""
    with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
        response = client.post(
            "/v1/ingest", content=body(event()),
            headers={"Authorization": f"Bearer {token(issuer)}"},
        )
    assert response.status_code == 202


def test_no_log_line_carries_plaintext_pii(verifier, tenants, keys, buckets, sink, issuer, caplog):
    with caplog.at_level(logging.DEBUG):
        with build_client(sink=sink, verifier=verifier, tenants=tenants, keys=keys, buckets=buckets) as client:
            ok = post(client, issuer, body(event()))
            bad = post(client, issuer, body(event(event_id="01JAA", type="com.nope.x")))

    assert ok.status_code == 202 and bad.status_code == 202
    logged = "\n".join(record.getMessage() for record in caplog.records)
    for canary in (CANARY_EMAIL, CANARY_NAME, CANARY_PHONE, CANARY_USER):
        assert canary not in logged


# =============================================================================
# 13. operator-only decrypt
# =============================================================================


def encrypted_event(keys, *, tenant: str = TENANT_A) -> dict:
    metadata = protect_candidate(
        registry=keys,
        source=f"/careers/{tenant}",
        event_id="01J8XQ4M7K2P9R3S01",
        event_type="com.careerpage.career.job-viewed",
        raw_user_id=CANARY_USER,
        email=CANARY_EMAIL,
        name=CANARY_NAME,
    )
    from contracts.cloudevent import CloudEvent, Data, EventPayload

    ev = CloudEvent(
        specversion="1.0",
        id="01J8XQ4M7K2P9R3S01",
        source=f"/careers/{tenant}",
        type="com.careerpage.career.job-viewed",
        time="2026-09-30T14:43:09.123Z",
        keyversion=keys.keys_for(tenant).key_version,
        data=Data(
            candidate=metadata,
            event_payload=EventPayload(job_id="job_88320491", session_id="sess_8839201923"),
        ),
    )
    return msgspec.json.decode(msgspec.json.encode(ev), type=dict)


@pytest.fixture
def audit() -> list:
    return []


def decrypt_client(keys, audit, *, bucket=None):
    from app.ratelimit.bucket import TokenBucket

    app = FastAPI()
    app.include_router(
        build_decrypt_router(
            keys=keys,
            operator_key=OPERATOR_KEY,
            operator_id=OPERATOR_ID,
            bucket=bucket or TokenBucket(rate=10.0, burst=10.0, clock=lambda: 1000.0),
            audit=audit.append,
        )
    )
    return TestClient(app)


def decrypt_request(**overrides) -> dict:
    payload = {"career_site_id": TENANT_A, "field": "name_enc"}
    payload.update(overrides)
    return payload


def test_the_decrypt_endpoint_needs_the_operator_credential(keys, audit):
    with decrypt_client(keys, audit) as client:
        response = client.post("/v1/decrypt", json=decrypt_request(event=encrypted_event(keys)))
    assert response.status_code == 401
    assert len(audit) == 1
    assert audit[0].outcome == "denied"


def test_a_tenant_jwt_cannot_decrypt_an_arbitrary_event(keys, audit, issuer):
    """The decrypt credential is separate from tenant auth, so a perfectly valid
    tenant token is simply not an operator credential."""
    with decrypt_client(keys, audit) as client:
        response = client.post(
            "/v1/decrypt",
            json=decrypt_request(event=encrypted_event(keys)),
            headers={"X-Operator-Key": token(issuer)},
        )
    assert response.status_code == 401


def test_the_operator_can_decrypt_one_field(keys, audit):
    with decrypt_client(keys, audit) as client:
        response = client.post(
            "/v1/decrypt",
            json=decrypt_request(event=encrypted_event(keys)),
            headers={"X-Operator-Key": OPERATOR_KEY.decode()},
        )

    assert response.status_code == 200
    assert response.json()["value"] == CANARY_NAME
    assert response.headers["Cache-Control"] == "no-store"


def test_the_decrypt_audit_record_names_who_and_which_event(keys, audit):
    with decrypt_client(keys, audit) as client:
        client.post(
            "/v1/decrypt",
            json=decrypt_request(event=encrypted_event(keys)),
            headers={"X-Operator-Key": OPERATOR_KEY.decode()},
        )

    assert len(audit) == 1
    record = audit[0]
    assert record.actor == OPERATOR_ID
    assert record.career_site_id == TENANT_A
    assert record.source == SOURCE_A
    assert record.event_id == "01J8XQ4M7K2P9R3S01"
    assert record.field == "name_enc"
    assert record.outcome == "ok"


def test_the_audit_record_carries_no_plaintext(keys, audit):
    with decrypt_client(keys, audit) as client:
        client.post(
            "/v1/decrypt",
            json=decrypt_request(event=encrypted_event(keys)),
            headers={"X-Operator-Key": OPERATOR_KEY.decode()},
        )

    blob = repr(audit[0])
    for canary in (CANARY_EMAIL, CANARY_NAME, CANARY_USER):
        assert canary not in blob
    assert OPERATOR_KEY.decode() not in blob


def test_a_denied_decrypt_is_audit_logged_too(keys, audit):
    with decrypt_client(keys, audit) as client:
        response = client.post(
            "/v1/decrypt",
            json=decrypt_request(event=encrypted_event(keys)),
            headers={"X-Operator-Key": "wrong"},
        )
    assert response.status_code == 401
    assert len(audit) == 1
    assert audit[0].outcome == "denied"
    assert audit[0].actor == "unidentified"
    assert audit[0].event_id is None


def test_a_failed_decryption_is_audit_logged_and_does_not_echo_the_error(keys, audit):
    tampered = encrypted_event(keys)
    tampered["data"]["candidate"]["name_enc"] = "1.Y2lwaGVydGV4dA"

    with decrypt_client(keys, audit) as client:
        response = client.post(
            "/v1/decrypt",
            json=decrypt_request(event=tampered),
            headers={"X-Operator-Key": OPERATOR_KEY.decode()},
        )

    assert response.status_code == 400
    assert response.json() == {"reason": "DECRYPT_FAILED"}
    assert audit[0].outcome == "failed"


def test_decrypt_is_rate_limited(keys, audit):
    from app.ratelimit.bucket import TokenBucket

    bucket = TokenBucket(rate=0.001, burst=1.0, clock=lambda: 1000.0)
    with decrypt_client(keys, audit, bucket=bucket) as client:
        first = client.post(
            "/v1/decrypt", json=decrypt_request(event=encrypted_event(keys)),
            headers={"X-Operator-Key": OPERATOR_KEY.decode()},
        )
        second = client.post(
            "/v1/decrypt", json=decrypt_request(event=encrypted_event(keys)),
            headers={"X-Operator-Key": OPERATOR_KEY.decode()},
        )

    assert first.status_code == 200
    assert second.status_code == 429
    assert int(second.headers["Retry-After"]) >= 1


def test_a_rate_limited_decrypt_audits_the_denial(keys, audit):
    from app.ratelimit.bucket import TokenBucket

    bucket = TokenBucket(rate=0.001, burst=1.0, clock=lambda: 1000.0)
    with decrypt_client(keys, audit, bucket=bucket) as client:
        client.post("/v1/decrypt", json=decrypt_request(event=encrypted_event(keys)), headers={"X-Operator-Key": OPERATOR_KEY.decode()})
        client.post("/v1/decrypt", json=decrypt_request(event=encrypted_event(keys)), headers={"X-Operator-Key": OPERATOR_KEY.decode()})

    assert [record.outcome for record in audit] == ["ok", "rate-limited"]


def test_an_unknown_tenant_is_404_and_never_leaks_the_reason(keys, audit):
    with decrypt_client(keys, audit) as client:
        response = client.post(
            "/v1/decrypt",
            json=decrypt_request(career_site_id="ghost_0001", event=encrypted_event(keys)),
            headers={"X-Operator-Key": OPERATOR_KEY.decode()},
        )
    assert response.status_code == 404
    assert response.json() == {"reason": "UNKNOWN_TENANT"}


def test_a_malformed_decrypt_body_is_400(keys, audit):
    with decrypt_client(keys, audit) as client:
        response = client.post(
            "/v1/decrypt", content=b"not json",
            headers={"X-Operator-Key": OPERATOR_KEY.decode()},
        )
    assert response.status_code == 400


def test_a_field_that_is_not_a_pii_ciphertext_is_400(keys, audit):
    """The endpoint is not a general read-back oracle: only the five ciphertext
    fields are in scope, so an HMAC cannot be asked for at all."""
    with decrypt_client(keys, audit) as client:
        response = client.post(
            "/v1/decrypt",
            json=decrypt_request(field="user_id_pseudo", event=encrypted_event(keys)),
            headers={"X-Operator-Key": OPERATOR_KEY.decode()},
        )
    assert response.status_code == 400
    assert response.json() == {"reason": "UNKNOWN_FIELD"}


def test_an_empty_operator_key_is_refused_at_wiring_time(keys, audit):
    """A misconfigured empty key would make the constant-time compare succeed for
    a caller who sent no header at all. That must fail at startup, not in
    production."""
    with pytest.raises(ValueError, match="operator_key"):
        build_decrypt_router(
            keys=keys, operator_key=b"", operator_id=OPERATOR_ID,
            bucket=None, audit=audit.append,
        )


def test_another_tenants_ciphertext_does_not_decrypt(keys, audit):
    """An operator credential is global, so the AAD is what stops a field swap
    across tenants from reading back a plausible value."""
    with decrypt_client(keys, audit) as client:
        response = client.post(
            "/v1/decrypt",
            json=decrypt_request(career_site_id=TENANT_A, event=encrypted_event(keys, tenant=TENANT_B)),
            headers={"X-Operator-Key": OPERATOR_KEY.decode()},
        )
    assert response.status_code == 400
    assert response.json() == {"reason": "DECRYPT_FAILED"}
