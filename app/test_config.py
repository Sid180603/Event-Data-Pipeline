"""The ingress cap, measured. RED before this file existed.

`app/config.py` said, in a comment, that `INGRESS_EVENT_BYTES = 48 KiB` was
"provisional" and that "T6 measures the real figure". T6 finished and never
replaced it, so the number other teams were told they may send was still an
estimate. This file is the measurement, and it runs through the real pipeline
rather than a formula: a maximal ingress element goes through `ingest_batch`,
which is what calls `app.crypto.facade.protect_candidate`, which is what writes
the ciphertext, and the bytes that come out are the bytes stage 6 measured.

## The two shapes, because they behave differently

An ingress event's bulk can sit in two very different places, and the cap means
something different in each:

* **In a field nothing encrypts** -- `data.event_payload.client_metadata`,
  `recommended_job_ids`, `subject`, `id`. The egress event carries those bytes
  across unchanged, plus the envelope around them, so a cap-sized event is
  comfortably inside the 64 KiB ceiling.
* **In one of the five encrypted fields** -- `email`, `phone`,
  `alternate_phone`, `name`, `gender`. Each becomes
  `base64(nonce(12) || AES-GCM ciphertext || tag(16))`, and base64 expands by
  4/3 before any of it is a character in a JSON document. That is where the cap
  can stop meaning "the ceiling is unreachable".

`app/ingest/pipeline.py` stage 6 claims the ingress cap "makes MAX_EVENT_BYTES
unreachable by construction". The first test below confirms that for the first
shape; the second and third measure it for the second, which is the one a
client filling every candidate field to the cap actually sends.

## What this file deliberately does not do

It does not raise or lower `INGRESS_EVENT_BYTES`. The cap is a published
contract value: changing it changes what clients may send, and that is the
consumer teams' decision, not a test's. What it does instead is pin the measured
numbers, so a future change to the cap cannot move them silently, and so the
gap between the two shapes is written down where a test can point at it.

Run with `-s` to see the measured headroom printed.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Callable

import jwt
import msgspec
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from app.auth import Credential, TenantRegistry, TokenVerifier
from app.config import INGRESS_EVENT_BYTES, MAX_EVENT_BYTES, Settings
from app.crypto.facade import PII_FIELDS
from app.crypto.registry import TenantKeyRegistry
from app.ingest import ingest_batch
from app.ratelimit.registry import BucketRegistry, TenantLimits

AUDIENCE = "career-api"
TENANT = "acme_8921"
SOURCE = f"/careers/{TENANT}"
CRED = Credential(credential_id="cred_a_web", career_site_id=TENANT, source_channel="WEB_APP")
DEV_MASTER = b"test-only-master-secret-do-not-use!"

#: The five ingress field names the gateway encrypts, read from
#: `app.crypto.facade.PII_FIELDS` rather than retyped: a hard-coded list would
#: stop measuring the real encryption path the moment a sixth field appears.
PII_INGRESS_FIELDS = tuple(plaintext for plaintext, _ciphertext in PII_FIELDS)


# --- fixtures -----------------------------------------------------------------


class _RecordingSink:
    """In-memory stand-in, so this measurement needs no broker."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, bytes]] = []
        self.dlq: list[tuple[str, bytes]] = []

    def sink(self, topic: str, key: str, value: bytes) -> None:
        self.sent.append((topic, key, value))

    def sink_dlq(self, key: str, value: bytes) -> None:
        self.dlq.append((key, value))

    def close(self) -> None:  # pragma: no cover - nothing to drain
        pass


@dataclass(frozen=True)
class _Harness:
    """Everything `ingest_batch` needs, bundled so the tests take one fixture."""

    token: str
    verifier: TokenVerifier
    tenants: TenantRegistry
    keys: TenantKeyRegistry
    buckets: BucketRegistry


@pytest.fixture(scope="module")
def issuer() -> tuple[Ed25519PrivateKey, str]:
    key = Ed25519PrivateKey.generate()
    pem = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return key, pem.decode()


@pytest.fixture
def harness(issuer) -> _Harness:
    now = dt.datetime.now(dt.timezone.utc)
    token = jwt.encode(
        {
            "career_site_id": TENANT,
            "credential_id": CRED.credential_id,
            "aud": AUDIENCE,
            "iat": now,
            "exp": now + dt.timedelta(hours=1),
        },
        issuer[0],
        algorithm="EdDSA",
    )
    settings = Settings(master_secret=DEV_MASTER, key_version=1)
    generous = TenantLimits(rate=1.0, burst=10_000.0)
    return _Harness(
        token=token,
        verifier=TokenVerifier(
            public_key_pem=issuer[1], algorithm="EdDSA", audience=AUDIENCE
        ),
        tenants=TenantRegistry((CRED,)),
        keys=TenantKeyRegistry(settings, (TENANT,)),
        buckets=BucketRegistry({TENANT: generous}, default=generous),
    )


# --- event builders -----------------------------------------------------------


def _event(**candidate: Any) -> dict:
    """A minimal, valid ingress element, in the shape a client would send."""
    return {
        "specversion": "1.0",
        "id": "01J8XQ4M7K2P9R3S01",
        "source": SOURCE,
        "type": "com.careerpage.career.application-step-completed",
        "time": "2026-09-30T14:43:09.123Z",
        "subject": "job_88320491",
        "sequence": "0000000001",
        "data": {
            "candidate": {"user_id": "usr_992182741", **candidate},
            "event_payload": {
                "job_id": "job_88320491",
                "session_id": "sess_8839201923",
                "step_number": 3,
            },
        },
    }


def _bulk_in_encrypted_fields(filler: int) -> dict:
    """Spread `filler` bytes across the five fields the gateway encrypts."""
    share, remainder = divmod(filler, len(PII_INGRESS_FIELDS))
    values = {name: "A" * share for name in PII_INGRESS_FIELDS}
    last = PII_INGRESS_FIELDS[-1]
    values[last] += "A" * remainder
    return _event(**values)


def _bulk_in_plain_fields(filler: int) -> dict:
    """Put `filler` bytes in `client_metadata`, which nothing encrypts."""
    event = _event()
    event["data"]["event_payload"]["client_metadata"] = {"blob": "A" * filler}
    return event


def _element_of_exactly(build: Callable[[int], dict], target: int) -> bytes:
    """The largest element `build` can produce that is still exactly `target` bytes.

    Binary search rather than a subtraction, because the envelope's own JSON
    overhead means "a 48 KiB event" is not "48 KiB of filler". The assertion is
    the point: the measurement has to be of a byte-length that
    `split_limited_batch` actually admits, and it refuses `> INGRESS_EVENT_BYTES`.
    """
    low, high = 0, target
    while low < high:
        mid = (low + high + 1) // 2
        if len(msgspec.json.encode(build(mid))) <= target:
            low = mid
        else:
            high = mid - 1
    raw = msgspec.json.encode(build(low))
    assert len(raw) == target, f"could not hit {target} bytes exactly, got {len(raw)}"
    return raw


# --- the measurement ----------------------------------------------------------


@dataclass(frozen=True)
class _Measured:
    ingress_bytes: int
    egress_bytes: int
    accepted: int
    published: int
    dlq: int
    dlq_value: bytes

    @property
    def headroom_bytes(self) -> int:
        """Negative means the egress event is over `MAX_EVENT_BYTES`."""
        return MAX_EVENT_BYTES - self.egress_bytes


def _measure(
    build: Callable[[int], dict],
    ingress_bytes: int,
    harness: _Harness,
    *,
    max_event_bytes: int = MAX_EVENT_BYTES,
) -> _Measured:
    """Push one element of exactly `ingress_bytes` through the real pipeline.

    Every quantity in this file is in **ingress element bytes**, the unit
    `split_limited_batch` checks against `INGRESS_EVENT_BYTES`. The builders are
    parameterised by filler length only as the mechanism for hitting an exact
    byte count; no assertion is ever written in filler units, because "48 KiB of
    filler" and "a 48 KiB event" are different events.

    `max_event_bytes` raises **only** the per-event ceiling, and exists so the
    size of a refused event can be measured rather than inferred. A refused event
    publishes nothing, so without it the observed egress size of an over-cap
    event would read 0 -- a number that means "not produced", not "this big".
    With it raised, the same event is produced and its real length read off the
    sink. `validate_batch` keeps the real `MAX_EVENT_BYTES`, and the ingress cap
    is untouched, so nothing else about the measurement changes.
    """
    element = _element_of_exactly(build, ingress_bytes)
    sink = _RecordingSink()
    result = ingest_batch(
        msgspec.json.encode([msgspec.json.decode(element)]),
        authorization=f"Bearer {harness.token}",
        sink=sink,
        verifier=harness.verifier,
        tenants=harness.tenants,
        keys=harness.keys,
        buckets=harness.buckets,
        max_event_bytes=max_event_bytes,
    )
    return _Measured(
        ingress_bytes=len(element),
        egress_bytes=len(sink.sent[0][2]) if sink.sent else 0,
        accepted=result.accepted,
        published=len(sink.sent),
        dlq=len(sink.dlq),
        dlq_value=sink.dlq[0][1] if sink.dlq else b"",
    )


def _size_of_the_refused_event(build: Callable[[int], dict], ingress_bytes: int, harness: _Harness) -> int:
    """How big the encrypted event for `ingress_bytes` actually is.

    Measured by producing it with the ceiling lifted, never computed from the
    base64 expansion factor: the expansion is not the only term (there is a
    12-byte nonce and a 16-byte tag per encrypted field, and `omit_defaults`
    decides how much envelope survives), and an estimate is exactly what this
    file exists to replace.
    """
    raised = _measure(build, ingress_bytes, harness, max_event_bytes=MAX_EVENT_BYTES * 4)
    assert raised.published == 1, "the raised-ceiling run did not publish; measurement void"
    return raised.egress_bytes


def _fits(build: Callable[[int], dict], ingress_bytes: int, harness: _Harness) -> bool:
    """Whether an event of `ingress_bytes` still fits under `MAX_EVENT_BYTES`."""
    measured = _measure(build, ingress_bytes, harness)
    return measured.published == 1 and measured.egress_bytes <= MAX_EVENT_BYTES


def _smallest(build: Callable[[int], dict]) -> int:
    """The byte length of `build(0)`: the smallest event these builders make.

    The floor of the search below. Not zero -- every event carries an envelope,
    and a search that started at zero would ask the measurement to build an
    event that cannot exist.
    """
    return len(msgspec.json.encode(build(0)))


# --- the tests ----------------------------------------------------------------


def test_a_cap_sized_event_with_its_bulk_outside_the_encrypted_fields_is_published(
    harness, record_property
) -> None:
    """The ordinary client: a big form payload, small candidate fields.

    None of it is encrypted, so the egress event is the ingress event plus the
    envelope and the ciphertext of ~50 bytes of candidate. The cap does its job
    with room to spare, and this is the case stage 6's comment is about.
    """
    measured = _measure(_bulk_in_plain_fields, INGRESS_EVENT_BYTES, harness)
    record_property("headroom_bytes", measured.headroom_bytes)
    record_property("ingress_bytes", measured.ingress_bytes)
    record_property("egress_bytes", measured.egress_bytes)
    print(
        f"\nplain bulk: ingress {measured.ingress_bytes} B -> egress "
        f"{measured.egress_bytes} B, headroom {measured.headroom_bytes} B of "
        f"{MAX_EVENT_BYTES} B"
    )

    assert measured.ingress_bytes == INGRESS_EVENT_BYTES
    assert measured.published == 1
    assert measured.dlq == 0
    assert measured.egress_bytes < MAX_EVENT_BYTES


def test_the_cap_does_not_bound_an_event_whose_bulk_is_all_encrypted_payload(
    harness, record_property
) -> None:
    """The finding, pinned: base64 expansion, measured rather than assumed.

    Every byte of the filler goes into one of the five encrypted fields, so the
    egress event is `4/3 x ingress` plus a nonce and tag per field. The
    endpoints of the search are asserted first so a change in the encryption
    path cannot quietly make this test vacuous, and then the largest
    fully-encrypted event that does fit is measured.

    What is asserted is only the half that stays true under any change to the
    cap: the cap is not *tighter* than the measurement. The gap between the two
    is reported, not asserted, because the gap is a finding about the current
    constant rather than a property the code should be forced to preserve.
    """
    floor = _smallest(_bulk_in_encrypted_fields)
    assert _fits(_bulk_in_encrypted_fields, floor, harness), "nothing fits at all"
    assert not _fits(_bulk_in_encrypted_fields, INGRESS_EVENT_BYTES, harness), (
        "a cap-sized fully-encrypted event now fits, so the measurement below is "
        "measuring the wrong end of the range"
    )

    low, high = floor, INGRESS_EVENT_BYTES
    while low < high:
        mid = (low + high + 1) // 2
        if _fits(_bulk_in_encrypted_fields, mid, harness):
            low = mid
        else:
            high = mid - 1

    record_property("largest_encrypted_bulk_ingress_bytes", low)
    record_property("admitted_but_refused_bytes", INGRESS_EVENT_BYTES - low)
    print(
        f"\nencrypted bulk: the largest ingress event that still fits under "
        f"{MAX_EVENT_BYTES} B is {low} B of {INGRESS_EVENT_BYTES} B, so "
        f"{INGRESS_EVENT_BYTES - low} B of the cap is admitted and then refused"
    )

    assert low > 0, "nothing fits at all, so this measurement is measuring nothing"
    assert low <= INGRESS_EVENT_BYTES, (
        "INGRESS_EVENT_BYTES is now tighter than the measured ceiling allows. It "
        "was lowered; update the limits table in contracts/CONTRACT.md and the "
        "guidance in docs/handoff.md, because both publish the cap to clients."
    )
    # And the event at that size really does reach the topic.
    measured = _measure(_bulk_in_encrypted_fields, low, harness)
    assert measured.published == 1
    assert measured.egress_bytes <= MAX_EVENT_BYTES


def test_a_cap_sized_fully_encrypted_event_is_refused_per_event_to_the_dlq(
    harness, record_property
) -> None:
    """Where the events the cap over-admits land, and in what shape.

    Not a bug report -- a map. Stage 6 rejects them one at a time with
    `OVERSIZED`, publishes nothing for them, and the client sees them in the
    `rejected` list of its `202`. The `413` answers a breach of the ingress cap;
    this ceiling is answered per event, here.

    The size of the refused event is measured with the ceiling lifted rather than
    inferred, so the number printed here is the event's real length and not the
    0 that "nothing was published" would report.
    """
    measured = _measure(_bulk_in_encrypted_fields, INGRESS_EVENT_BYTES, harness)
    record_property("published", measured.published)
    record_property("dlq", measured.dlq)
    refused_bytes = _size_of_the_refused_event(
        _bulk_in_encrypted_fields, INGRESS_EVENT_BYTES, harness
    )
    record_property("refused_event_bytes", refused_bytes)
    print(
        f"\nencrypted bulk at the cap: ingress {measured.ingress_bytes} B produces a "
        f"{refused_bytes} B event, which is {refused_bytes - MAX_EVENT_BYTES} B over "
        f"the {MAX_EVENT_BYTES} B ceiling; published {measured.published}, "
        f"dlq {measured.dlq}"
    )

    assert refused_bytes > MAX_EVENT_BYTES, (
        f"the refused event measured {refused_bytes} B, which is not over the "
        f"{MAX_EVENT_BYTES} B ceiling, so the size limit did not fire and the "
        "DLQ assertion below would be testing something else"
    )
    assert measured.published == 0, (
        "the fully-encrypted cap-sized event was published, so the ceiling is no "
        "longer binding. contracts/CONTRACT.md's limits table and docs/handoff.md "
        "both state that it is."
    )
    assert measured.accepted == 0
    assert measured.dlq == 1

    record = msgspec.json.decode(measured.dlq_value, type=dict)
    context = record["data"]["error_context"]
    assert record["source"] == SOURCE
    assert context["stage"] == "encrypt"
    assert context["reason"].startswith("OVERSIZED")
    # The encrypt stage already ran, so the DLQ holds the ENCRYPTED event. The
    # plaintext candidate block must not be in there -- that is the other half
    # of CONTRACT.md section 4, and it is what makes this record replayable.
    original = record["data"]["original_payload"]
    assert "email" not in original["data"]["candidate"]
    assert original["data"]["candidate"]["email_enc"]
