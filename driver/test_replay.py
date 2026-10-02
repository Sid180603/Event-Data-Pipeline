"""T8d: the replay client -- the entry point the demo drives load through.

Everything else in the driver generates traffic; this module is what PUTS it on
the wire, and it is the only place in the system that speaks the **ingress**
shape (`contracts/ingress.py`: plaintext PII, `data.candidate.user_id`). A
post-encryption event is refused with `SCHEMA at $.data.candidate`, which is the
way the first version of the driver could not drive load at all.

The properties that matter, and the reason each is a test rather than a comment:

* **Caps respected before the send.** 500 events and 4 MiB are the gateway's
  ceilings (`app/config.py`) and a request over either is a whole-batch `413`
  with no per-event verdicts, so the batcher cuts the batch rather than learning
  about it.
* **One tenant per request.** A mixed-tenant batch is a `403`; the corpus already
  buffers per tenant and the batcher refuses a batch that is not homogeneous.
* **A stable, single-tenant token.** The driver holds the PRIVATE half of the
  signing key -- the gateway only ever sees the public one, so this is the only
  place in the system that can mint a token naming a tenant at all. The token is
  judged by the gateway's own `TokenVerifier`, not by a decode in this test.
* **The body is encoded once and reused verbatim.** CONTRACT.md section 5 makes
  `id` stable across retries the client's responsibility, and a retried batch
  that regenerated its events would resend a different `id` and become a
  duplicate instead of a retry.
* **Retries are bounded and identical.** A 429 denies the whole batch and a 503
  means it was never buffered, so both are resent -- same bytes, same ids, after
  `Retry-After` -- and an exhausted batch is counted rather than dropped silently.
* **Keep-alive is one client for the whole run**, never one per batch: a TLS
  handshake per request is fatal at this request rate and gets misdiagnosed as a
  gateway problem.
* **The ledger records every event that went out**, including the ones the
  gateway rejected on purpose, so reconciliation can tell "refused" from "lost".
* **No `application-abandoned`.** The Queue team's Flink job synthesises it on
  the watermark timeout; a driver that emitted it too would double-count drop-off.
"""

from __future__ import annotations

import copy
import datetime as dt
import http.server
import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import jwt
import msgspec
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient

from app.auth import TokenVerifier
from app.auth.errors import Unauthorized
from app.auth.jwt import CREDENTIAL_CLAIM, TENANT_CLAIM
from app.config import MAX_BATCH_BYTES, MAX_EVENTS_PER_BATCH, TOPIC_RAW, Settings
from app.kafka.producer import FakeSink
from app.main import create_app, credential_id_for, credentials_from_env
from app.metrics import Metrics
from app.ratelimit.registry import TenantLimits
from app.validate.events import CODE_DUPLICATE_ID
from contracts.attributes import career_site_id_from_source
from contracts.cloudevent import EVENT_TYPES
from contracts.ingress import decode_ingress
from contracts.ledger import Ledger
from driver.corpus import CorpusBuilder
from driver.inject import VARIANT_CODES
from driver.main import ConfiguredCatalog, main, tenants_from_env
from driver.replay import (
    BATCH_CONTENT_TYPE,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_EVENTS_PER_BATCH,
    DEFAULT_RETRY_AFTER_SECONDS,
    MAX_RETRY_AFTER_SECONDS,
    REASON_MALFORMED_202,
    REASON_TRANSPORT_ERROR,
    ReplayDriver,
    TokenMinter,
    plan_batches,
)
from driver.tenants import TenantCatalog

TENANTS = [f"tenant_{i:04d}" for i in range(1, 4)]


# --- helpers ------------------------------------------------------------------


def _corpus(**kw) -> CorpusBuilder:
    kw.setdefault("career_site_ids", TENANTS)
    kw.setdefault("seed", 7)
    kw.setdefault("users_per_tenant", 10)
    return CorpusBuilder(**kw)


def _one_batch(sessions: int = 40) -> list[dict]:
    return next(_corpus().batches(sessions, max_events=10_000))


def _events(count: int) -> list[dict]:
    """The first `count` events of one corpus batch, so a test can state an exact
    size. A session is 3-6 events, so counting SESSIONS does not count events."""
    return _one_batch(40)[:count]


def _padded(event: dict, size: int) -> dict:
    """A copy of `event` whose encoding is at least `size` bytes."""
    big = copy.deepcopy(event)
    name = big["data"]["candidate"]["name"]
    deficit = size - len(msgspec.json.encode(big))
    if deficit > 0:
        # ASCII appends one JSON byte per character, so the deficit is exact rather
        # than a guess a re-encode then has to check.
        big["data"]["candidate"]["name"] = name + "N" * deficit
    assert len(msgspec.json.encode(big)) >= size
    return big


# =============================================================================
# 1. the batcher respects the gateway's ceilings
# =============================================================================


def test_a_planned_batch_never_exceeds_the_gateway_event_cap():
    """500 is the contract cap and 413 is per batch with no per-event verdicts, so
    a request over it loses the whole request rather than one event."""
    for batch in _corpus().batches(200, max_events=10_000):
        for planned in plan_batches(batch, max_events=DEFAULT_MAX_EVENTS_PER_BATCH):
            assert planned.size <= min(DEFAULT_MAX_EVENTS_PER_BATCH, MAX_EVENTS_PER_BATCH)


def test_the_default_operating_point_is_below_the_ceilings():
    """CONTRACT.md section 2 recommends <= 200 events / <= 2 MiB as a client
    operating point, and the driver holds to the recommendation rather than
    driving at the ceiling."""
    assert DEFAULT_MAX_EVENTS_PER_BATCH < MAX_EVENTS_PER_BATCH
    assert DEFAULT_MAX_BATCH_BYTES < MAX_BATCH_BYTES


def test_a_caller_asking_for_more_than_the_ceiling_is_clamped_not_obeyed():
    """The cap is a property of the gateway, not of the caller's mood: a
    mis-parameterised `--max-events 5000` must not produce 413s for the whole run."""
    batch = _one_batch()
    planned = list(plan_batches(batch, max_events=MAX_EVENTS_PER_BATCH * 10))
    assert all(p.size <= MAX_EVENTS_PER_BATCH for p in planned)


def test_a_planned_batch_body_never_exceeds_the_gateway_byte_cap():
    """Padded events, because real corpus events are ~1 KB and 4 MiB is 4,000 of
    them: without padding this test could not fail."""
    batch = [_padded(ev, 40_000) for ev in _events(6)]
    for planned in plan_batches(batch, max_bytes=200_000):
        assert len(planned.body) <= 200_000
        assert planned.size > 1


def test_the_byte_cap_is_measured_on_the_encoded_body_not_on_the_event_count():
    """The body is what the gateway measures, and the brackets and commas are
    part of it -- so a batch that fits the events but not the punctuation would
    be the one that 413s."""
    planned = list(plan_batches([_padded(ev, 9_000) for ev in _events(4)], max_bytes=20_000))
    assert sum(p.size for p in planned) == 4
    assert all(len(p.body) <= 20_000 for p in planned)


def test_an_event_that_cannot_fit_a_batch_alone_is_refused_rather_than_sent():
    """Sending it would earn a 413 for the whole request, and the honest failure
    is at planning time with the size in the message."""
    fat = _padded(_events(1)[0], 50_000)
    with pytest.raises(ValueError, match="cannot fit"):
        list(plan_batches([fat], max_bytes=10_000))


def test_every_event_of_a_batch_lands_in_exactly_one_planned_batch():
    """A dropped event is invisible in the report -- the ledger row would be
    missing, not wrong -- so the partition is asserted rather than assumed."""
    for batch in _corpus().batches(120, max_events=500):
        planned = list(plan_batches(batch, max_events=25))
        assert [ev["id"] for p in planned for ev in p.events] == [ev["id"] for ev in batch]


# =============================================================================
# 2. one tenant per request, and the deliberate not-one-channel
# =============================================================================


def test_a_planned_batch_never_spans_two_tenants():
    for batch in _corpus().batches(200, max_events=300):
        for planned in plan_batches(batch):
            assert len({ev["source"] for ev in planned.events}) == 1
            assert planned.tenant == career_site_id_from_source(planned.events[0]["source"])


def test_a_multi_tenant_batch_is_refused_before_anything_is_sent():
    """`CorpusBuilder.batches` guarantees homogeneity, so this can only fire on a
    bug or a hand-built stream -- and it has to be a loud one, because the
    gateway's answer would be a 403 on every request in the run."""
    mixed = [next(_corpus(career_site_ids=[t]).batches(2, max_events=10))[0] for t in TENANTS[:2]]
    with pytest.raises(ValueError, match="tenant"):
        list(plan_batches(mixed))


def test_requests_are_not_split_by_channel():
    """The deliberate trade, pinned so a later 'fix' has to argue with it.

    A request carries one credential and the credential fixes `sourcechannel` for
    the whole request (`app.auth.authorize_batch`), so a per-channel split is the
    only way to keep every event's own channel label. It is refused here because
    the corpus gives every user a session on every channel, so a per-channel split
    delivers each user's WEB_APP events, then their MOBILE_APP events, then their
    third-party ones -- reordering a journey that CONTRACT.md section 6 promises to
    produce in order, and that the gateway counts as a violation.
    `test_the_planned_requests_keep_a_users_sequence_in_order` is the other half
    of this argument."""
    batch = _one_batch(200)
    assert {ev["sourcechannel"] for ev in batch} == {
        "WEB_APP",
        "MOBILE_APP",
        "THIRD_PARTY_SERVICE",
    }
    # The ceilings as the only cuts, so the batch count is decided by the caps and
    # not by this test's own arithmetic.
    planned = list(
        plan_batches(batch, max_events=MAX_EVENTS_PER_BATCH, max_bytes=MAX_BATCH_BYTES)
    )
    assert len(planned) == 1, "the driver split a batch by channel"
    assert planned[0].channel == batch[0]["sourcechannel"]


def test_a_planned_batch_names_the_channel_its_credential_is_registered_for():
    """The request header and the credential have to agree or the gateway answers
    403 (`_check_source_type_hint`), so the label the driver sends is the one it
    holds a token for -- not a channel read out of the events."""
    for batch in _corpus(seed=11).batches(300, max_events=500):
        for planned in plan_batches(batch):
            headers = planned.headers(f"token-for-{planned.tenant}-{planned.channel}")
            assert headers["x-source-type"] == planned.channel
            assert planned.channel in {"WEB_APP", "MOBILE_APP", "THIRD_PARTY_SERVICE"}


def test_the_planned_requests_keep_a_users_sequence_in_order():
    """Cutting a batch into several requests is only safe if the cut cannot
    interleave one user's journey, and the order that matters is the order the
    requests go out in. The cap cut is a contiguous run of the stream, so it cannot;
    and there is deliberately no channel cut -- see
    `test_requests_are_not_split_by_channel`, which is the other half of this
    argument.

    Scoped to one tenant because the corpus flushes each tenant's tail only at the
    end (`CorpusBuilder.batches`), so tenant B's last events are delivered after
    tenant A's -- which is why the cap is per request rather than global."""
    tenant = TENANTS[0]
    delivered: dict[str, list[int]] = {}
    for batch in _corpus(career_site_ids=[tenant]).batches(300, max_events=10_000):
        for planned in plan_batches(batch, max_events=17):
            for ev in planned.events:
                delivered.setdefault(ev["data"]["candidate"]["user_id"], []).append(
                    int(ev["sequence"])
                )
    journeys = [seqs for seqs in delivered.values() if len(seqs) > 1]
    assert len(journeys) >= 5, f"expected repeated users, got {len(journeys)}"
    assert all(len(seqs) > 10 for seqs in journeys), "expected full journeys, not pairs"
    for seqs in journeys:
        assert seqs == sorted(seqs), "the batcher reordered one user's events"


# =============================================================================
# 3. the body is the ingress shape, encoded once
# =============================================================================


def test_the_planned_body_decodes_as_ingress_not_as_the_published_shape():
    """The two directions are different structs on purpose. A body carrying
    `user_id_pseudo` or `*_enc` is refused with `SCHEMA at $.data.candidate`,
    which is exactly how the driver first became unable to drive load."""
    for planned in plan_batches(_one_batch()):
        decoded = decode_ingress(planned.body)
        assert len(decoded) == planned.size
        assert all(ev.data.candidate.user_id.startswith("usr_") for ev in decoded)
        assert all(ev.data.candidate.email for ev in decoded)


def test_the_body_is_a_json_array_of_exactly_the_planned_events():
    for planned in plan_batches(_one_batch()):
        assert msgspec.json.decode(planned.body, type=list) == list(planned.events)


def test_planning_the_same_batch_twice_produces_the_same_bytes():
    """A batch is a value, not a stream: nothing in planning may advance state,
    or a retried run would send different bytes for the same events."""
    batch = _one_batch()
    first = [p.body for p in plan_batches(batch)]
    second = [p.body for p in plan_batches(batch)]
    assert first == second


def test_an_empty_batch_plans_to_no_request():
    """A zero-event POST is a 400-shaped waste; the corpus's last flush is the
    only place one could appear."""
    assert list(plan_batches([])) == []


def test_a_caller_asking_for_no_events_per_batch_is_refused():
    with pytest.raises(ValueError):
        list(plan_batches(_one_batch(2), max_events=0))


# =============================================================================
# 4. the token: the private half lives here
# =============================================================================

AUDIENCE = "career-api"
#: Real "now", captured once: a hand-written epoch would be a landmine, because
#: pyjwt checks `iat` and `exp` against the wall clock even when the minter's own
#: clock is injected.
NOW = time.time()


@pytest.fixture(scope="module")
def keypair() -> tuple[bytes, str]:
    """`(private PEM, public PEM)` -- the pair the compose file writes to
    `driver-signing-key.pem` and to `JWT_PUBLIC_KEY_PEM` respectively."""
    key = Ed25519PrivateKey.generate()
    private = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    public = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return private, public.decode()


@pytest.fixture
def minter(keypair) -> TokenMinter:
    return TokenMinter(keypair[0], audience=AUDIENCE, clock=lambda: NOW)


def _verifier(public_pem: str) -> TokenVerifier:
    return TokenVerifier(public_pem, "EdDSA", AUDIENCE)


def test_a_minted_token_is_accepted_by_the_gateways_own_verifier(minter, keypair):
    """Not a decode in this file: the token is judged by the code that will judge
    it in production, against the public half the gateway is given. A token the
    driver likes and the gateway does not is a driver that cannot drive load."""
    verified = _verifier(keypair[1]).verify(
        f"Bearer {minter.token(TENANTS[0], 'WEB_APP')}"
    )
    assert verified.career_site_id == TENANTS[0]
    assert verified.credential_id == credential_id_for(TENANTS[0], "WEB_APP")


def test_the_driver_holds_the_private_half_and_the_gateway_only_the_public_one(
    minter, keypair
):
    """The split the compose file is built around (D4/H2): a gateway that could
    mint a token could mint one for any of the 500 tenants."""
    with pytest.raises(ValueError):
        TokenVerifier(keypair[0].decode(), "EdDSA", AUDIENCE)
    assert _verifier(keypair[1]).verify(f"Bearer {minter.token(TENANTS[0], 'WEB_APP')}")


def test_each_channel_gets_its_own_credential_because_the_token_asserts_one(
    minter, keypair
):
    """`credential_id` is a signed claim and it is what the gateway maps to a
    channel, so one token cannot cover two channels -- and pairing a tenant with
    another tenant's credential is a 403 (`authorize_batch`)."""
    verifier = _verifier(keypair[1])
    web = verifier.verify(f"Bearer {minter.token(TENANTS[0], 'WEB_APP')}")
    mobile = verifier.verify(f"Bearer {minter.token(TENANTS[0], 'MOBILE_APP')}")
    assert web.credential_id != mobile.credential_id
    assert web.career_site_id == mobile.career_site_id == TENANTS[0]


def test_a_token_carries_the_audience_and_an_expiry_the_verifier_requires(minter):
    """The gateway's decode requires `exp` and `aud` (`options={"require": ...}`),
    so a token without them is a 401 no matter how well it is signed."""
    claims = jwt.decode(
        minter.token(TENANTS[0], "WEB_APP"),
        options={"verify_signature": False},
        audience=AUDIENCE,
    )
    assert claims["aud"] == AUDIENCE
    assert claims[TENANT_CLAIM] == TENANTS[0]
    assert claims[CREDENTIAL_CLAIM] == credential_id_for(TENANTS[0], "WEB_APP")
    assert dt.datetime.fromtimestamp(claims["exp"], dt.timezone.utc) > dt.datetime.fromtimestamp(
        NOW, dt.timezone.utc
    )


def test_one_signature_per_credential_not_one_per_batch(keypair, monkeypatch):
    """A 5M-event run is ~25,000 requests over 500 tenants, so signing per request
    would be tens of thousands of EdDSA operations doing nothing. Counted on
    `jwt.encode` because the signature is the cost being avoided."""
    signed: list[int] = []
    real = jwt.encode
    monkeypatch.setattr(jwt, "encode", lambda *a, **kw: (signed.append(1), real(*a, **kw))[1])
    minter = TokenMinter(keypair[0], audience=AUDIENCE, clock=lambda: NOW)
    tokens = [minter.token(TENANTS[0], "WEB_APP") for _ in range(50)]
    assert len(signed) == 1
    assert len(set(tokens)) == 1
    assert minter.token(TENANTS[1], "WEB_APP") != tokens[0], "a per-tenant token is required"


def test_a_token_is_re_minted_before_it_expires_rather_than_after(keypair):
    """A 401 storm halfway through a long run would look like a gateway fault.
    The margin is what makes the re-mint proactive; without it a token is used for
    the last seconds of its life and the run dies at the boundary."""
    now = [NOW]
    minter = TokenMinter(
        keypair[0],
        audience=AUDIENCE,
        ttl_seconds=600,
        refresh_margin=60,
        clock=lambda: now[0],
    )
    first = minter.token(TENANTS[0], "WEB_APP")
    now[0] = NOW + 539
    assert minter.token(TENANTS[0], "WEB_APP") == first, "re-minted too early"
    now[0] = NOW + 541
    assert minter.token(TENANTS[0], "WEB_APP") != first, "used past the refresh margin"


def test_a_shared_secret_algorithm_is_refused_because_it_would_mint_for_every_tenant(
    keypair,
):
    """HS256 is the exact r3.1 mistake `app/auth/jwt.py` documents: a shared secret
    lets its holder mint a token for any tenant, so one leak is total. The driver
    signs for all 500, so it would inherit that mistake in full."""
    with pytest.raises(ValueError, match="HS256"):
        TokenMinter(keypair[0], audience=AUDIENCE, algorithm="HS256")


def test_a_key_that_is_not_a_key_is_refused_at_construction(keypair):
    with pytest.raises(ValueError):
        TokenMinter(b"not a pem", audience=AUDIENCE)
    with pytest.raises(ValueError):
        TokenMinter(keypair[1], audience=AUDIENCE)  # the public half cannot sign


def test_a_token_for_an_unknown_audience_is_refused_by_the_gateway(keypair):
    """Both halves of the contract in one check: the driver signs the audience the
    gateway wants and no other, and the gateway's refusal is an `Unauthorized`
    (401) rather than anything that would let a prober tell which check failed."""
    signer = TokenMinter(keypair[0], audience="some-other-api", clock=lambda: NOW)
    with pytest.raises(Unauthorized):
        _verifier(keypair[1]).verify(f"Bearer {signer.token(TENANTS[0], 'WEB_APP')}")


# =============================================================================
# 5. the driver: one client, one request per planned batch
# =============================================================================


@dataclass(frozen=True, slots=True)
class Call:
    url: str
    body: bytes
    headers: dict[str, str]

    @property
    def ids(self) -> list[str]:
        return [ev["id"] for ev in msgspec.json.decode(self.body, type=list)]


class FakeResponse:
    """Only what the driver reads: a status, headers, and a JSON body."""

    def __init__(self, status_code: int, payload: object, headers: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> object:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class ScriptedClient:
    """A stand-in for `httpx.Client` that records posts and replays a script.

    The LAST entry repeats, so "the gateway is refusing everything" is one entry
    rather than a comprehension. A script entry may be a callable taking the
    batch's event count, which is how a 202 echoes the size it was given -- a
    fixed number would make every batch look unaccounted for. `close()` exists so
    a test can prove the driver does not call it: the driver is handed a client,
    it does not own it.
    """

    def __init__(self, script) -> None:
        self.script = list(script)
        self.calls: list[Call] = []
        self.closed = False

    def post(self, url, *, content, headers) -> FakeResponse:
        call = Call(url, content, headers)
        self.calls.append(call)
        outcome = self.script[min(len(self.calls) - 1, len(self.script) - 1)]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome(len(call.ids)) if callable(outcome) else outcome

    def close(self) -> None:
        self.closed = True

    @property
    def attempts(self) -> int:
        return len(self.calls)


def _accept(accepted: int, rejected: int = 0) -> FakeResponse:
    return FakeResponse(
        202,
        {"accepted": accepted, "rejected": [{"index": 0, "reason": "SCHEMA"}] * rejected},
    )


def _ok(rejected: int = 0):
    """A 202 that accounts for exactly the events it was sent -- the honest run."""

    def respond(count: int) -> FakeResponse:
        return _accept(count - rejected, rejected)

    return respond


def _refuse(status: int, reason: str, retry_after: int | None = None) -> FakeResponse:
    headers = {"Retry-After": str(retry_after)} if retry_after is not None else {}
    return FakeResponse(status, {"reason": reason}, headers)


def _driver(client, minter, tmp_path, **kw) -> ReplayDriver:
    return ReplayDriver(
        url="http://gateway:8000",
        signer=minter,
        client=client,
        ledger_path=tmp_path / "ledger.jsonl",
        sleep=kw.pop("sleep", lambda _seconds: None),
        **kw,
    )


def _corpus_batches(sessions: int = 60, max_events: int = 500) -> list[list[dict]]:
    return list(_corpus().batches(sessions, max_events=max_events))


def _expected_events(batches) -> int:
    return sum(len(batch) for batch in batches)


def test_a_clean_run_sends_every_event_once_and_the_gateway_accepts_it(
    minter, tmp_path
):
    batches = _corpus_batches()
    client = ScriptedClient([_ok()])
    report = _driver(client, minter, tmp_path).run(batches)
    # The scripted client cannot know each batch's size, so the ledger is what
    # proves the run: one row per event, and the summary agrees with it.
    records = Ledger(tmp_path / "ledger.jsonl").read_all()
    assert len(records) == _expected_events(batches) == report.sent
    assert report.clean and report.batches == client.attempts


def test_every_planned_batch_becomes_exactly_one_request(minter, tmp_path):
    batches = _corpus_batches()
    client = ScriptedClient([_ok()])
    _driver(client, minter, tmp_path).run(batches)
    planned = [p for b in batches for p in plan_batches(b)]
    assert client.attempts == len(planned) == len({c.body for c in client.calls})


def test_the_run_uses_one_url_and_one_client_and_never_closes_it(minter, tmp_path):
    """Keep-alive is the client's connection pool, so the driver is handed the
    client rather than building one: a client per batch would be a TCP handshake
    per request, which is fatal at this request rate and is misdiagnosed as a
    gateway problem. The real socket count is measured in the CLI section. The
    driver is also not the owner, so it does not close what it was handed."""
    client = ScriptedClient([_ok()])
    driver = _driver(client, minter, tmp_path)
    driver.run(_corpus_batches())
    assert driver.client is client
    assert not client.closed
    assert {c.url for c in client.calls} == {"http://gateway:8000/v1/ingest"}


def test_every_request_carries_the_three_contract_headers(minter, tmp_path):
    client = ScriptedClient([_ok()])
    _driver(client, minter, tmp_path).run(_corpus_batches(20))
    for call in client.calls:
        assert call.headers["content-type"] == BATCH_CONTENT_TYPE
        assert call.headers["authorization"].startswith("Bearer ey")
        assert call.headers["x-source-type"] in {"WEB_APP", "MOBILE_APP", "THIRD_PARTY_SERVICE"}


# =============================================================================
# 6. the ledger is the ground truth for what went out
# =============================================================================


def test_the_ledger_holds_one_row_per_event_in_the_t2_schema(minter, tmp_path):
    batches = _corpus_batches()
    _driver(ScriptedClient([_ok()]), minter, tmp_path).run(batches)
    records = Ledger(tmp_path / "ledger.jsonl").read_all()
    assert len(records) == _expected_events(batches)
    assert {r.tenant for r in records} == set(TENANTS)
    assert all(r.source == f"/careers/{r.tenant}" for r in records)
    assert all(r.id and r.type.startswith("com.careerpage.career.") for r in records)
    assert all(r.user_pseudo.startswith("usr_") for r in records)
    assert all(isinstance(r.seq, int) for r in records)


def test_the_ledger_covers_the_corpus_exactly(minter, tmp_path):
    """Same ids, same order, same count: the ledger is the driver's own account of
    the run, and a reconciliation that cannot match it proves nothing."""
    batches = _corpus_batches()
    _driver(ScriptedClient([_ok()]), minter, tmp_path).run(batches)
    records = Ledger(tmp_path / "ledger.jsonl").read_all()
    assert [(r.id, r.source) for r in records] == [
        (ev["id"], ev["source"]) for b in batches for ev in b
    ]


def test_events_the_gateway_rejected_are_still_in_the_ledger(minter, tmp_path):
    """The whole point of the receipt: "the gateway refused this on purpose" has to
    be distinguishable from "this never arrived". So the rejected events are in the
    ledger and counted as rejected, and `unaccepted` stays zero -- a run that shed
    events on purpose is still a run that balances."""
    batches = _corpus_batches()
    report = _driver(ScriptedClient([_ok(rejected=3)]), minter, tmp_path).run(batches)
    records = Ledger(tmp_path / "ledger.jsonl").read_all()
    assert len(records) == report.sent == _expected_events(batches)
    assert report.rejected == 3 * report.batches
    assert report.unaccepted == 0 and report.clean


def test_a_202_with_a_body_the_driver_cannot_read_is_not_counted_as_an_acceptance(
    minter, tmp_path
):
    """Something is answering on the gateway's behalf. Reading zero out of it would
    let `sent == accepted` balance against a body nobody looked at."""
    report = _driver(ScriptedClient([FakeResponse(202, [])]), minter, tmp_path).run(
        [_events(3)]
    )
    assert report.by_status == {REASON_MALFORMED_202: 1}
    assert report.accepted == 0
    assert report.unaccepted == 3
    assert not report.clean


def test_a_202_whose_body_is_not_json_does_not_kill_the_run(minter, tmp_path):
    """The rest of the run still has to be reported: one unreadable body from
    something in front of the gateway must not take the receipt down with it."""
    client = ScriptedClient([FakeResponse(202, ValueError("not json")), _ok()])
    report = _driver(client, minter, tmp_path).run([_events(2), _events(2)])
    assert report.by_status == {REASON_MALFORMED_202: 1}
    assert report.sent == 4 and report.accepted == 2


def test_a_second_run_reports_the_second_run_and_not_the_sum_of_both(minter, tmp_path):
    """Re-running is a normal thing to do after a fix, and a report that
    accumulated would overstate every count in it."""
    driver = _driver(ScriptedClient([_ok()]), minter, tmp_path)
    first = driver.run([_events(4)]).to_dict()
    second = driver.run([_events(4)]).to_dict()
    assert first == second
    assert len(Ledger(tmp_path / "ledger.jsonl").read_all()) == second["sent"]


def test_a_batch_that_was_never_accepted_is_still_in_the_ledger(minter, tmp_path):
    """Sent is sent. A 429'd batch is not lost, it is deferred -- and the ledger is
    what makes that a shortfall reconciliation can see rather than a silent hole."""
    batches = _corpus_batches(20)
    client = ScriptedClient([_refuse(429, "RATE_LIMITED", retry_after=1)])
    report = _driver(client, minter, tmp_path).run(batches)
    records = Ledger(tmp_path / "ledger.jsonl").read_all()
    assert len(records) == _expected_events(batches) == report.sent
    assert report.unaccepted == report.sent
    assert not report.clean


def test_a_clean_run_leaves_no_duplicate_source_id_pairs(minter, tmp_path):
    """`Ledger.duplicates` is the check `tools/verify.py` will run, so a clean run
    has to be clean under it -- and it would not be if a retry re-encoded."""
    _driver(ScriptedClient([_ok()]), minter, tmp_path).run(_corpus_batches())
    assert Ledger.duplicates(Ledger(tmp_path / "ledger.jsonl").read_all()) == 0


def test_replay_never_emits_application_abandoned(minter, tmp_path):
    """The Queue team's Flink job synthesises it on the watermark timeout
    (SPEC.txt:338-339); emitting it here too would double-count drop-off."""
    _driver(ScriptedClient([_ok()]), minter, tmp_path).run(_corpus_batches(300))
    types = {r.type for r in Ledger(tmp_path / "ledger.jsonl").read_all()}
    assert not any(t.endswith("application-abandoned") for t in types)
    assert len(types) >= 4, "expected the whole funnel, not one event type"


# =============================================================================
# 7. retries: same bytes, bounded, never silent
# =============================================================================


def test_a_rate_limited_batch_is_resent_with_byte_identical_events(minter, tmp_path):
    """The 429 case that matters: the whole batch was denied, so it goes back
    whole. Same bytes means the same `id`s, and `(source, id)` dedup makes the
    resend free of duplicates. Splitting the batch to get under the limit would
    break that and would not help -- the limiter is per tenant, not per batch."""
    client = ScriptedClient([_refuse(429, "RATE_LIMITED", retry_after=1), _ok()])
    report = _driver(client, minter, tmp_path).run([_events(6)])
    assert client.attempts == 2
    assert client.calls[0].body == client.calls[1].body
    assert client.calls[0].ids == client.calls[1].ids
    assert report.sent == 6, "a retried batch must be counted once, not twice"
    assert report.rate_limited == 1 and report.retried == 1


def test_a_rate_limited_batch_waits_for_the_gateways_retry_after(minter, tmp_path):
    slept: list[float] = []
    client = ScriptedClient([_refuse(429, "RATE_LIMITED", retry_after=3), _ok()])
    _driver(client, minter, tmp_path, sleep=slept.append).run([_events(4)])
    assert slept == [3.0]


def test_a_retry_after_the_driver_cannot_wait_out_is_clamped(minter, tmp_path):
    """A driver that obeys a 600-second Retry-After is a driver the demo cannot
    interrupt, so the wait is bounded and the clamp is visible in the summary's
    exhaustion rather than silent."""
    slept: list[float] = []
    client = ScriptedClient([_refuse(429, "RATE_LIMITED", retry_after=86_400)])
    report = _driver(client, minter, tmp_path, sleep=slept.append, max_attempts=2).run(
        [_events(2)]
    )
    assert slept == [float(MAX_RETRY_AFTER_SECONDS)]
    assert report.by_status == {"RATE_LIMITED": 1}


def test_a_retry_after_that_is_not_a_number_falls_back_to_the_floor(minter, tmp_path):
    slept: list[float] = []
    client = ScriptedClient(
        [FakeResponse(503, {"reason": "SINK_UNAVAILABLE"}, {"Retry-After": "soon"}), _ok()]
    )
    _driver(client, minter, tmp_path, sleep=slept.append).run([_events(2)])
    assert slept == [float(DEFAULT_RETRY_AFTER_SECONDS)]


def test_the_last_attempt_does_not_sleep_before_giving_up(minter, tmp_path):
    """Otherwise a run that is being refused waits out a full Retry-After after
    the attempt that was going to be refused anyway."""
    slept: list[float] = []
    client = ScriptedClient([_refuse(503, "SINK_UNAVAILABLE", retry_after=2)])
    _driver(client, minter, tmp_path, sleep=slept.append, max_attempts=3).run([_events(2)])
    assert slept == [2.0, 2.0]


def test_a_503_is_resent_because_accepted_into_a_buffer_is_not_durability(
    minter, tmp_path
):
    """`202` is an in-memory buffer, not Kafka (C4), so a 503 means these events
    never made it anywhere and genuinely have to go out again."""
    client = ScriptedClient([_refuse(503, "SINK_UNAVAILABLE", retry_after=1), _ok()])
    report = _driver(client, minter, tmp_path).run([_events(3)])
    assert client.calls[0].body == client.calls[1].body
    assert report.sink_unavailable == 1 and report.sent == 3


def test_retries_are_bounded_and_exhaustion_is_visible_in_the_report(minter, tmp_path):
    """Never silent: a refused batch leaves the run with `clean == False` and a
    non-zero exit, because `sent == accepted` is exactly the claim that no longer
    holds for it."""
    client = ScriptedClient([_refuse(429, "RATE_LIMITED", retry_after=0)])
    report = _driver(client, minter, tmp_path).run([_events(5)])
    assert client.attempts == DEFAULT_MAX_ATTEMPTS
    assert report.by_status == {"RATE_LIMITED": 1}
    assert report.rate_limited == DEFAULT_MAX_ATTEMPTS
    assert report.unaccepted == 5
    assert not report.clean


def test_a_503_that_never_clears_is_bounded_too(minter, tmp_path):
    client = ScriptedClient([_refuse(503, "SINK_UNAVAILABLE", retry_after=0)])
    report = _driver(client, minter, tmp_path, max_attempts=2).run([_events(5)])
    assert client.attempts == 2
    assert report.by_status == {"SINK_UNAVAILABLE": 1}


def test_a_connection_reset_is_resent_with_the_same_ids(minter, tmp_path):
    """CONTRACT.md section 3 requires it: a gateway killed mid-flight resets the
    connection, and the resend is safe only because the ids are the same ones."""
    client = ScriptedClient([httpx.ConnectError("peer reset"), _ok()])
    report = _driver(client, minter, tmp_path).run([_events(4)])
    assert client.attempts == 2
    assert client.calls[0].ids == client.calls[1].ids
    assert report.retried == 1 and report.sent == 4


def test_a_connection_reset_that_never_clears_is_bounded_and_named(minter, tmp_path):
    client = ScriptedClient([httpx.ConnectError("refused")])
    report = _driver(client, minter, tmp_path, max_attempts=2).run([_events(2)])
    assert client.attempts == 2
    assert report.by_status == {REASON_TRANSPORT_ERROR: 1}
    assert report.unaccepted == 2


@pytest.mark.parametrize("status,reason", [(400, "BODY_NOT_A_BATCH"), (401, "UNAUTHORIZED"),
                                          (403, "FORBIDDEN"), (413, "TOO_MANY_EVENTS")])
def test_a_refusal_that_retrying_cannot_fix_is_not_retried(minter, tmp_path, status, reason):
    """A 403 is a tenancy failure, a 413 is a cap: resending the identical bytes
    would earn the identical answer, and the retry budget belongs to the failures
    where a resend can actually help."""
    client = ScriptedClient([_refuse(status, reason)])
    report = _driver(client, minter, tmp_path).run([_events(3)])
    assert client.attempts == 1
    assert report.by_status == {reason: 1}
    assert report.unaccepted == 3


def test_the_failure_reason_is_the_gateways_own_code_not_its_status(minter, tmp_path):
    """`503` covers two different faults -- the producer buffer and the DLQ sink --
    and the operator needs to tell them apart, so the key is the reason code the
    gateway put in the body."""
    client = ScriptedClient([_refuse(503, "DLQ_UNAVAILABLE", retry_after=1)],)
    report = _driver(client, minter, tmp_path, max_attempts=1).run([_events(2)])
    assert report.by_status == {"DLQ_UNAVAILABLE": 1}


def test_a_failure_body_that_is_not_json_falls_back_to_the_status(minter, tmp_path):
    """A proxy in front of the gateway can answer with HTML. The driver must still
    say something specific enough to act on."""
    client = ScriptedClient([FakeResponse(502, ValueError("not json"))])
    report = _driver(client, minter, tmp_path).run([_events(2)])
    assert report.by_status == {"502": 1}


def test_the_report_serialises_the_fields_the_demo_asserts_on(minter, tmp_path):
    report = _driver(ScriptedClient([_ok(rejected=1)]), minter, tmp_path).run(
        _corpus_batches(20)
    )
    summary = report.to_dict()
    assert summary["sent"] == summary["accepted"] + summary["rejected"]
    assert summary["unaccepted"] == 0
    assert summary["clean"] is True
    assert set(summary) == {
        "batches",
        "sent",
        "accepted",
        "rejected",
        "retried",
        "rate_limited",
        "sink_unavailable",
        "by_status",
        "unaccepted",
        "clean",
    }


# =============================================================================
# 8. end to end through the real endpoint, with the FakeSink
# =============================================================================
#
# Nothing here is a stub of the gateway: a real FastAPI app, the real handler and
# pipeline, a real EdDSA token verified by the real `TokenVerifier`, and the real
# bearer/tenant binding. Only the broker is replaced (`FakeSink`), which is the
# substitution `app/test_app.py` already makes and the reason this file can prove
# the acceptance criterion without a cluster.

#: The gateway only reaches `MASTER_SECRET` and the operator key on the decrypt
#: path, which this run never touches; the values match `app/test_app.py` so a
#: reader comparing the two files sees the same harness.
DEV_MASTER = b"test-only-master-secret-do-not-use!"
OPERATOR_KEY = b"test-only-operator-key"


def _build_gateway(public_pem: str, **kwargs):
    """A real gateway with the driver's own tenant catalog registered."""
    return create_app(
        credentials=credentials_from_env({"GATEWAY_TENANTS": ",".join(TENANTS)}),
        settings=Settings(
            master_secret=DEV_MASTER,
            jwt_public_key_pem=public_pem,
            jwt_algorithm="EdDSA",
            jwt_audience=AUDIENCE,
        ),
        operator_key=OPERATOR_KEY,
        sink=FakeSink(),
        metrics=Metrics(worker_id="test-worker"),
        **kwargs,
    )


@pytest.fixture
def gateway(keypair):
    return _build_gateway(keypair[1])


@pytest.fixture
def ingest(gateway):
    """An `httpx.Client` bound to the app. `TestClient` IS one, so the driver
    cannot tell it from a network client -- which is the point."""
    with TestClient(gateway) as client:
        yield client


def test_a_full_driver_run_is_accepted_by_the_ingest_endpoint(ingest, gateway, minter, tmp_path):
    batches = _corpus_batches(120)
    sink = gateway.state.sink.inner
    report = _driver(ingest, minter, tmp_path).run(batches)

    assert report.clean and report.sent == report.accepted
    assert report.accepted == _expected_events(batches)
    assert len(sink.records) == report.accepted
    assert {topic for topic, _key, _value in sink.records} == {TOPIC_RAW}
    assert len(Ledger(tmp_path / "ledger.jsonl").read_all()) == report.sent


def test_the_gateway_verifies_one_token_per_request_and_not_one_per_event(
    ingest, gateway, minter, tmp_path
):
    """D2's claim, measured on the gateway's own counter. A client that forced a
    verification per event would be paying ~50,000 signature checks a second."""
    report = _driver(ingest, minter, tmp_path).run(_corpus_batches(60))
    assert gateway.state.verifier.verify_count == report.batches
    assert report.sent > report.batches


def test_the_driver_sends_plaintext_and_the_gateway_publishes_ciphertext(
    ingest, gateway, minter, tmp_path
):
    """The two schemas, proved in one run. The driver can only post
    `data.candidate.user_id` and `email`; what lands on the topic must carry
    `user_id_pseudo` and `email_enc` and no plaintext."""
    _driver(ingest, minter, tmp_path).run(_corpus_batches(20))
    published = msgspec.json.decode(gateway.state.sink.inner.records[0][2], type=dict)
    candidate = published["data"]["candidate"]
    assert "user_id" not in candidate and "email" not in candidate
    assert candidate["user_id_pseudo"] and candidate["email_enc"]
    assert published["type"] in EVENT_TYPES


def test_every_published_record_is_partitioned_under_the_token_s_tenant(
    ingest, gateway, minter, tmp_path
):
    """The key is `<career_site_id>|<user_id_pseudo>` and the tenant half came from
    the signed claim, so a request whose events belonged to another tenant could
    not have been accepted at all -- this is the observable half of that."""
    _driver(ingest, minter, tmp_path).run(_corpus_batches(60))
    records = gateway.state.sink.inner.records
    assert records
    for _topic, key, value in records:
        tenant, _, pseudo = key.partition("|")
        assert tenant in TENANTS and pseudo
        assert msgspec.json.decode(value, type=dict)["partitionkey"] == key


def test_a_shed_tenant_is_a_reported_shortfall_and_not_a_lost_run(keypair, minter, tmp_path):
    """The flood beat's receipt: the per-tenant limiter refuses the whole batch,
    the driver reports exactly which reason code, and every event is still in the
    ledger -- so the shortfall is visible instead of being a hole. `max_attempts=1`
    because the point is the first refusal, not the patience."""
    shed = _build_gateway(
        keypair[1], tenant_limits={TENANTS[0]: TenantLimits(rate=1.0, burst=1.0)}
    )
    batches = _corpus_batches(60)
    with TestClient(shed) as client:
        report = _driver(client, minter, tmp_path, max_attempts=1).run(batches)

    assert report.rate_limited > 0
    assert report.by_status.get("RATE_LIMITED")
    assert report.unaccepted > 0
    assert not report.clean
    assert len(Ledger(tmp_path / "ledger.jsonl").read_all()) == report.sent


# =============================================================================
# 9. the CLI, over a real socket
# =============================================================================
#
# A real TCP server on the loopback, speaking the 202 CONTRACT.md section 2
# specifies, because the two things the CLI has to get right cannot be seen
# through a transport double: httpx has to be willing to reuse ONE connection
# across the whole run, and the driver has to exit non-zero when the gateway kept
# refusing. `python -m driver.main` under compose is the same code path.


class _StubGateway(http.server.ThreadingHTTPServer):
    """A loopback `/v1/ingest` that accepts a batch and counts connections.

    `connections` is the keep-alive evidence: a driver that opened a client per
    batch would show one accepted socket per request, and the run would still
    "work", which is exactly why it needs a number rather than a green tick.
    """

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _StubHandler)
        self.requests: list[tuple[str, dict, bytes]] = []
        self.connections = 0
        self.status = 202

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host}:{port}"

    def get_request(self):
        self.connections += 1
        return super().get_request()


class _StubHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # persistent, which is the point

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length)
        server: _StubGateway = self.server  # type: ignore[assignment]
        server.requests.append((self.path, dict(self.headers), body))
        status = server.status
        if status == 202:
            payload = msgspec.json.encode(
                {"accepted": len(msgspec.json.decode(body, type=list)), "rejected": []}
            )
        else:
            payload = msgspec.json.encode({"reason": "RATE_LIMITED"})
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        if status != 202:
            self.send_header("Retry-After", "0")
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args) -> None:
        """Silent: the default handler logs every request to stderr, which would
        bury the pytest output this file's assertions live in."""


@pytest.fixture
def live_gateway():
    server = _StubGateway()
    # The default 0.5s poll interval would put half a second of dead time in the
    # teardown of every test that uses this.
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture(autouse=True)
def no_ambient_tenants(monkeypatch):
    """The CLI reads `GATEWAY_TENANTS`; a developer's shell must not decide what
    these tests replay."""
    monkeypatch.delenv("GATEWAY_TENANTS", raising=False)


@pytest.fixture
def signing_key(tmp_path, keypair) -> str:
    path = tmp_path / "driver-signing-key.pem"
    path.write_bytes(keypair[0])
    return str(path)


def test_a_full_cli_run_completes_and_reconciles(live_gateway, signing_key, tmp_path, capsys):
    assert main(
        [
            "--url", live_gateway.url,
            "--signing-key", signing_key,
            "--ledger", str(tmp_path / "ledger.jsonl"),
            "--sessions", "120", "--tenants", "3", "--max-events", "40",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["clean"] is True
    assert summary["sent"] == summary["accepted"] == summary["ledger_rows"]
    assert summary["unaccepted"] == 0
    assert summary["batches"] == len(live_gateway.requests) > 3
    assert summary["tenants"] == 3
    assert summary["url"] == live_gateway.url
    assert len(Ledger(tmp_path / "ledger.jsonl").read_all()) == summary["sent"]


def test_the_whole_run_reuses_one_connection(live_gateway, signing_key, tmp_path, capsys):
    """Keep-alive, measured. `docker-compose.yml` gives the driver its own cores
    precisely because the driver is the instrument; a handshake per request would
    make it report the network instead of the gateway."""
    main(
        [
            "--url", live_gateway.url,
            "--signing-key", signing_key,
            "--ledger", str(tmp_path / "ledger.jsonl"),
            "--sessions", "120", "--tenants", "3", "--max-events", "25",
        ]
    )
    summary = json.loads(capsys.readouterr().out)
    assert summary["batches"] >= 8, "not enough requests for keep-alive to mean anything"
    assert live_gateway.connections == 1, "the driver opened more than one connection"


def test_every_request_the_cli_made_carries_the_contract_headers(
    live_gateway, signing_key, tmp_path, capsys
):
    main(
        [
            "--url", live_gateway.url,
            "--signing-key", signing_key,
            "--ledger", str(tmp_path / "ledger.jsonl"),
            "--sessions", "40", "--tenants", "2",
        ]
    )
    capsys.readouterr()
    for path, headers, body in live_gateway.requests:
        assert path == "/v1/ingest"
        assert headers["content-type"] == BATCH_CONTENT_TYPE
        assert headers["authorization"].startswith("Bearer ey")
        assert headers["x-source-type"] in {"WEB_APP", "MOBILE_APP", "THIRD_PARTY_SERVICE"}
        assert len({ev["source"] for ev in msgspec.json.decode(body, type=list)}) == 1


def test_a_gateway_that_keeps_refusing_makes_the_run_exit_non_zero(
    live_gateway, signing_key, tmp_path, capsys
):
    """`sent == accepted` is the checkpoint, so a run that cannot make it has to
    fail loudly instead of printing a summary and exiting 0."""
    live_gateway.status = 429
    code = main(
        [
            "--url", live_gateway.url,
            "--signing-key", signing_key,
            "--ledger", str(tmp_path / "ledger.jsonl"),
            "--sessions", "20", "--tenants", "2", "--max-attempts", "2",
        ]
    )
    summary = json.loads(capsys.readouterr().out)
    assert code == 1
    assert summary["clean"] is False
    assert summary["by_status"] == {"RATE_LIMITED": summary["batches"]}
    assert summary["unaccepted"] == summary["sent"] > 0


def test_the_cli_replays_the_gateway_s_own_tenant_list(
    live_gateway, signing_key, tmp_path, capsys, monkeypatch
):
    """`GATEWAY_TENANTS` is the gateway's registry, and a driver naming a tenant it
    has not heard of gets a 403 per request. So the env wins over `--tenants`."""
    monkeypatch.setenv("GATEWAY_TENANTS", "tenant_0001,tenant_0009")
    assert main(
        [
            "--url", live_gateway.url,
            "--signing-key", signing_key,
            "--ledger", str(tmp_path / "ledger.jsonl"),
            "--tenants", "500", "--sessions", "40",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["tenants"] == 2
    assert {r.tenant for r in Ledger(tmp_path / "ledger.jsonl").read_all()} == {
        "tenant_0001",
        "tenant_0009",
    }


def test_the_cli_injects_malformed_events_without_writing_the_ledger_twice(
    live_gateway, signing_key, tmp_path, capsys
):
    """`MalformedInjector.build` writes the ledger itself, so the CLI uses its
    pass-through and lets the driver -- the thing that knows what went out -- own
    the file. A double append would show up as every event being a duplicate."""
    assert main(
        [
            "--url", live_gateway.url,
            "--signing-key", signing_key,
            "--ledger", str(tmp_path / "ledger.jsonl"),
            "--sessions", "120", "--tenants", "2", "--max-events", "50",
            "--inject-invalid-rate", "20",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)
    records = Ledger(tmp_path / "ledger.jsonl").read_all()
    assert summary["injected"] > 0
    assert set(summary["by_variant"]) == set(VARIANT_CODES)
    assert len(records) == summary["sent"] == summary["ledger_rows"]
    assert Ledger.duplicates(records) == summary["by_variant"][CODE_DUPLICATE_ID]


def test_a_clean_cli_run_never_emits_application_abandoned(
    live_gateway, signing_key, tmp_path, capsys
):
    main(
        [
            "--url", live_gateway.url,
            "--signing-key", signing_key,
            "--ledger", str(tmp_path / "ledger.jsonl"),
            "--sessions", "300", "--tenants", "3",
        ]
    )
    capsys.readouterr()
    records = Ledger(tmp_path / "ledger.jsonl").read_all()
    assert not any(r.type.endswith("application-abandoned") for r in records)


def test_a_missing_signing_key_is_a_usage_error_not_a_stack_trace(
    live_gateway, tmp_path, capsys
):
    """The driver's most likely misconfiguration: the compose header writes the
    private key next to `.env`, and a run without it should say so by name."""
    with pytest.raises(SystemExit) as exit_info:
        main(
            [
                "--url", live_gateway.url,
                "--signing-key", str(tmp_path / "absent.pem"),
                "--ledger", str(tmp_path / "ledger.jsonl"),
            ]
        )
    assert exit_info.value.code == 2
    assert "absent.pem" in capsys.readouterr().err


def test_the_configured_catalog_carries_the_gateways_tenant_list_not_a_count():
    """`TenantCatalog` mints `tenant_0001..N`; a gateway whose list is anything
    else would be replayed against tenants it has never heard of."""
    catalog = ConfiguredCatalog(["acme_8921", "globex_4471"], seed=3, users_per_tenant=4)
    assert catalog.ids == ["acme_8921", "globex_4471"]
    assert catalog.count == 2
    assert catalog.users("acme_8921", 2) == TenantCatalog(2, seed=3).users("acme_8921", 2)
    assert catalog.shard_ids(0, 1) == ["acme_8921", "globex_4471"]


def test_a_configured_catalog_with_no_tenants_is_refused():
    with pytest.raises(ValueError, match="at least one tenant"):
        ConfiguredCatalog([])
    with pytest.raises(ValueError):
        ConfiguredCatalog([" ", ""])


def test_the_tenant_list_reads_the_gateways_own_variable():
    assert tenants_from_env({"GATEWAY_TENANTS": " a , b ,, c "}) == ["a", "b", "c"]
    assert tenants_from_env({}) == [], "an unset list falls back to --tenants, not an error"


def test_a_rate_outside_a_percentage_is_refused(live_gateway, signing_key, tmp_path, capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(
            [
                "--url", live_gateway.url,
                "--signing-key", signing_key,
                "--ledger", str(tmp_path / "ledger.jsonl"),
                "--inject-invalid-rate", "150",
            ]
        )
    assert exit_info.value.code == 2
    assert "100" in capsys.readouterr().err


def test_the_token_audience_follows_the_same_environment_variable_the_gateway_reads(
    live_gateway, signing_key, tmp_path, capsys, monkeypatch
):
    """A token signed for the wrong audience is a 401 on every request, and the two
    sides read one variable -- so a deployment that changes it must not be able to
    change it for the gateway alone."""
    monkeypatch.setenv("JWT_AUD", "career-api-eu")
    assert main(
        [
            "--url", live_gateway.url,
            "--signing-key", signing_key,
            "--ledger", str(tmp_path / "ledger.jsonl"),
            "--sessions", "20", "--tenants", "2",
        ]
    ) == 0
    capsys.readouterr()
    import jwt as _jwt

    _, headers, _body = live_gateway.requests[0]
    claims = _jwt.decode(
        headers["authorization"].removeprefix("Bearer "),
        options={"verify_signature": False},
        audience="career-api-eu",
    )
    assert claims["aud"] == "career-api-eu"


def test_a_bad_max_events_is_refused(live_gateway, signing_key, tmp_path, capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(
            [
                "--url", live_gateway.url,
                "--signing-key", signing_key,
                "--ledger", str(tmp_path / "ledger.jsonl"),
                "--max-events", "0",
            ]
        )
    assert exit_info.value.code == 2
    assert "--max-events" in capsys.readouterr().err


@pytest.mark.parametrize(
    "flag,value", [("--max-batch-bytes", "0"), ("--max-attempts", "0"), ("--timeout", "0")]
)
def test_a_nonsensical_operating_point_is_refused_before_anything_is_sent(
    live_gateway, signing_key, tmp_path, capsys, flag, value
):
    """A zero byte cap or a zero timeout is a run that cannot work, and finding that
    out from the exception is worse than being told which flag it was."""
    with pytest.raises(SystemExit) as exit_info:
        main(
            [
                "--url", live_gateway.url,
                "--signing-key", signing_key,
                "--ledger", str(tmp_path / "ledger.jsonl"),
                flag, value,
            ]
        )
    assert exit_info.value.code == 2
    assert flag in capsys.readouterr().err
    assert live_gateway.requests == []


def test_the_cli_defaults_are_the_ones_the_compose_file_provides(
    live_gateway, signing_key, tmp_path, capsys, monkeypatch
):
    """`docker-compose.yml` sets `GATEWAY_URL` and writes
    `driver-signing-key.pem` into the working directory, so a bare
    `python -m driver.main` has to work with no arguments beyond that."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "driver-signing-key.pem").write_bytes(Path(signing_key).read_bytes())
    monkeypatch.setenv("GATEWAY_URL", live_gateway.url)
    assert main(["--sessions", "20", "--tenants", "2"]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["url"] == live_gateway.url
    assert summary["clean"] and summary["sent"] > 0
    assert Path(summary["ledger"]).exists()
