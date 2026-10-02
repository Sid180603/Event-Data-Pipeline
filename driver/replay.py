"""T8d: replay. The driver's only HTTP client.

Everything else in `driver/` GENERATES traffic; this module PUTS it on the wire,
and it is the only place in the system that speaks the **ingress** shape
(`contracts/ingress.py` -- plaintext PII, `data.candidate.user_id`). The egress
shape carries `user_id_pseudo` and `*_enc`, and posting that is refused with
`SCHEMA at $.data.candidate`: that is precisely how the driver first became
unable to drive load at all.

Seven properties are load-bearing, and every one of them is a test rather than a
comment because each of them fails *quietly* if broken:

* **Caps are respected before the send.** 500 events and 4 MiB
  (`app/config.py`) are refused per BATCH with a 413 and no per-event verdicts, so
  a request over either loses every event in it rather than one. The batcher cuts
  to fit instead of learning about it from the gateway.
* **One tenant per request.** A batch spanning two tenants is a 403. The corpus
  already buffers per tenant (`driver/corpus.py`); this module refuses a mixed
  batch rather than trusting that.
* **The batch is cut by the caps only -- deliberately NOT by channel**, and the
  reason is the whole subtlety of this module. A request carries one credential,
  and the credential fixes `sourcechannel` for every event in it
  (`app.auth.authorize_batch`), so a per-channel split is the only way to keep each
  event's own channel label. It is refused because the corpus gives *every* user a
  session on *every* channel, so a per-channel split would deliver a user's WEB_APP
  events, then their MOBILE_APP events, then their partner-webhook ones --
  reordering a journey that CONTRACT.md section 6 promises to produce in order, and
  that the gateway counts as an ordering violation. Ordering is a contract
  guarantee; the channel label is not claimed anywhere. So the cost is paid in the
  label: each request is labelled with the channel of its first event, and the
  gateway records the credential's channel for all of it.
* **The body is encoded once and reused verbatim.** CONTRACT.md section 5 makes a
  stable `id` across retries the client's obligation. Re-encoding on a retry would
  mint a new `id`, and a retry that changes `id` is a duplicate, not a retry. So
  `PlannedBatch` carries the bytes and every attempt PUTs exactly them.
* **Retries are bounded and honest.** A 429 is charged per event and denied per
  batch, so the whole batch comes back refused and is resent -- the same bytes, the
  same ids, after `Retry-After`. A 503 means the events were never buffered, so
  they genuinely have to go out again. Both are retried a bounded number of times
  and an exhausted batch is counted, never dropped silently: a run that lost a
  batch has to say so, because that is the run `sent == accepted` will not hold
  for. A 429'd batch is never split to get under the limit either: the limiter is
  per tenant, not per batch size, so splitting would not help and it would break
  the stable-id guarantee.
* **The ledger is written here**, by the component that knows what actually went
  out. Reconciliation's whole job is to distinguish "the gateway refused this on
  purpose" from "this never arrived", so an event the gateway rejected is in the
  ledger and an event that was never POSTed is not.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import jwt
import msgspec
from cryptography.hazmat.primitives import serialization

from app.auth.jwt import ALLOWED_ALGORITHMS, CREDENTIAL_CLAIM, TENANT_CLAIM, TokenVerifier
from app.config import MAX_BATCH_BYTES, MAX_EVENTS_PER_BATCH
from app.main import credential_id_for
from app.ratelimit.bucket import RETRY_AFTER_HEADER
from contracts.attributes import career_site_id_from_source
from contracts.ledger import Ledger, LedgerRecord

#: CONTRACT.md section 2. Not a constant anywhere in the shipped code -- the
#: gateway's handler passes the header straight through -- so the driver states it
#: once here and every request it builds carries it.
BATCH_CONTENT_TYPE = "application/cloudevents-batch+json"

#: CONTRACT.md section 2's recommended client operating point, not the ceiling.
#: The ceilings are 500 events and 4 MiB; flushing at 200 / 2 MiB leaves headroom
#: below them, and a load run wants headroom for exactly the case where the
#: gateway is slow enough that the margin matters.
DEFAULT_MAX_EVENTS_PER_BATCH = 200
DEFAULT_MAX_BATCH_BYTES = 2 * 1024 * 1024

#: CONTRACT.md section 2. The endpoint, the one path the driver ever POSTs to.
INGEST_PATH = "/v1/ingest"

#: Attempts per batch, first try included. Five is a run that outlives a broker
#: restart without becoming a run that hangs: at `DEFAULT_RETRY_AFTER_SECONDS`
#: apart that is a handful of seconds of patience, after which the batch is
#: reported as refused rather than retried until the demo is over.
DEFAULT_MAX_ATTEMPTS = 5

#: How long a minted token is good for, and how long before its expiry it is
#: replaced. An hour is generous for a demo run and short enough that a leaked
#: token is not a standing key; the margin is what keeps a long run from walking
#: into a 401 at the boundary, which would look like a gateway fault.
DEFAULT_TOKEN_TTL_SECONDS = 3_600
DEFAULT_TOKEN_REFRESH_MARGIN = 60

#: The wait when the gateway did not say one. Matches the floor
#: `app.ingest.handler` puts on its own `Retry-After`.
DEFAULT_RETRY_AFTER_SECONDS = 1.0

#: Ceiling on an honoured `Retry-After`. A gateway or a proxy that answers with
#: `Retry-After: 86400` would otherwise hold the whole run for a day, and the
#: driver's job is to be interruptible by whoever is watching the demo.
MAX_RETRY_AFTER_SECONDS = 30.0

#: The two statuses that mean "send it again", and that the report counts as they
#: arrive. A 429 is a load-shed signal and the whole batch is denied, not partly; a
#: 503 says the events were never buffered, and "accepted into the buffer" is not
#: durability (C4). Everything else -- 400, 401, 403, 413 -- is a property of the
#: bytes, so an identical resend earns an identical answer and the retry budget
#: belongs to the two that can change.
RETRYABLE_STATUSES = frozenset({429, 503})

#: The `by_status` key for a batch whose connection never came back. The other keys
#: are the gateway's own reason codes, which is what keeps `DLQ_UNAVAILABLE` and
#: `SINK_UNAVAILABLE` apart -- both arrive as 503 and only one is a broker problem.
REASON_TRANSPORT_ERROR = "TRANSPORT_ERROR"

#: The `by_status` key for a 202 whose body was not the documented object.
REASON_MALFORMED_202 = "MALFORMED_202"

#: `[]` and `,`. The batch body is a JSON array, so its punctuation is part of
#: what the gateway's byte cap measures, and a batch sized on the events alone
#: overshoots by the number of commas.
_BRACKETS = 2
_COMMA = 1


@dataclass(frozen=True, slots=True)
class _Token:
    value: str
    expires_at: float


class TokenMinter:
    """The issuing side of the contract, and the only holder of the PRIVATE key.

    `app/auth/jwt.py` gives the gateway the public half and no signing path at
    all, so this class is what makes a driver-driven load run possible at all --
    and it is deliberately the only place a token naming a tenant can come into
    existence. The key pair is the one the compose file writes to
    `driver-signing-key.pem` and `JWT_PUBLIC_KEY_PEM`.

    Tokens are cached per (tenant, channel) and re-minted shortly before they
    expire. Signing is per *request* rather than per event, but a 5M-event run is
    ~25,000 requests over 500 tenants and 3 channels, so one signature per request
    would be tens of thousands of EdDSA operations buying nothing. The credential
    id is derived by `app.main.credential_id_for` rather than formatted here: the
    gateway registers `cred_<tenant>_<channel>` and the pairing has to be
    derivable by the driver, which is exactly what that function's docstring says
    it is for.
    """

    __slots__ = (
        "_algorithm",
        "_audience",
        "_cache",
        "_clock",
        "_key",
        "_margin",
        "_ttl",
    )

    def __init__(
        self,
        private_key_pem: bytes,
        *,
        audience: str,
        algorithm: str = "EdDSA",
        ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS,
        refresh_margin: int = DEFAULT_TOKEN_REFRESH_MARGIN,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if algorithm not in ALLOWED_ALGORITHMS:
            raise ValueError(
                f"algorithm must be one of {ALLOWED_ALGORITHMS}: a shared secret would "
                f"let the driver mint a token for every tenant, got {algorithm!r}"
            )
        if ttl_seconds < 1:
            raise ValueError(f"ttl_seconds must be at least 1, got {ttl_seconds}")
        if not 0 <= refresh_margin < ttl_seconds:
            raise ValueError(
                f"refresh_margin must leave a usable window inside a {ttl_seconds}s token"
            )
        if not audience:
            raise ValueError("audience is required: the gateway pins it (JWT_AUD)")
        self._key = _load_private_key(private_key_pem, algorithm, audience)
        self._algorithm = algorithm
        self._audience = audience
        self._ttl = ttl_seconds
        self._margin = refresh_margin
        self._clock = clock
        self._cache: dict[str, _Token] = {}

    def token(self, tenant: str, channel: str) -> str:
        """A bearer token asserting `tenant` and the credential for `channel`."""
        credential_id = credential_id_for(tenant, channel)
        now = self._clock()
        cached = self._cache.get(credential_id)
        if cached is not None and now < cached.expires_at - self._margin:
            return cached.value
        expires_at = now + self._ttl
        value = jwt.encode(
            {
                TENANT_CLAIM: tenant,
                CREDENTIAL_CLAIM: credential_id,
                "aud": self._audience,
                "iat": int(now),
                "exp": int(expires_at),
            },
            self._key,
            algorithm=self._algorithm,
        )
        self._cache[credential_id] = _Token(value, expires_at)
        return value


def _load_private_key(pem: bytes, algorithm: str, audience: str):
    """The signing key, or a loud failure at construction.

    A public key, a shared secret and garbage all fail here rather than at the
    first request, because a driver that cannot sign produces nothing and says
    nothing, and the obvious place to look is the network.

    The check is the gateway's own: the public half of whatever was loaded is put
    through `TokenVerifier`, so the driver refuses exactly the keys the gateway
    would refuse -- an Ed448 key offered as EdDSA, say -- instead of keeping a
    second table of key types to drift from the first.
    """
    try:
        key = serialization.load_pem_private_key(pem, password=None)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"private key is not loadable: {exc}. The driver needs the PRIVATE half of "
            "the pair; docker-compose writes it to driver-signing-key.pem"
        ) from exc
    TokenVerifier(
        key.public_key()
        .public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        .decode(),
        algorithm,
        audience,
    )
    return key


@dataclass(frozen=True, slots=True)
class PlannedBatch:
    """One request's worth of events, encoded once, ready to be PUT repeatedly.

    `body` is the reason this type exists. It is built here from events that are
    never re-encoded, and every attempt at this batch sends exactly these bytes,
    which is what makes a retry a retry rather than a duplicate.

    `channel` is the channel this request's CREDENTIAL is registered for, and it
    is what every event in the batch is recorded as on the topic. It is taken from
    the batch's first event rather than required to be uniform -- see the module
    docstring for why the caps and not the channel are what cut a batch.
    """

    tenant: str
    channel: str
    events: tuple[dict, ...]
    body: bytes

    @property
    def size(self) -> int:
        return len(self.events)

    def headers(self, token: str) -> dict[str, str]:
        """The three headers CONTRACT.md section 2 requires of a request.

        `x-source-type` is sent because it is the client's half of a two-sided
        check: the gateway validates the hint against the credential
        registration and records the *registration's* channel, so a hint that
        disagreed would be a 403 rather than something honoured. Sending the one
        we hold a credential for is what turns that check into a live assertion.
        """
        return {
            "content-type": BATCH_CONTENT_TYPE,
            "authorization": f"Bearer {token}",
            "x-source-type": self.channel,
        }


def plan_batches(
    batch: Sequence[dict],
    *,
    max_events: int = DEFAULT_MAX_EVENTS_PER_BATCH,
    max_bytes: int = DEFAULT_MAX_BATCH_BYTES,
) -> Iterator[PlannedBatch]:
    """Plan every request one single-tenant corpus batch becomes.

    Per-batch planning because that is the guarantee the gateway's tenancy check
    has to be met against: the plan spans exactly the tenants the batch spans, so
    "no request spans two tenants" is checkable from one batch, and a caller that
    hands the driver a mixed batch gets a refusal instead of a 403 per request.

    The caps are CLAMPED to the gateway's ceilings rather than trusted. A caller
    asking for 5,000 events per request does not get a run full of 413s; it gets
    500-event requests, which is the most the gateway will ever take.

    Cut by the caps and by nothing else -- notably not by channel. See the module
    docstring; the short version is that a per-channel split reorders every user
    who applied on more than one channel, and the corpus gives every user a
    session on every channel.
    """
    if max_events < 1:
        raise ValueError(f"max_events must be at least 1, got {max_events}")
    if max_bytes < _BRACKETS:
        raise ValueError(f"max_bytes must leave room for the array brackets, got {max_bytes}")
    if not batch:
        return

    tenant = career_site_id_from_source(batch[0]["source"])
    if any(ev["source"] != batch[0]["source"] for ev in batch):
        raise ValueError(
            f"a request must carry exactly one tenant, and this batch spans more "
            f"than {tenant}: the gateway answers a mixed-tenant batch with 403 "
            f"for the whole request (CorpusBuilder.batches already buffers per tenant)"
        )

    limits = (min(max_events, MAX_EVENTS_PER_BATCH), min(max_bytes, MAX_BATCH_BYTES))
    for events, encoded in _chunks(batch, *limits):
        yield PlannedBatch(
            tenant=tenant,
            channel=_channel_of(events[0]),
            events=tuple(events),
            body=b"[" + b",".join(encoded) + b"]",
        )


def _channel_of(event: dict) -> str:
    """The source channel a request's credential must be registered for.

    There has to be one: the gateway records the channel the request's credential
    is registered under, not the one in the body, so an event carrying no channel
    leaves the driver unable to say which credential it is sending under.
    """
    channel = event.get("sourcechannel")
    if not channel:
        raise ValueError(
            "every event needs a sourcechannel: the request's credential fixes the "
            f"channel for the whole request, and {event['id']} has none"
        )
    return channel


def _chunks(
    events: Sequence[dict], max_events: int, max_bytes: int
) -> Iterator[tuple[list[dict], list[bytes]]]:
    """`(events, encoded)` runs, cut so each body is within both caps.

    Accumulates the per-event encodings rather than re-encoding the buffer to
    measure it: the body is built once per planned batch (see `PlannedBatch`), and
    a cut decided by re-encoding everything in the buffer would encode every event
    twice. An event is encoded once here and the returned bytes are the ones that
    go on the wire, so the size that decided the cut and the body that is sent
    cannot disagree.
    """
    events_here: list[dict] = []
    encoded_here: list[bytes] = []
    total = _BRACKETS
    for ev in events:
        encoded = msgspec.json.encode(ev)
        size = len(encoded)
        if events_here and (len(events_here) >= max_events or total + size + _COMMA > max_bytes):
            yield events_here, encoded_here
            events_here, encoded_here, total = [], [], _BRACKETS
        total += size + (_COMMA if events_here else 0)
        if total > max_bytes:
            raise ValueError(
                f"one event encodes to {size} bytes and cannot fit a {max_bytes}-byte "
                "batch: the gateway refuses it as a 413 for the whole request, and no "
                "cut of this batch can help"
            )
        events_here.append(ev)
        encoded_here.append(encoded)
    if events_here:
        yield events_here, encoded_here


# --- the receipt --------------------------------------------------------------


@dataclass(slots=True)
class ReplayReport:
    """What the run put on the wire, and what came back.

    `sent` is the number the ledger holds and the number reconciliation starts
    from. `accepted + rejected` is what the gateway accounted for, and
    `unaccepted` is the gap between them: the events that went out and were never
    taken by any request. On a clean run that gap is zero, and `clean` is the
    single boolean the CLI turns into its exit code, because a run that lost a
    batch has to fail loudly rather than print a summary nobody reads.

    `by_status` is keyed by the gateway's own reason code and holds only the
    batches that ended without a 202, so an empty dict is the whole claim "every
    batch was accepted, in the end, after its retries".
    """

    batches: int = 0
    sent: int = 0
    accepted: int = 0
    rejected: int = 0
    retried: int = 0
    rate_limited: int = 0
    sink_unavailable: int = 0
    by_status: dict[str, int] = field(default_factory=dict)

    @property
    def unaccepted(self) -> int:
        """Events that went out and no request ever accounted for."""
        return self.sent - self.accepted - self.rejected

    @property
    def clean(self) -> bool:
        return not self.by_status and self.unaccepted == 0

    def to_dict(self) -> dict:
        return {
            "batches": self.batches,
            "sent": self.sent,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "retried": self.retried,
            "rate_limited": self.rate_limited,
            "sink_unavailable": self.sink_unavailable,
            "by_status": dict(sorted(self.by_status.items())),
            "unaccepted": self.unaccepted,
            "clean": self.clean,
        }

    def record_refusal(self, reason: str) -> None:
        """Note that a batch ended without a 202, under the gateway's own code."""
        self.by_status[reason] = self.by_status.get(reason, 0) + 1


# --- the client ---------------------------------------------------------------


class ReplayDriver:
    """POSTs planned batches to `POST /v1/ingest` and writes the ledger.

    **The HTTP client is a parameter, not something built here.** Keep-alive is
    the client's connection pool, so a client per batch is a TCP handshake per
    batch -- at 250 requests/sec that is 250 handshakes a second, which is fatal
    and gets misdiagnosed as a gateway problem rather than a driver one. Handing
    the client in also puts the timeout in one place, and makes "the whole run
    went over one connection" a thing a test can measure.

    **Single-threaded on purpose.** One loop, one ledger writer, and therefore
    ledger order equal to request order for free. The gateway is the thing being
    measured; a driver with a thread pool of its own would be measuring the
    scheduler (docker-compose says so in as many words about the cpus limits).
    Concurrency belongs in `bench/load.py`, which is allowed to be a benchmark.

    The ledger is written here, once per batch, after the attempt loop and
    whatever it cost. Appending per attempt would put the same event in the ground
    truth twice, and `Ledger.duplicates` is the number `tools/verify.py` asserts is
    zero.
    """

    def __init__(
        self,
        *,
        url: str,
        signer: TokenMinter,
        client: httpx.Client,
        ledger_path: Path,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        max_events: int = DEFAULT_MAX_EVENTS_PER_BATCH,
        max_bytes: int = DEFAULT_MAX_BATCH_BYTES,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be at least 1, got {max_attempts}")
        self.url = url.rstrip("/") + INGEST_PATH
        self.signer = signer
        self.client = client
        self.ledger_path = Path(ledger_path)
        self.max_attempts = max_attempts
        self.max_events = max_events
        self.max_bytes = max_bytes
        self.sleep = sleep
        self.report = ReplayReport()

    def run(self, batches: Iterable[Sequence[dict]]) -> ReplayReport:
        """Replay `batches` and return the report. One pass over the stream.

        The report starts fresh, so a second `run` on the same driver reports the
        second run rather than the sum of both -- and `Ledger` appends, so a stale
        file from a previous run would double every count in the reconciliation
        anyway. Removed first, like `MalformedInjector.build`.
        """
        self.report = ReplayReport()
        self.ledger_path.unlink(missing_ok=True)
        with Ledger(self.ledger_path) as ledger:
            for batch in batches:
                for planned in plan_batches(
                    batch, max_events=self.max_events, max_bytes=self.max_bytes
                ):
                    self._send(planned)
                    self._record(planned, ledger)
                    self.report.batches += 1
                    self.report.sent += planned.size
        return self.report

    def _send(self, planned: PlannedBatch) -> None:
        """POST one batch, retrying the SAME bytes while it is worth retrying.

        The body is planned once and never rebuilt, so every attempt is the same
        `id`s. That is what makes the retries idempotent under `(source, id)` and
        what keeps `Ledger.duplicates` at zero.
        """
        headers = planned.headers(self.signer.token(planned.tenant, planned.channel))
        for attempt in range(1, self.max_attempts + 1):
            last = attempt == self.max_attempts
            try:
                response = self.client.post(self.url, content=planned.body, headers=headers)
            except httpx.TransportError:
                # CONTRACT.md section 3: a connection reset must be retried, and
                # the resend is safe for the same reason a 503 retry is.
                if last:
                    self.report.record_refusal(REASON_TRANSPORT_ERROR)
                    return
                self.report.retried += 1
                self.sleep(DEFAULT_RETRY_AFTER_SECONDS)
                continue

            status = response.status_code
            if status == 202:
                self._accept(response)
                return
            # Counted as they arrive, not only when the retries run out: a shed that
            # recovered on the third try is still the interesting number for the
            # flood beat, and these two statuses are exactly `RETRYABLE_STATUSES`.
            if status == 429:
                self.report.rate_limited += 1
            elif status == 503:
                self.report.sink_unavailable += 1
            if status not in RETRYABLE_STATUSES:
                self.report.record_refusal(_failure_reason(response))
                return
            if last:
                self.report.record_refusal(_failure_reason(response))
                return
            self.report.retried += 1
            self.sleep(_retry_after(response))

    def _accept(self, response: httpx.Response) -> None:
        """Fold one `202 {accepted, rejected}` into the report.

        A body that is not the documented object is a refusal, not an acceptance
        of zero. The gateway's own encoder cannot produce one, so it means something
        is answering on its behalf -- and letting `sent == accepted` balance against
        a body nobody read is the one thing this report must never do.
        """
        try:
            body = response.json()
        except ValueError:
            body = None
        if not isinstance(body, dict):
            self.report.record_refusal(REASON_MALFORMED_202)
            return
        self.report.accepted += int(body.get("accepted") or 0)
        self.report.rejected += len(body.get("rejected") or ())

    def _record(self, planned: PlannedBatch, ledger: Ledger) -> None:
        """One ledger row per event that went out, in the T2 schema.

        After the attempt loop and unconditionally: an event that was POSTed five
        times and refused is still an event the driver sent, and the receipt's
        whole job is to let reconciliation see the difference between that and an
        event it never produced.
        """
        for ev in planned.events:
            ledger.append(
                LedgerRecord(
                    id=ev["id"],
                    source=ev["source"],
                    type=ev["type"],
                    tenant=planned.tenant,
                    user_pseudo=ev["data"]["candidate"]["user_id"],
                    seq=int(ev["sequence"]),
                )
            )


def _failure_reason(response: httpx.Response) -> str:
    """The gateway's reason code, falling back to the status.

    The body is a JSON object the gateway wrote, and its `reason` is the operator's
    diagnosis -- `SINK_UNAVAILABLE` against `DLQ_UNAVAILABLE` is the difference
    between a full producer buffer and a broken DLQ, and both arrive as 503. If it
    is unreadable (a proxy answering with HTML) the status is still something an
    operator can act on, so the fallback is a status rather than a shrug.
    """
    try:
        body = response.json()
    except ValueError:
        return str(response.status_code)
    reason = body.get("reason") if isinstance(body, dict) else None
    return reason if isinstance(reason, str) and reason else str(response.status_code)


def _retry_after(response: httpx.Response) -> float:
    """Seconds to wait, from `Retry-After`, clamped to a wait a demo can sit through.

    RFC 9110's integer-seconds form is what the gateway sends; anything else -- an
    HTTP-date, a typo, a missing header -- falls back to the same floor the
    gateway itself uses, rather than to a guess of zero that would turn a 429 into
    a hot loop.
    """
    try:
        seconds = float(response.headers.get(RETRY_AFTER_HEADER, DEFAULT_RETRY_AFTER_SECONDS))
    except (TypeError, ValueError):
        return DEFAULT_RETRY_AFTER_SECONDS
    return min(max(seconds, 0.0), MAX_RETRY_AFTER_SECONDS)


__all__ = [
    "BATCH_CONTENT_TYPE",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_BATCH_BYTES",
    "DEFAULT_MAX_EVENTS_PER_BATCH",
    "DEFAULT_RETRY_AFTER_SECONDS",
    "DEFAULT_TOKEN_REFRESH_MARGIN",
    "DEFAULT_TOKEN_TTL_SECONDS",
    "INGEST_PATH",
    "MAX_RETRY_AFTER_SECONDS",
    "REASON_MALFORMED_202",
    "REASON_TRANSPORT_ERROR",
    "RETRYABLE_STATUSES",
    "PlannedBatch",
    "ReplayDriver",
    "ReplayReport",
    "TokenMinter",
    "plan_batches",
]
