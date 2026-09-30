"""The ingest pipeline. `decode -> auth -> validate -> encrypt -> produce`.

That order is load-bearing, and three separate guarantees fall out of it:

* **The tenant is bound from the signed claim before any per-event decision is
  disclosed or written anywhere** (D4). A rejection is a fact about one tenant's
  data, so an unauthenticated caller must not be able to learn it, and must not
  be able to make us write a DLQ record.
* **Encryption happens after validation and before production**, so the record
  that reaches `career.events.raw` and the record that reaches `career.events.dlq`
  are both post-encryption. The DLQ is therefore not a second plaintext PII
  store, and a replayed DLQ record is not double-encrypted (C3).
* **The final size check runs on the encrypted event** (G2), because that is the
  one that has to fit inside a Kafka message.

## The ingress shape is not the published shape

`contracts/cloudevent.py` describes what lands on the topic: `data.candidate`
already holds ciphertexts. A request cannot look like that, because the client
has no tenant key. So `IngressEvent` below is the *request* shape -- plaintext
PII under the same names `app.crypto.facade.PII_FIELDS` encrypts from -- and it
is the only place the gateway accepts a raw candidate value.

The rule that keeps this from becoming a hole: **the gateway is the only writer
of ciphertext.** `IngressCandidate.forbid_unknown_fields` refuses `*_enc` and
`email_hmac` outright, so a client can never get ciphertext of its own choosing
published under a key it picked, and a replayed record can never arrive here to
be encrypted twice. Replay goes through the Queue team's worker via
`app.dlq.envelope.dlq_data_to_ingress`, which does not come back through here.

The ingress structs *carry*; the contract structs *validate*. So the enums
(`specversion`, `type`, `experience_status`) are typed `str` here and enforced
against the real contract structs in `app.validate`, rather than being declared
twice and drifting.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import msgspec
from msgspec import structs

from app.auth import TokenParseBudget, TokenVerifier, TenantRegistry, authorize_batch
from app.auth.errors import Forbidden
from app.config import MAX_EVENT_BYTES, TOPIC_RAW
from app.crypto.facade import protect_candidate
from app.crypto.registry import TenantKeyRegistry
from app.dlq.envelope import UNKNOWN_TENANT, build_dlq_event
from app.ingest.limits import IngestError, split_limited_batch
from app.ratelimit.registry import BucketRegistry
from app.validate.events import (
    CODE_OVERSIZED,
    CODE_SCHEMA,
    CODE_UNKNOWN_ATTR,
    Rejection,
    # Not a public name, deliberately: it is the single place that knows how to
    # pull a JSON path out of a msgspec error message without also picking up the
    # offending value that message starts with. Reimplementing it here would
    # mean two copies of that regex to keep in step.
    _error_path,
    validate_batch,
)
from contracts.attributes import (
    career_site_id_from_source,
    derive_kafka_key,
    extra_attributes,
    partitionkey_conflicts,
)
from contracts.cloudevent import CandidateMetadata, CloudEvent, Data, EventPayload

REASON_RATE_LIMITED = "RATE_LIMITED"
REASON_SINK_UNAVAILABLE = "SINK_UNAVAILABLE"
REASON_DLQ_UNAVAILABLE = "DLQ_UNAVAILABLE"

#: What a 202 actually asserts. Deliberately not a durability claim: the events
#: are in the producer's bounded in-memory buffer, and a process killed
#: mid-flight loses the un-acked window. That window is measured in T11, not
#: denied here.
DURABILITY_ACCEPTED_INTO_BUFFER = "accepted-into-buffer"


class RateLimited(IngestError):
    """429. Charged per event, denied per batch, never rolled back."""

    status_code = 429

    def __init__(self, retry_after: int, reason: str = REASON_RATE_LIMITED) -> None:
        super().__init__(reason)
        #: Whole seconds until the bucket holds one token again; goes straight
        #: into `Retry-After` (RFC 9110 integer-seconds form).
        self.retry_after = retry_after


class SinkUnavailable(IngestError):
    """503. The producer buffer is full, or the broker is unreachable.

    Raised by the injected sink. `app/kafka` is expected to translate whatever
    its client raises (queue full, `OSError`, a client-specific exception) into
    this, because "we accepted work we cannot hold" is the gateway's promise to
    break, and only the gateway knows what that promise is.
    """

    status_code = 503
    retry_after = 1


class Sink(Protocol):
    """The seam between this pipeline and Kafka. Injected, never imported.

    Returning means *enqueued*, not *durable* -- see
    `DURABILITY_ACCEPTED_INTO_BUFFER`.
    """

    def sink(self, topic: str, key: str, value: bytes) -> None:
        """Enqueue one record. `key` is the raw derived key string."""

    def sink_dlq(self, key: str, value: bytes) -> None:
        """Enqueue one DLQ record. `key` is the tenant `source`.

        No topic argument: `career.events.dlq` is a property of the producer, and
        naming it here would put a second copy of the topic table in the file
        that has no business owning it.
        """


# --- the request shape -------------------------------------------------------


class IngressCandidate(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    """`data.candidate` as a client sends it: plaintext, before encryption.

    `forbid_unknown_fields` is a security control here, not tidiness: it is what
    refuses a client-supplied `email_enc`/`email_hmac`, so the gateway can never
    be talked into publishing ciphertext it did not produce.
    """

    #: Opaque per-user identifier. It is NOT a pseudonym yet -- the gateway HMACs
    #: it in the encrypt stage, because the client cannot: the `mac_key` derives
    #: from a master secret the gateway holds alone.
    user_id_pseudo: str
    email: str | None = None
    phone: str | None = None
    alternate_phone: str | None = None
    name: str | None = None
    gender: str | None = None
    experience_status: str | None = None
    years_of_experience: float | None = None
    education_degree: str | None = None
    education_branch: str | None = None


class IngressData(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    candidate: IngressCandidate
    #: The published payload struct verbatim. There is nothing to loosen about
    #: it, and a second copy would only be a second thing to keep in sync.
    event_payload: EventPayload


class IngressEvent(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    """One CloudEvents batch element as it arrives.

    The field set is the published allowlist (`contracts.attributes.
    SAFE_CONTEXT_ATTRS`) plus `data`, which a test asserts, so the two cannot
    drift. `forbid_unknown_fields` therefore enforces the allowlist here instead
    of relying on a separate pass over the raw dict.
    """

    specversion: str
    id: str
    source: str
    type: str
    time: str
    data: IngressData
    subject: str | None = None
    dataschema: str | None = None
    datacontenttype: str | None = None
    keyversion: int | None = None
    sequence: str | None = None
    sourcechannel: str | None = None
    referrertype: str | None = None
    completionmethod: str | None = None
    #: Never used for routing. Read only so a hint disagreeing with the derived
    #: key can be refused with 403 (C2).
    partitionkey: str | None = None


# --- the result --------------------------------------------------------------


@dataclass(slots=True)
class IngestResult:
    """What the handler turns into `202 {accepted, rejected}`.

    `accepted` counts events handed to the sink, and `durability` says plainly
    what that means: **accepted into the producer's in-memory buffer, NOT
    durably in Kafka** (C4). The field exists so the distinction is readable at
    the call site and in anything derived from this object, rather than living
    in a comment some later reader will not find.

    A sink failure part-way through raises `SinkUnavailable` instead of returning
    a partial result: the client resends the whole batch with the same event
    `id`s, and `(source, id)` dedup makes that safe.
    """

    accepted: int
    rejected: list[Rejection]
    buffered: int
    durability: str = DURABILITY_ACCEPTED_INTO_BUFFER

    def to_response(self) -> dict:
        return {"accepted": self.accepted, "rejected": [r.to_dict() for r in self.rejected]}


# --- stages ------------------------------------------------------------------


def _decode_reason(index: int, item: msgspec.Raw, exc: msgspec.DecodeError) -> str:
    """A safe reason CODE for an event that would not decode.

    msgspec puts the offending *value* in the message prefix ("Invalid enum value
    'x'"), so the message is never used -- only its trailing JSON path, which
    names a field and not its value.
    """
    try:
        raw = msgspec.json.decode(item, type=dict)
    except msgspec.DecodeError:
        return f"{CODE_SCHEMA} at {_error_path(exc)}"
    extra = extra_attributes(raw)
    if extra:
        # An attribute NAME is a name, not a value, and it is exactly what the
        # client needs in order to fix their request.
        return f"{CODE_UNKNOWN_ATTR} at $[{index}]: {sorted(extra)[0]}"
    return f"{CODE_SCHEMA} at {_error_path(exc)}"


def _decode_contract_event(form: dict) -> CloudEvent | None:
    """The contract view of one event, or None when the contract refuses it."""
    try:
        return msgspec.json.decode(msgspec.json.encode(form), type=CloudEvent)
    except msgspec.DecodeError:
        return None


def _contract_form(event: IngressEvent) -> IngressEvent:
    """The same event with its plaintext candidate swapped for the contract's.

    The `user_id_pseudo` carried through is the client's raw per-user id, not a
    pseudonym -- it stands in for the required field so the published structs can
    do the validating. It stays in memory and reaches a DLQ record only for an
    event that failed validation; the published record always carries the HMAC.
    """
    candidate = event.data.candidate
    return structs.replace(
        event,
        data=IngressData(
            candidate=CandidateMetadata(
                user_id_pseudo=candidate.user_id_pseudo,
                experience_status=candidate.experience_status,
                years_of_experience=candidate.years_of_experience,
                education_degree=candidate.education_degree,
                education_branch=candidate.education_branch,
            ),
            event_payload=event.data.event_payload,
        ),
    )


def _encrypt(event: IngressEvent, *, keys: TenantKeyRegistry, source_channel: str) -> tuple[CloudEvent, str]:
    """Encrypt the candidate; return `(published_event, kafka_key)`.

    Every envelope value is copied from the ingress event, which is safe only
    because this runs over `outcome.events` -- events the contract structs have
    already checked, so `specversion` really is "1.0" and `type` really is in the
    published enum. The one field NOT copied is `keyversion`, which comes from
    the registry: a client does not get to choose which key encrypts its data.

    The Kafka key is derived here, from the tenant the token named and the
    pseudonym the encrypt stage just produced. A client `partitionkey` is never
    used for routing, and a hint that disagrees is refused rather than ignored --
    silently ignoring it would leave the client believing it controls placement.
    """
    candidate = event.data.candidate
    career_site_id = career_site_id_from_source(event.source)
    metadata = protect_candidate(
        registry=keys,
        source=event.source,
        event_id=event.id,
        event_type=event.type,
        raw_user_id=candidate.user_id_pseudo,
        email=candidate.email,
        phone=candidate.phone,
        alternate_phone=candidate.alternate_phone,
        name=candidate.name,
        gender=candidate.gender,
    )
    key = derive_kafka_key(career_site_id, metadata.user_id_pseudo)
    if partitionkey_conflicts(event.partitionkey, career_site_id, metadata.user_id_pseudo):
        # The reason names no value: a partitionkey carries a user pseudonym, so
        # echoing the client's would put an identifier into a log line.
        raise Forbidden("client partitionkey disagrees with the gateway-derived key")

    return (
        CloudEvent(
            specversion=event.specversion,
            id=event.id,
            source=event.source,
            type=event.type,
            time=event.time,
            data=Data(candidate=metadata, event_payload=event.data.event_payload),
            subject=event.subject,
            dataschema=event.dataschema,
            datacontenttype=event.datacontenttype or "application/json",
            keyversion=keys.key_version,
            sequence=event.sequence,
            # From the credential registration, never the body: the client is not
            # a source of truth about which of its apps is calling.
            sourcechannel=source_channel,
            referrertype=event.referrertype,
            completionmethod=event.completionmethod,
            # The derived key, published so a consumer can check its own routing
            # instead of having to trust the partitioner.
            partitionkey=key,
        ),
        key,
    )


def _dlq_key(payload: dict) -> str:
    """Partition the DLQ by tenant source, not by the derived user key.

    A rejected event often has no usable `user_id_pseudo` -- frequently that is
    *why* it was rejected -- and DLQ consumers group by tenant anyway.
    """
    source = payload.get("source")
    if isinstance(source, str) and source.startswith("/careers/"):
        return source
    return UNKNOWN_TENANT


def _dlq_payload(item: msgspec.Raw) -> dict:
    """The request element as it arrived, for a DLQ record.

    Empty for an element that is not a JSON object at all: there is no payload to
    preserve, and the record's `reason` (a SCHEMA code) is the whole of what is
    known about it. `build_dlq_event` falls back to an unknown-tenant source in
    that case rather than inventing one.
    """
    try:
        payload = msgspec.json.decode(item, type=dict)
    except msgspec.DecodeError:
        return {}
    return payload


def ingest_batch(
    raw: bytes,
    *,
    authorization: str | None,
    sink: Sink,
    verifier: TokenVerifier,
    tenants: TenantRegistry,
    keys: TenantKeyRegistry,
    buckets: BucketRegistry,
    x_source_type: str | None = None,
    parse_budget: TokenParseBudget | None = None,
    max_event_bytes: int = MAX_EVENT_BYTES,
) -> IngestResult:
    """Run one request through the five stages and return the `202` result.

    Raises rather than returning a status, so the handler has no decisions left
    to make: `NotABatch` (400), `Unauthorized` (401), `Forbidden` (403),
    `PayloadTooLarge` (413), `RateLimited` (429), `SinkUnavailable` (503).

    `max_event_bytes` is injectable because the ingress cap makes
    `MAX_EVENT_BYTES` unreachable by construction -- 48 KiB of plaintext becomes
    at most ~64 KiB of base64 -- so the post-encryption check is a belt-and-braces
    defence that only bites if `INGRESS_EVENT_BYTES` is raised. A defence that
    cannot be reached cannot be tested, and an untested size limit is a comment.
    """
    # --- 1. limits, before any per-event work or allocation --------------------
    items = split_limited_batch(raw)

    # --- 2. decode, per event -------------------------------------------------
    # One undecodable event is one rejection. The batch is not the unit of
    # failure: a 50-event batch that lost 49 of them to a single bad byte would
    # be useless to the client, and to us.
    contract_forms: list[dict] = []
    original_index: list[int] = []
    rejections: list[Rejection] = []
    # (source, id) -> (original body index, ingress event). First wins, matching
    # the survivor `validate_batch` keeps when a (source, id) repeats.
    by_key: dict[tuple[str, str], tuple[int, IngressEvent]] = {}

    for index, item in enumerate(items):
        try:
            event = msgspec.json.decode(item, type=IngressEvent)
        except msgspec.DecodeError as exc:
            rejections.append(Rejection(index, _decode_reason(index, item, exc)))
            continue

        contract = _contract_form(event)
        contract_forms.append(msgspec.json.decode(msgspec.json.encode(contract), type=dict))
        original_index.append(index)
        by_key.setdefault((event.source, event.id), (index, event))

    # --- 3. auth --------------------------------------------------------------
    # Over every event that decoded AND that the contract structs accept, not
    # just the ones that will survive validation: a batch spanning two tenants
    # must be refused as a tenancy failure even when one of the two is also
    # malformed. An event the contract cannot decode is left out here and is
    # rejected as SCHEMA in stage 4 -- it reaches neither the cipher nor the
    # topic, so leaving it out does not weaken the tenant binding.
    #
    # The contract structs are what validate here, which is why this stage
    # decodes them separately from the validation stage below. That is one extra
    # msgspec pass per event, bought so no per-event verdict is reached before
    # the tenant is known.
    auth_events = [
        event for form in contract_forms if (event := _decode_contract_event(form)) is not None
    ]
    authed = authorize_batch(
        auth_events,
        authorization=authorization,
        verifier=verifier,
        registry=tenants,
        x_source_type=x_source_type,
        parse_budget=parse_budget,
    )

    # --- 4. validate ----------------------------------------------------------
    outcome = validate_batch(contract_forms)
    # `validate_batch` indexes into the list it was handed, which is the subset
    # that decoded, so remap or a rejection would point at the wrong event.
    for rejection in outcome.rejections:
        rejections.append(Rejection(original_index[rejection.index], rejection.reason))

    # --- 5. rate limit: charged per event, denied per batch -------------------
    # Before any DLQ write and before any encryption, because a 429 is a
    # load-shed signal and not a poison pill: the client will resend the whole
    # batch, and filing it in the DLQ would bury real poison under retry noise.
    #
    # The tokens spent on the events before the denial are not refunded. They were
    # spent, and a refund would be a second accounting system to keep correct
    # under concurrency.
    for _event in outcome.events:
        decision = buckets.acquire(authed.career_site_id)
        if not decision.allowed:
            raise RateLimited(decision.retry_after)

    # --- 6. encrypt -----------------------------------------------------------
    # Only now, with the tenant bound from the signed claim, is anything
    # encrypted, and with a key that cannot belong to another tenant.
    published: list[tuple[str, bytes]] = []
    encrypted_rejections: list[tuple[Rejection, dict]] = []

    for event in outcome.events:
        index, source_event = by_key[(event.source, event.id)]
        encrypted, key = _encrypt(source_event, keys=keys, source_channel=authed.source_channel)
        value = msgspec.json.encode(encrypted)
        if len(value) > max_event_bytes:
            # G2: the check that matters is on the ENCRYPTED event. base64 expands
            # ciphertext ~33% and each field adds a 12-byte nonce and a 16-byte
            # tag, so an event comfortably inside the ingress cap can cross
            # MAX_EVENT_BYTES here.
            encrypted_rejections.append(
                (
                    Rejection(index, f"{CODE_OVERSIZED} at $[{index}]: exceeds {max_event_bytes} bytes"),
                    structs.asdict(encrypted),
                )
            )
            continue
        published.append((key, value))

    # --- 7. DLQ, after encrypt and after the rate limit ----------------------
    # Two payload sources, and the difference is the entire point of the stage
    # order (C3):
    #
    #   * An event rejected during validation was NEVER encrypted, so the only
    #     payload that exists is the plaintext request element. That is accepted
    #     only because the event failed before the encrypt stage -- it is a bad
    #     event, and redacting it would leave an operator with a DLQ record they
    #     cannot diagnose.
    #   * An event that WAS encrypted and then crossed the size limit is DLQ'd in
    #     its encrypted form. A plaintext payload there would make the DLQ a
    #     second PII store on a second topic, and a replayed record would arrive
    #     already carrying ciphertext to be encrypted again.
    dlq: list[tuple[int, str, dict, str]] = [
        (r.index, r.reason, _dlq_payload(items[r.index]), "validate") for r in rejections
    ]
    dlq.extend((r.index, r.reason, payload, "encrypt") for r, payload in encrypted_rejections)

    try:
        for index, reason, payload, stage in sorted(dlq, key=lambda entry: entry[0]):
            dlq_event = build_dlq_event(
                payload, reason=reason, index=index, stage=stage, original_topic=TOPIC_RAW
            )
            sink.sink_dlq(_dlq_key(payload), msgspec.json.encode(dlq_event))
    except (SinkUnavailable, OSError) as exc:
        # The DLQ is where a rejected event goes so the pipeline can move on. If
        # it cannot be written the event is not lost -- the client gets a 503 and
        # resends the whole batch with the same `id`s -- so failing loudly beats
        # acknowledging a rejection we could not record.
        raise SinkUnavailable(REASON_DLQ_UNAVAILABLE) from exc

    # --- 8. produce -----------------------------------------------------------
    try:
        for key, value in published:
            sink.sink(TOPIC_RAW, key, value)
    except (SinkUnavailable, OSError) as exc:
        raise SinkUnavailable(REASON_SINK_UNAVAILABLE) from exc

    return IngestResult(
        accepted=len(published),
        rejected=sorted(rejections + [r for r, _ in encrypted_rejections], key=lambda r: r.index),
        buffered=len(published),
    )
