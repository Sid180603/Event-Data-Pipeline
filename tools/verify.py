"""R3: the verification oracle -- one command that says whether the run balanced.

The demo's claim is falsifiable only if something compares the driver's ground truth
against what the system actually did, and the comparison has to be the same
comparison every time. That is this module: read the ledger the driver wrote, read
the two topics the gateway wrote, and print the difference.

## Why the arithmetic is a pure function and only the read is not

`reconcile()` takes plain dataclasses and returns a verdict. `parse_raw_messages()`
and `parse_dlq_messages()` turn bytes into those dataclasses. The one impure step is
`TopicReader.read()`, behind a seam with exactly one real implementation
(`KafkaTopicReader`) and a fake in the tests. So the part that can be wrong -- the
arithmetic that decides pass or fail -- is testable with no broker, and
`import tools.verify` never touches the network or constructs a Consumer. That
matters more than it sounds: a verifier that can only be run against live
infrastructure is a verifier nobody runs before the demo, and one that constructs a
client at import time cannot be imported on the CI box that has no broker either.

## A missing dimension is not a zero

"Unreadable" and "empty" are different sentences and this tool refuses to conflate
them. Three of them are load-bearing here:

* **A topic read that stopped before its end offsets is not a reconciliation.** It is
  a truncated view that happens to look balanced, and printing a scorecard for it is
  the one outcome worse than having no tool: it converts an unknown into a pass.
  `TopicSnapshot.drained` carries that, and `main` exits `EXIT_UNVERIFIED` without
  printing any scorecard at all.
* **A message that would not decode is counted, never dropped.** Silently skipping it
  would present a record nobody can vouch for as a record the ledger never emitted,
  which reads as loss rather than as a broken decoder.
* **A dimension not supplied is printed as `skipped`.** The pseudonym check needs a
  master secret the tool is not entitled to assume it has; the Cassandra count needs
  a stack that belongs to another team and is not installed here. Neither is printed
  as a passing zero.

## Reconciling on a field that changed shape

The ledger's field is named `user_pseudo` and holds the **ingress plaintext**
`user_id`; `career.events.raw` carries `data.candidate.user_id_pseudo`, the
`HMAC-SHA256(mac_key, user_id)` the gateway derived from it (`app.pseudonym.hmac`
via `app.crypto.facade.protect_candidate`). Those two strings are never equal. So
the join is not `ledger.user_pseudo == topic.user_id_pseudo`: it is
`ledger.user_pseudo -> pseudonymize(tenant mac_key, ...) == topic.user_id_pseudo`,
which needs the master secret and is reported as skipped when it is absent. Joining
on the strings directly would report a total loss on a run where nothing was lost,
and joining on nothing at all would leave the one field most likely to be mangled by
a pipeline bug unchecked.

`(source, id)` is the other join, and it is the one reconciliation runs on by
default. It is shape-stable across the ingress and egress forms, which is exactly
why `contracts.ledger.dedup_key` is defined over it rather than over `id` alone.

## The duplicate number is scoped, deliberately

Chaos 1 kills the gateway mid-flight and the driver resends the batch, so one
`(source, id)` reaches the topic twice **by construction**. A verifier that asserts
"duplicates == 0" therefore fails the scenario it is supposed to be measuring. So:
`stored` counts distinct `(source, id)`; `wire_duplicates` counts only the extra
copies; and the assertion is `wire_duplicates == --expect-duplicates`, which the
kill-gateway script passes the number it caused. The scorecard says so in words, so
nobody reads a nonzero duplicate count as a bug when it is the demo.

The same scoping appears in the ground truth: a `DUPLICATE_ID` injection copies the
partner's id onto the corrupted event (`driver.inject.corrupt`), so the ledger holds
two rows under one `(source, id)`. Counting rows would report a phantom loss of one.

## Exit code is the contract

`0` when every check passed, `EXIT_MISMATCH` (1) when something does not balance,
`EXIT_UNVERIFIED` (2) when the tool could not find out -- an unreadable topic, a
missing or empty ledger. 2 exists so a CI job or the demo runbook can tell "the
system is wrong" from "we did not look", which are not the same page.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import msgspec

from app.config import TOPIC_DLQ, TOPIC_RAW, Settings
from app.crypto.keys import derive_tenant_keys
from app.dlq.envelope import DlqEvent
from app.pseudonym.hmac import pseudonymize
from app.validate.events import (
    CODE_BAD_ID,
    CODE_BAD_SOURCE,
    CODE_BAD_TIME,
    CODE_DUPLICATE_ID,
    CODE_MIXED_TENANT,
    CODE_OVERSIZED,
    CODE_RAW_SUBJECT,
    CODE_SCHEMA,
    CODE_UNKNOWN_ATTR,
)
from contracts.cloudevent import CloudEvent
from contracts.ledger import Ledger, LedgerRecord, dedup_key

#: Every reason CODE `app.validate.events` can produce, imported rather than
#: retyped: a code added there and missed here would show up as an unknown code in
#: the DLQ breakdown, which is the failure mode worth having.
KNOWN_CODES: frozenset[str] = frozenset(
    {
        CODE_SCHEMA,
        CODE_UNKNOWN_ATTR,
        CODE_DUPLICATE_ID,
        CODE_MIXED_TENANT,
        CODE_BAD_TIME,
        CODE_BAD_SOURCE,
        CODE_BAD_ID,
        CODE_RAW_SUBJECT,
        CODE_OVERSIZED,
    }
)

EXIT_OK = 0
EXIT_MISMATCH = 1
#: The tool could not find out. Distinct from a mismatch so "we did not look" is
#: never reported with the same authority as "the system is wrong".
EXIT_UNVERIFIED = 2

#: The ledger the driver writes by default (`driver.main.DEFAULT_LEDGER`).
DEFAULT_LEDGER = Path("ledger.jsonl")
#: `app.config.Settings.kafka_bootstrap_servers` falls back to this.
DEFAULT_BOOTSTRAP = "localhost:9092"
#: How long one topic read may take before it is called unverified. Long enough for a
#: 5M-record topic on a slow box, short enough that a dead broker is reported rather
#: than waited on.
DEFAULT_TIMEOUT_SECONDS = 30.0
#: Rows of the per-source and per-type tables before the rest are summarised. One
#: screen: 500 tenants do not fit on a screen a person reads during a demo.
DEFAULT_GROUP_ROWS = 8
#: Failure lines printed before the remainder is summarised, same reason.
DEFAULT_FAILURE_ROWS = 5

#: Where a rejection lands in the per-`type` table when its `original_payload` named
#: no type -- an empty payload, which `_dlq_payload` really does produce. A visible
#: bucket rather than a skipped one: "the DLQ holds a rejection we cannot place" is
#: information, and folding it into a neighbouring type would invent one.
_UNGROUPED = "(unattributable)"


# --- what was read off the wire ----------------------------------------------


@dataclass(frozen=True, slots=True)
class TopicMessage:
    topic: str
    partition: int
    offset: int
    value: bytes


@dataclass(frozen=True, slots=True)
class RawRecord:
    """One record on `career.events.raw`, reduced to the joinable fields.

    `user_pseudo` is the EGRESS pseudonym, `seq` the `sequence` extension already
    coerced to an int or None when it is not comparable. Both are named as the
    topic has them, so nothing here can be confused with the ledger's same-named
    field, which holds the ingress plaintext.
    """

    source: str
    id: str
    type: str
    user_pseudo: str
    seq: int | None
    partition: int
    offset: int


@dataclass(frozen=True, slots=True)
class DlqRecord:
    """One record on `career.events.dlq`.

    `code` is the leading token of `data.error_context.reason` with any trailing
    colon stripped: `SCHEMA at $.type` and `MIXED_TENANT: spans more than one
    tenant` are both `"<CODE> ..."`, and the field path is dropped because the
    breakdown is by code. The rejected event's own identity is read out of
    `data.original_payload`, which is the only place it survives -- and which is
    empty when the request element was not an object at all, hence the `None`s.
    """

    source: str
    code: str
    rejected_source: str | None
    rejected_id: str | None
    rejected_type: str | None
    partition: int
    offset: int


def _as_sequence(value: str | int | None) -> int | None:
    """`app.metrics._as_sequence` with "raise" turned into "untrackable".

    The metrics layer raises so it can count the record in
    `gateway_ordering_unchecked_total`; here the record still has to be reconciled,
    so an uncomparable sequence becomes None and is counted as unchecked. Same
    rule, same reason: treating it as 0 would make every later event for that
    (source, user) look like a violation.
    """
    if type(value) is int:
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _code_from_reason(reason: str) -> str:
    """The leading CODE of a rejection reason, stripped of a trailing colon."""
    head = reason.split(maxsplit=1)
    if not head:
        return ""
    return head[0].rstrip(":")


def parse_raw_messages(messages: Iterable[TopicMessage]) -> tuple[list[RawRecord], int]:
    """Decode `career.events.raw` messages into `RawRecord`s.

    Returns `(records, undecodable)`. The count is the whole reason this returns a
    tuple: a message that does not decode as a `CloudEvent` is not evidence of
    anything, and dropping it would let it masquerade as an event the ledger never
    emitted.
    """
    records: list[RawRecord] = []
    undecodable = 0
    for message in messages:
        try:
            event = msgspec.json.decode(message.value, type=CloudEvent)
        except msgspec.DecodeError:
            undecodable += 1
            continue
        records.append(
            RawRecord(
                source=event.source,
                id=event.id,
                type=event.type,
                user_pseudo=event.data.candidate.user_id_pseudo,
                seq=_as_sequence(event.sequence),
                partition=message.partition,
                offset=message.offset,
            )
        )
    return records, undecodable


def parse_dlq_messages(messages: Iterable[TopicMessage]) -> tuple[list[DlqRecord], int]:
    """Decode `career.events.dlq` messages into `DlqRecord`s. See `parse_raw_messages`."""
    records: list[DlqRecord] = []
    undecodable = 0
    for message in messages:
        try:
            event = msgspec.json.decode(message.value, type=DlqEvent)
        except msgspec.DecodeError:
            undecodable += 1
            continue
        payload = event.data.original_payload
        records.append(
            DlqRecord(
                source=event.source,
                code=_code_from_reason(event.data.error_context.reason),
                rejected_source=_payload_str(payload, "source"),
                rejected_id=_payload_str(payload, "id"),
                rejected_type=_payload_str(payload, "type"),
                partition=message.partition,
                offset=message.offset,
            )
        )
    return records, undecodable


def _payload_str(payload: Mapping[str, object], field: str) -> str | None:
    """One string field of `original_payload`, or None when it is absent or not a str.

    None rather than a placeholder: an unattributable rejection is a real category
    and is reported as one.
    """
    value = payload.get(field)
    return value if isinstance(value, str) else None


# --- the seam -----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TopicSnapshot:
    """Everything one topic read produced, and whether it read everything.

    `drained` is the field this whole module is built around. A consumer that stops
    early -- a timeout, a broker that vanishes mid-poll -- returns a perfectly
    ordinary-looking list of records and a reconciliation of that list would look
    clean. So the read says whether it reached every partition's end offset, and
    `main` refuses to print a scorecard when it did not.
    """

    topic: str
    messages: tuple[TopicMessage, ...]
    drained: bool


class TopicReader(Protocol):
    """Read a topic from its beginning to its end offsets.

    A protocol rather than a concrete type so `main` can take a fake, exactly as
    `app.kafka.producer.ProducerClient` exists so the producer can be tested without
    a broker.
    """

    def read(self, topic: str, *, timeout: float) -> TopicSnapshot: ...


class BrokerUnreachable(RuntimeError):
    """The topic could not be read at all. Never a reconciliation result."""


def build_consumer(config: dict):
    """The real `confluent_kafka.Consumer`, imported here and nowhere else.

    Imported inside the function for the reason `app.kafka.producer.build_producer`
    does it: `tools/verify.py` must be importable -- and unit-testable -- on a
    machine with no Kafka client and no broker.
    """
    from confluent_kafka import Consumer

    return Consumer(config)


class KafkaTopicReader:
    """Read one topic end to end, from the retention floor to its end offsets.

    Assigns explicit offsets rather than subscribing: a group join is a rebalance,
    a rebalance is a wait, and reconciliation wants a fixed window of the log that
    does not move under it. Nothing here commits, because nothing here writes.

    `consumer_factory` exists so the read loop -- the watermark arithmetic and the
    drained/not-drained decision, which is where a verifier quietly lies -- is
    testable against a stand-in. It defaults to the real client, and the real client
    is constructed per read rather than at construction so building a reader never
    needs a broker.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        poll_seconds: float = 0.5,
        consumer_factory=None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.timeout = timeout
        self.poll_seconds = poll_seconds
        self._factory = consumer_factory or build_consumer

    def read(self, topic: str, *, timeout: float | None = None) -> TopicSnapshot:
        from confluent_kafka import TopicPartition

        budget = self.timeout if timeout is None else timeout
        deadline = time.monotonic() + budget
        consumer = self._factory(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": f"verify-{topic}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                # Long enough that a 5M-record topic drains inside `budget` and short
                # enough that a broker which is not there fails fast rather than at
                # the end of a thirty-second silence.
                "socket.timeout.ms": 10_000,
            }
        )
        messages: list[TopicMessage] = []
        try:
            try:
                end_offsets = self._end_offsets(consumer, topic, budget)
            except Exception as exc:  # noqa: BLE001 - re-raised as our own type
                raise BrokerUnreachable(f"{topic}: {exc}") from exc
            consumer.assign([TopicPartition(topic, p, low) for p, (low, _high) in end_offsets.items()])
            # Only the high watermark: the low one is where the read starts and is
            # already spent. Keeping both here is how a "drained" flag ends up
            # comparing an offset against a tuple.
            remaining = {p: high for p, (_low, high) in end_offsets.items()}
            while remaining:
                message = consumer.poll(self.poll_seconds)
                if message is None:
                    if time.monotonic() >= deadline:
                        break
                    continue
                error = message.error()
                if error is not None:
                    raise BrokerUnreachable(f"{topic}: {error}")
                partition = message.partition()
                messages.append(
                    TopicMessage(
                        topic=topic,
                        partition=partition,
                        offset=message.offset(),
                        value=message.value(),
                    )
                )
                # A partition is drained once we have seen a record at its high
                # watermark, which is the offset one past the last retained record.
                # The `pop` default absorbs a record from an already-drained
                # partition rather than raising, because losing a message to an
                # assumption about ordering would be worse than counting it twice.
                if message.offset() + 1 >= remaining.get(partition, 0):
                    remaining.pop(partition, None)
        except BrokerUnreachable:
            raise
        except Exception as exc:  # noqa: BLE001 - any client failure is the same sentence
            raise BrokerUnreachable(f"{topic}: {exc}") from exc
        finally:
            consumer.close()
        return TopicSnapshot(topic=topic, messages=tuple(messages), drained=not remaining)

    @staticmethod
    def _end_offsets(consumer, topic: str, budget: float) -> dict[int, tuple[int, int]]:
        """Every partition's `(low, high)` watermark. An empty topic has none."""
        from confluent_kafka import TopicPartition

        metadata = consumer.list_topics(topic=topic, timeout=budget)
        found = metadata.topics.get(topic)
        if found is None or found.error is not None:
            raise BrokerUnreachable(f"{topic}: {found.error if found else 'topic not found'}")
        return {
            partition: consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=budget, cached=False
            )
            for partition in sorted(found.partitions)
        }


# --- the reconciliation -------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GroupDelta:
    """One `source` or one `type`, ledger against topic.

    `delta` is `stored - sent`, the direction the plan asks for and the one an
    operator reads first: negative means the topic holds less than the driver says
    it sent. `unresolved` is the count of ledger keys in this group that are on
    neither the topic nor the DLQ, which is what makes a delta of 0 with a missing
    event visible rather than balanced.
    """

    key: str
    sent: int
    stored: int
    rejected: int
    unresolved: int

    @property
    def delta(self) -> int:
        return self.stored - self.sent


@dataclass(slots=True)
class Reconciliation:
    """The whole verdict. `ok` is derived, never passed in."""

    ledger_rows: int
    expected: int
    ledger_duplicates: int
    accepted: int
    stored: int
    wire_duplicates: int
    missing: int
    unexpected: int
    dlq: int
    dlq_by_code: dict[str, int]
    unattributed_dlq: int
    unknown_codes: tuple[str, ...]
    by_source: tuple[GroupDelta, ...]
    by_type: tuple[GroupDelta, ...]
    ordering_violations: int
    ordering_unchecked: int
    seq_mismatches: int
    pseudonyms_checked: int
    pseudonym_mismatches: int | None
    undecodable: int
    drained: bool
    stored_partitions: dict[int, int]
    stored_end_offsets: dict[int, int]
    failures: tuple[str, ...] = ()
    ok: bool = False

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "counts": {
                "ledger_rows": self.ledger_rows,
                "expected": self.expected,
                "ledger_duplicates": self.ledger_duplicates,
                "accepted": self.accepted,
                "stored": self.stored,
                "wire_duplicates": self.wire_duplicates,
                "missing": self.missing,
                "unexpected": self.unexpected,
                "dlq": self.dlq,
                "unattributed_dlq": self.unattributed_dlq,
                "undecodable": self.undecodable,
                "ordering_violations": self.ordering_violations,
                "ordering_unchecked": self.ordering_unchecked,
                "seq_mismatches": self.seq_mismatches,
                "pseudonyms_checked": self.pseudonyms_checked,
                # None stays None. `json` renders it as null, which is the honest
                # reading of a check that was not run.
                "pseudonym_mismatches": self.pseudonym_mismatches,
                "drained": self.drained,
            },
            "dlq_by_code": dict(sorted(self.dlq_by_code.items())),
            "unknown_codes": list(self.unknown_codes),
            "by_source": [_group_dict(g) for g in self.by_source],
            "by_type": [_group_dict(g) for g in self.by_type],
            "stored_partitions": {str(k): v for k, v in sorted(self.stored_partitions.items())},
            "stored_end_offsets": {str(k): v for k, v in sorted(self.stored_end_offsets.items())},
            "failures": list(self.failures),
        }


def _group_dict(group: GroupDelta) -> dict:
    return {
        "key": group.key,
        "sent": group.sent,
        "stored": group.stored,
        "rejected": group.rejected,
        "delta": group.delta,
        "unresolved": group.unresolved,
    }


def ordering_violations(records: Sequence[RawRecord]) -> int:
    """Events whose `sequence` went backwards for a `(source, user)` pair.

    A pure function of records already read, on purpose: the gateway's own
    `gateway_ordering_violations_total` is a counter over what one worker saw, and
    this is the end-to-end version over what is on the topic -- the same guarantee
    checked from the reader's side instead of adding a second mechanism. It applies
    `app.metrics._SequenceWindow`'s rule verbatim (lower than the high-water mark is a
    violation, equal is not) so the two counters cannot disagree about the same run.

    Read in topic order, which is why one `(source, user)` pair has to be on one
    partition for the answer to mean anything: the derived key is
    `career_site_id|user_id_pseudo` (`contracts.attributes.derive_kafka_key`), and a
    pair spread across partitions is interleaved here in a way no producer's ordering
    promise covers. That shows up as a violation, which is the intended outcome --
    sticky routing failing should be visible, not smoothed over.
    """
    high: dict[tuple[str, str], int] = {}
    violations = 0
    for record in records:
        if record.seq is None:
            continue
        key = (record.source, record.user_pseudo)
        seen = high.get(key)
        if seen is not None and record.seq < seen:
            violations += 1
        high[key] = record.seq if seen is None else max(seen, record.seq)
    return violations


def derive_expected_pseudonyms(
    records: Sequence[LedgerRecord], master_secret: bytes
) -> dict[tuple[str, str], str]:
    """`{(source, id): expected user_id_pseudo}` for the ledger's plaintext ids.

    This is the whole point of matching the two user fields rather than comparing
    them. The ledger carries what the client sent; the topic carries
    `pseudonymize(mac_key, that)` with `mac_key` derived per tenant by
    `app.crypto.keys`. Re-deriving with the gateway's own functions means a change to
    the derivation breaks this check loudly instead of silently turning every user
    into a mismatch.

    Keyed by `(source, id)` because that is the reconciliation key, and because
    `mac_key` is per tenant so the derivation is not even well defined across
    sources.

    Raises `ValueError` for a master secret shorter than
    `MIN_MASTER_SECRET_BYTES`, by way of `derive_tenant_keys`: there is no such thing
    as reconciling pseudonyms with a passphrase guess, and silently deriving from
    one would produce a confident wrong answer.
    """
    settings = Settings(master_secret=master_secret)
    tags: dict[tuple[str, str], str] = {}
    for record in records:
        key = dedup_key(record.source, record.id)
        if key in tags:
            continue
        keys = derive_tenant_keys(
            settings.master_secret,
            settings.tenant_key_salt(record.tenant),
            career_site_id=record.tenant,
            key_version=settings.key_version,
        )
        tags[key] = pseudonymize(keys.mac_key, record.user_pseudo)
    return tags


def reconcile(
    ledger: Sequence[LedgerRecord],
    stored: Sequence[RawRecord],
    rejected: Sequence[DlqRecord],
    *,
    expected_dlq: int | None = None,
    expected_duplicates: int = 0,
    expected_pseudonyms: Mapping[tuple[str, str], str] | None = None,
    drained: bool = True,
    undecodable: int = 0,
) -> Reconciliation:
    """The ledger against the topic and the DLQ. No I/O, no clock, no globals.

    `expected_dlq` is the driver's injected count and `expected_duplicates` the
    number of resends the scenario caused; both default to "not asserted" (the
    former) and "none" (the latter). `expected_pseudonyms` comes from
    `derive_expected_pseudonyms` and is optional because it needs a master secret;
    when it is None the pseudonym dimension is reported as unmeasured, not as zero.
    """
    ledger_keys = {dedup_key(r.source, r.id) for r in ledger}
    ledger_seq = {dedup_key(r.source, r.id): r.seq for r in ledger}
    stored_keys = {dedup_key(r.source, r.id) for r in stored}
    rejected_keys = {
        dedup_key(r.rejected_source, r.rejected_id)
        for r in rejected
        if r.rejected_source and r.rejected_id
    }

    accounted = stored_keys | rejected_keys
    missing = ledger_keys - accounted
    unexpected = stored_keys - ledger_keys
    unattributed = [r for r in rejected if not _attributable(r, ledger_keys)]

    dlq_by_code: dict[str, int] = {}
    for record in rejected:
        dlq_by_code[record.code] = dlq_by_code.get(record.code, 0) + 1
    unknown_codes = tuple(sorted(set(dlq_by_code) - KNOWN_CODES))

    seq_mismatches = sum(
        1
        for record in stored
        if record.seq is not None
        and (expected_seq := ledger_seq.get(dedup_key(record.source, record.id))) is not None
        and expected_seq != record.seq
    )

    pseudonyms_checked = 0
    pseudonym_mismatches: int | None = None
    if expected_pseudonyms is not None:
        mismatches = 0
        for record in stored:
            expected = expected_pseudonyms.get(dedup_key(record.source, record.id))
            if expected is None:
                continue
            pseudonyms_checked += 1
            if expected != record.user_pseudo:
                mismatches += 1
        pseudonym_mismatches = mismatches

    failures: list[str] = []
    if not drained:
        failures.append("the topic read did not reach every partition's end offsets")
    if undecodable:
        failures.append(f"{undecodable} topic message(s) could not be decoded")
    if missing:
        failures.append(
            f"{missing} ledger (source, id) pair(s) were neither stored nor rejected"
            + _sample(sorted(missing))
        )
    if unexpected:
        failures.append(
            f"{unexpected} stored (source, id) pair(s) are absent from the ledger"
            + _sample(sorted(unexpected - ledger_keys))
        )
    if unattributed:
        failures.append(
            f"{len(unattributed)} DLQ record(s) could not be attributed to a ledger (source, id)"
        )
    if expected_dlq is not None and len(rejected) != expected_dlq:
        failures.append(f"DLQ holds {len(rejected)} record(s), expected {expected_dlq}")
    if unknown_codes:
        failures.append(f"DLQ reports codes the gateway does not define: {', '.join(unknown_codes)}")
    duplicates = len(stored) - len(stored_keys)
    if duplicates != expected_duplicates:
        plural = "" if duplicates == 1 else "s"
        failures.append(
            f"{duplicates} wire duplicate{plural} after (source, id) dedup, "
            f"expected {expected_duplicates}"
        )
    violations = ordering_violations(stored)
    if violations:
        failures.append(
            f"{violations} event(s) arrived with a sequence lower than one already seen "
            "for their (source, user)"
        )
    if seq_mismatches:
        failures.append(f"{seq_mismatches} stored sequence value(s) disagree with the ledger")
    if pseudonym_mismatches:
        failures.append(
            f"{pseudonym_mismatches} stored user_id_pseudo value(s) are not the expected HMAC "
            "of the ledger's user_id"
        )

    return Reconciliation(
        ledger_rows=len(ledger),
        expected=len(ledger_keys),
        ledger_duplicates=len(ledger) - len(ledger_keys),
        accepted=len(ledger_keys & accounted),
        stored=len(stored_keys),
        wire_duplicates=len(stored) - len(stored_keys),
        missing=len(missing),
        unexpected=len(unexpected),
        dlq=len(rejected),
        dlq_by_code=dlq_by_code,
        unattributed_dlq=len(unattributed),
        unknown_codes=unknown_codes,
        by_source=_group_deltas(
            ledger, stored, rejected, _source_of, lambda r: r.source, _rejected_source
        ),
        by_type=_group_deltas(
            ledger,
            stored,
            rejected,
            _type_of,
            lambda r: r.type,
            lambda r: r.rejected_type or _UNGROUPED,
        ),
        ordering_violations=violations,
        ordering_unchecked=sum(1 for r in stored if r.seq is None),
        seq_mismatches=seq_mismatches,
        pseudonyms_checked=pseudonyms_checked,
        pseudonym_mismatches=pseudonym_mismatches,
        undecodable=undecodable,
        drained=drained,
        stored_partitions=_partition_counts(stored),
        stored_end_offsets=_end_offsets(stored),
        failures=tuple(failures),
        ok=not failures,
    )


def _sample(keys: Sequence[tuple[str, str]], limit: int = 3) -> str:
    """Up to `limit` offending keys, so a failure names what to go and look at.

    Three and not all of them: a 5M-event run that lost everything must not print
    5M lines, and a scorecard nobody can read is not a scorecard. The counts above
    the sample are the claim; this is the pointer.
    """
    head = ", ".join(f"{source} {event_id}" for source, event_id in keys[:limit])
    return f" (e.g. {head})" if head else ""


def _attributable(record: DlqRecord, ledger_keys: set[tuple[str, str]]) -> bool:
    """Whether a rejection names an event the ledger recorded.

    False for a DLQ record whose `original_payload` was empty -- `_dlq_payload`
    returns `{}` when the request element was not a JSON object -- and for one that
    rejects an id the ground truth never mentions. Both are reported rather than
    quietly counted into `accepted`.
    """
    if not record.rejected_source or not record.rejected_id:
        return False
    return dedup_key(record.rejected_source, record.rejected_id) in ledger_keys


def _source_of(record: LedgerRecord) -> str:
    return record.source


def _type_of(record: LedgerRecord) -> str:
    return record.type


def _rejected_source(record: DlqRecord) -> str:
    """A rejection's group is the event it rejected, not the DLQ envelope's own
    `source`: `build_dlq_event` falls back to `/careers/_unknown` when the payload's
    source is malformed, which would file every `BAD_SOURCE` rejection under a tenant
    that sent nothing.
    """
    return record.rejected_source or record.source


def _group_deltas(
    ledger: Sequence[LedgerRecord],
    stored: Sequence[RawRecord],
    rejected: Sequence[DlqRecord],
    ledger_group,
    stored_group,
    rejected_group,
) -> tuple[GroupDelta, ...]:
    """Per-group sent/stored/rejected/unresolved, one row per group name.

    Three accessors rather than one, because the three record shapes name a group
    differently -- a rejection's group is the *rejected* event's. Rows are sorted so
    two runs of the same data print the same table, which is what makes the diff of
    two scorecards readable.
    """
    sent: dict[str, set[tuple[str, str]]] = {}
    for record in ledger:
        sent.setdefault(ledger_group(record), set()).add(dedup_key(record.source, record.id))

    stored_by: dict[str, set[tuple[str, str]]] = {}
    for record in stored:
        stored_by.setdefault(stored_group(record), set()).add(dedup_key(record.source, record.id))

    rejected_by: dict[str, int] = {}
    rejected_keys: dict[str, set[tuple[str, str]]] = {}
    for record in rejected:
        name = rejected_group(record)
        rejected_by[name] = rejected_by.get(name, 0) + 1
        if record.rejected_source and record.rejected_id:
            rejected_keys.setdefault(name, set()).add(
                dedup_key(record.rejected_source, record.rejected_id)
            )

    rows = []
    for name in sorted(set(sent) | set(stored_by) | set(rejected_by)):
        sent_keys = sent.get(name, set())
        rows.append(
            GroupDelta(
                key=name,
                sent=len(sent_keys),
                stored=len(stored_by.get(name, set())),
                rejected=rejected_by.get(name, 0),
                unresolved=len(sent_keys - stored_by.get(name, set()) - rejected_keys.get(name, set())),
            )
        )
    return tuple(rows)


def _partition_counts(records: Sequence[RawRecord]) -> dict[int, int]:
    counts: dict[int, int] = {}
    for record in records:
        counts[record.partition] = counts.get(record.partition, 0) + 1
    return dict(sorted(counts.items()))


def _end_offsets(records: Sequence[RawRecord]) -> dict[int, int]:
    """The offset one past the last record seen per partition: "where we got to"."""
    ends: dict[int, int] = {}
    for record in records:
        ends[record.partition] = max(ends.get(record.partition, 0), record.offset + 1)
    return dict(sorted(ends.items()))


# --- the scorecard ------------------------------------------------------------


def _n(value: int) -> str:
    return f"{value:,}"


def render_scorecard(result: Reconciliation, *, ledger_path: str, verbose: bool = False) -> str:
    """One screen. Read live during a demo, so the headline is the first line.

    Everything past the headline is grouped and capped: a per-tenant table for 500
    tenants is not a scorecard, it is a wall. `--verbose` prints the remainder.
    """
    lines = [
        f"ledger {ledger_path}  {_n(result.ledger_rows)} rows, "
        f"{_n(result.expected)} distinct (source, id)",
        "",
        f"sent {_n(result.ledger_rows)}   accepted {_n(result.accepted)}   "
        f"stored {_n(result.stored)}   duplicates {_n(result.wire_duplicates)}   "
        f"dlq {_n(result.dlq)}",
        f"missing {_n(result.missing)}   unexpected {_n(result.unexpected)}   "
        f"ledger-duplicate rows {_n(result.ledger_duplicates)}   "
        f"undecodable {_n(result.undecodable)}",
        f"ordering violations {_n(result.ordering_violations)}   "
        f"unchecked {_n(result.ordering_unchecked)}   "
        f"sequence mismatches {_n(result.seq_mismatches)}",
        f"pseudonyms  {_pseudonym_line(result)}",
        f"cassandra  skipped (another team's stack; no reader here and no dependency on it)",
        f"offsets  {_offset_line(result)}",
    ]
    lines.extend(_group_lines("sources", result.by_source, verbose))
    lines.extend(_group_lines("types", result.by_type, verbose))
    lines.extend(_dlq_lines(result, verbose))
    lines.append(
        "note  duplicates are counted after (source, id) dedup; the kill-gateway "
        "scenario produces them by construction"
    )
    lines.append("")
    lines.append("PASS" if result.ok else "FAIL")
    for failure in result.failures[: None if verbose else DEFAULT_FAILURE_ROWS]:
        lines.append(f"  - {failure}")
    hidden = len(result.failures) - (len(result.failures) if verbose else DEFAULT_FAILURE_ROWS)
    if hidden > 0:
        lines.append(f"  - ... and {hidden} more failure(s); re-run with --verbose")
    return "\n".join(lines)


def _pseudonym_line(result: Reconciliation) -> str:
    if result.pseudonym_mismatches is None:
        return "skipped (no master secret)"
    return f"{_n(result.pseudonyms_checked)} checked, {_n(result.pseudonym_mismatches)} mismatched"


def _offset_line(result: Reconciliation) -> str:
    """How far into the log the read got. Records read, not distinct keys.

    `stored + wire_duplicates` rather than `stored`: the offsets line is about what
    came off the topic, and printing the deduped count here would understate the
    volume a run actually produced by exactly the number the headline calls
    duplicates.
    """
    ends = list(result.stored_end_offsets.items())
    shown = ", ".join(f"p{partition}={offset}" for partition, offset in ends[:DEFAULT_GROUP_ROWS])
    hidden = len(ends) - DEFAULT_GROUP_ROWS
    if hidden > 0:
        shown += f" (+{hidden} more)"
    return (
        f"{_n(result.stored + result.wire_duplicates)} records over "
        f"{len(result.stored_partitions)} partition(s), end offsets {shown or 'none'}"
    )


def _group_lines(
    title: str, groups: Sequence[GroupDelta], verbose: bool
) -> list[str]:
    shown = groups if verbose else groups[:DEFAULT_GROUP_ROWS]
    header = (
        f"{title}  {len(groups)}"
        if len(shown) == len(groups)
        else f"{title}  {len(groups)} ({len(shown)} shown, {len(groups) - len(shown)} more {title})"
    )
    lines = [header]
    for group in shown:
        lines.append(
            f"  {group.key}  sent {group.sent}  stored {group.stored}  dlq {group.rejected}  "
            f"delta {group.delta}  unresolved {group.unresolved}"
        )
    return lines


def _dlq_lines(result: Reconciliation, verbose: bool) -> list[str]:
    if not result.dlq_by_code:
        return ["dlq by code  none"]
    codes = sorted(result.dlq_by_code.items())
    if not verbose and len(codes) > DEFAULT_GROUP_ROWS:
        head = codes[:DEFAULT_GROUP_ROWS]
        rest = sum(count for _code, count in codes[DEFAULT_GROUP_ROWS:])
        rendered = "  ".join(f"{code} {count}" for code, count in head)
        return [f"dlq by code  {rendered}  (+{rest} in {len(codes) - len(head)} more codes)"]
    return ["dlq by code  " + "  ".join(f"{code} {count}" for code, count in codes)]


# --- the CLI ------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.verify",
        description="Reconcile the driver's ledger against career.events.raw and "
        "career.events.dlq, and print the scorecard.",
    )
    parser.add_argument(
        "--ledger",
        type=Path,
        default=DEFAULT_LEDGER,
        help=f"ground-truth ledger to reconcile against. Default: {DEFAULT_LEDGER}",
    )
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP),
        help="broker list. Default: $KAFKA_BOOTSTRAP, else " + DEFAULT_BOOTSTRAP,
    )
    parser.add_argument("--topic-raw", default=TOPIC_RAW, help=f"default: {TOPIC_RAW}")
    parser.add_argument("--topic-dlq", default=TOPIC_DLQ, help=f"default: {TOPIC_DLQ}")
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        metavar="SECONDS",
        help=f"per-topic read budget. Default: {DEFAULT_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--expect-dlq",
        type=int,
        default=None,
        metavar="N",
        help="the driver's injected count, from InjectionReport.injected. Omitted: the "
        "DLQ depth is reported but not asserted.",
    )
    parser.add_argument(
        "--expect-duplicates",
        type=int,
        default=0,
        metavar="N",
        help="wire duplicates the scenario caused, counted after (source, id) dedup. "
        "The kill-gateway script passes what it resent. Default 0.",
    )
    parser.add_argument(
        "--master-secret",
        default=None,
        help="master secret, to reconcile the ledger's plaintext user_id against the "
        "topic's HMAC pseudonym. Default: $MASTER_SECRET. Omitted: that check is skipped.",
    )
    parser.add_argument("--json", action="store_true", help="machine-readable report")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print every group row and every failure instead of the first few",
    )
    return parser


def main(argv: Sequence[str] | None = None, *, reader: TopicReader | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.expect_duplicates < 0:
        parser.error("--expect-duplicates must be non-negative")
    if args.expect_dlq is not None and args.expect_dlq < 0:
        parser.error("--expect-dlq must be non-negative")

    path = Path(args.ledger)
    if not path.is_file():
        print(f"no such ledger: {path}")
        return EXIT_UNVERIFIED
    try:
        with Ledger(path) as ledger_file:
            ledger = ledger_file.read_all()
    except (ValueError, json.JSONDecodeError, KeyError, TypeError) as exc:
        # A half-written last line is what a kill during the driver's own write looks
        # like, and a traceback here would bury the reason the run was being verified
        # at all. The line number is included because "the last line" is an assumption
        # this tool is in no position to make.
        print(f"could not read the ledger {path}: {exc}")
        return EXIT_UNVERIFIED
    if not ledger:
        print(f"ledger {path} holds no rows: there is nothing to reconcile against")
        return EXIT_UNVERIFIED

    if reader is None:
        reader = KafkaTopicReader(args.bootstrap, timeout=args.timeout)

    try:
        raw_snapshot = reader.read(args.topic_raw, timeout=args.timeout)
        dlq_snapshot = reader.read(args.topic_dlq, timeout=args.timeout)
    except (BrokerUnreachable, OSError) as exc:
        print(f"could not read Kafka: {exc}")
        return EXIT_UNVERIFIED
    for snapshot in (raw_snapshot, dlq_snapshot):
        if not snapshot.drained:
            print(
                f"could not read Kafka: {snapshot.topic} was not drained to its end offsets "
                "within the timeout, so there is no complete read to reconcile"
            )
            return EXIT_UNVERIFIED

    stored, raw_undecodable = parse_raw_messages(raw_snapshot.messages)
    rejected, dlq_undecodable = parse_dlq_messages(dlq_snapshot.messages)

    secret = args.master_secret if args.master_secret is not None else os.getenv("MASTER_SECRET")
    try:
        expected_pseudonyms = derive_expected_pseudonyms(ledger, secret.encode()) if secret else None
    except ValueError as exc:
        print(f"could not derive the expected user_id_pseudo values: {exc}")
        return EXIT_UNVERIFIED

    result = reconcile(
        ledger,
        stored,
        rejected,
        expected_dlq=args.expect_dlq,
        expected_duplicates=args.expect_duplicates,
        expected_pseudonyms=expected_pseudonyms,
        undecodable=raw_undecodable + dlq_undecodable,
    )

    if args.json:
        document = result.to_dict()
        document["ledger"] = str(path)
        document["topics"] = {"raw": args.topic_raw, "dlq": args.topic_dlq}
        print(json.dumps(document, indent=2))
    else:
        print(render_scorecard(result, ledger_path=str(path), verbose=args.verbose))
    return EXIT_OK if result.ok else EXIT_MISMATCH


__all__ = [
    "DEFAULT_BOOTSTRAP",
    "DEFAULT_GROUP_ROWS",
    "DEFAULT_LEDGER",
    "DEFAULT_TIMEOUT_SECONDS",
    "EXIT_MISMATCH",
    "EXIT_OK",
    "EXIT_UNVERIFIED",
    "KNOWN_CODES",
    "BrokerUnreachable",
    "DlqRecord",
    "GroupDelta",
    "KafkaTopicReader",
    "RawRecord",
    "Reconciliation",
    "TopicMessage",
    "TopicReader",
    "TopicSnapshot",
    "build_consumer",
    "build_parser",
    "derive_expected_pseudonyms",
    "main",
    "ordering_violations",
    "parse_dlq_messages",
    "parse_raw_messages",
    "reconcile",
    "render_scorecard",
]


if __name__ == "__main__":
    raise SystemExit(main())