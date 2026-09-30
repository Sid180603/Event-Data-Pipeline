"""Batch validation. Rejection is per-event, never per-batch (plan D6)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import msgspec

from app.config import MAX_EVENT_BYTES
from contracts import attributes as A
from contracts.cloudevent import CloudEvent
from contracts.ledger import dedup_key

# Reason CODES, not messages. A rejection reason is written to the DLQ, and a
# message built from the offending value would put that value into a second
# Kafka topic (C3).
CODE_SCHEMA = "SCHEMA"
CODE_UNKNOWN_ATTR = "UNKNOWN_ATTRIBUTE"
CODE_DUPLICATE_ID = "DUPLICATE_ID"
CODE_MIXED_TENANT = "MIXED_TENANT"
CODE_BAD_TIME = "BAD_TIME"
CODE_BAD_SOURCE = "BAD_SOURCE"
CODE_BAD_ID = "BAD_ID"
CODE_RAW_SUBJECT = "RAW_SUBJECT"
CODE_OVERSIZED = "OVERSIZED"

_PROBLEM_FIELD = {
    CODE_BAD_TIME: "$.time",
    CODE_BAD_SOURCE: "$.source",
    CODE_BAD_ID: "$.id",
    CODE_RAW_SUBJECT: "$.subject",
}


@dataclass(frozen=True, slots=True)
class Rejection:
    index: int
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {"index": self.index, "reason": self.reason}


@dataclass(slots=True)
class BatchOutcome:
    events: list[CloudEvent] = field(default_factory=list)
    rejections: list[Rejection] = field(default_factory=list)

    @property
    def accepted(self) -> int:
        return len(self.events)

    @property
    def rejected(self) -> list[Rejection]:
        return self.rejections

    def to_response(self) -> dict[str, Any]:
        """The `202 {accepted, rejected}` shape in CONTRACT.md section 2."""
        return {"accepted": self.accepted, "rejected": [r.to_dict() for r in self.rejections]}


#: msgspec puts the path at the END of the message but the offending VALUE in
#: the prefix -- e.g. "Invalid enum value 'com.careerpage.career.not-real' - at
#: `$.type`". Its `.path` attribute is None for enum errors, so the only way to
#: learn the field is to parse the message. We take the trailing path and
#: discard the rest, because a rejection reason is written to the DLQ and must
#: not carry the offending value into a second Kafka topic.
_PATH_TAIL = re.compile(r"at `([^`]*)`\s*$")


def _error_path(exc: Exception) -> str:
    path = getattr(exc, "path", None)
    if path:
        return "$" + "".join(f"[{p!r}]" if isinstance(p, int) else f".{p}" for p in path)
    tail = _PATH_TAIL.search(str(exc))
    return tail.group(1) if tail else "$"


def validate_batch(
    raw_events: list[Any], *, max_bytes: int = MAX_EVENT_BYTES
) -> BatchOutcome:
    """Decode and validate a CloudEvents batch.

    Check order is deliberate: tenant homogeneity runs BEFORE duplicate-id, so a
    batch spanning two tenants is reported as MIXED_TENANT rather than as a
    duplicate (M13).
    """
    rejections: list[Rejection] = []
    decoded: list[CloudEvent | None] = [None] * len(raw_events)

    # Pass 1 -- structural: shape, unknown attributes, size, schema.
    for i, raw in enumerate(raw_events):
        if not isinstance(raw, dict):
            rejections.append(Rejection(i, f"{CODE_SCHEMA} at $[{i}]: not an object"))
            continue
        extra = A.extra_attributes(raw)
        if extra:
            rejections.append(Rejection(i, f"{CODE_UNKNOWN_ATTR} at $[{i}]: {sorted(extra)[0]}"))
            continue
        if len(msgspec.json.encode(raw)) > max_bytes:
            rejections.append(
                Rejection(i, f"{CODE_OVERSIZED} at $[{i}]: exceeds {max_bytes} bytes")
            )
            continue
        try:
            decoded[i] = msgspec.json.decode(msgspec.json.encode(raw), type=CloudEvent)
        except msgspec.ValidationError as exc:
            rejections.append(Rejection(i, f"{CODE_SCHEMA} at {_error_path(exc)}"))

    # Pass 2 -- one tenant per batch. A batch carries one bearer token, so a
    # multi-tenant batch would mean trusting `source` from the payload.
    if len({ev.source for ev in decoded if ev is not None}) > 1:
        return BatchOutcome([], [Rejection(0, f"{CODE_MIXED_TENANT}: spans more than one tenant")])

    # Pass 3 -- semantic rules the struct cannot express.
    for i, ev in enumerate(decoded):
        if ev is None:
            continue
        problem = A.envelope_problem(ev)
        if problem:
            rejections.append(
                Rejection(i, f"{problem} at {_PROBLEM_FIELD.get(problem, '$')}")
            )
            decoded[i] = None

    # Pass 4 -- duplicate (source, id) within the batch. A client reusing one id
    # across many events would otherwise have them silently collapsed downstream.
    seen: set[tuple[str, str]] = set()
    for i, ev in enumerate(decoded):
        if ev is None:
            continue
        key = dedup_key(ev.source, ev.id)
        if key in seen:
            rejections.append(Rejection(i, f"{CODE_DUPLICATE_ID} at $.id"))
            decoded[i] = None
            continue
        seen.add(key)

    rejections.sort(key=lambda r: r.index)
    return BatchOutcome([ev for ev in decoded if ev is not None], rejections)
