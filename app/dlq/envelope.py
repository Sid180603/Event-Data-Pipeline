"""T5 dead-letter queue.

The DLQ carries the POST-encryption event (plan C3). Writing the
pre-encryption payload would make the DLQ a plaintext PII store on a second
Kafka topic, and re-injecting such an event would double-encrypt it.

`error_context` never carries the offending value.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import msgspec

from contracts.cloudevent import CloudEvent

#: An internal type, deliberately not added to the published ingress contract's
#: 9 event types (contracts/cloudevent.py). The wire contract stays untouched by
#: an internal artifact.
DLQ_ERROR_TYPE = "com.careerpage.career.ingest-rejected"

UNKNOWN_TENANT = "/careers/_unknown"


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class ErrorContext(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    """Why an event was rejected. Never the offending value."""

    reason: str
    failed_at: str
    exception_class: str
    field: str | None = None
    index: int | None = None
    stage: str = "validate"
    retry_count: int = 0
    original_topic: str | None = None
    original_partition: int | None = None
    original_offset: int | None = None


class DlqData(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    original_payload: dict
    error_context: ErrorContext


class DlqEvent(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    """A CloudEvent, so the Queue team's replay worker needs no special parser."""

    specversion: str
    id: str
    source: str
    type: str
    time: str
    data: DlqData
    subject: str | None = None
    dataschema: str | None = None
    datacontenttype: str = "application/json"


def build_dlq_event(
    rejected_payload: dict[str, Any],
    *,
    reason: str,
    exception_class: str = "ValidationError",
    field: str | None = None,
    index: int | None = None,
    stage: str = "validate",
    original_topic: str | None = None,
    original_partition: int | None = None,
    original_offset: int | None = None,
) -> DlqEvent:
    """Wrap a rejected event. `rejected_payload` must already be encrypted."""
    source = rejected_payload.get("source")
    if not isinstance(source, str) or not source.startswith("/careers/"):
        # A malformed source must not propagate as if it were a tenant.
        source = UNKNOWN_TENANT

    return DlqEvent(
        specversion="1.0",
        id=f"dlq-{uuid.uuid4().hex[:20].upper()}",
        source=source,
        type=DLQ_ERROR_TYPE,
        time=_now_rfc3339(),
        subject=rejected_payload.get("subject"),
        dataschema="https://schema.careerpage.example/dlq/1.0",
        data=DlqData(
            original_payload=rejected_payload,
            error_context=ErrorContext(
                reason=reason,
                failed_at=_now_rfc3339(),
                exception_class=exception_class,
                field=field,
                index=index,
                stage=stage,
                retry_count=0,
                original_topic=original_topic,
                original_partition=original_partition,
                original_offset=original_offset,
            ),
        ),
    )


def dlq_data_to_ingress(data: DlqData, target: type = CloudEvent):
    """Recover the original event for replay, without re-encrypting (C3).

    `target` is the shape the payload was stored in. A validation rejection is
    recorded before the encrypt stage, so it round-trips to an `IngressEvent`
    and re-submits to be encrypted exactly once. A post-encryption rejection
    round-trips to a `CloudEvent` and must NOT be re-encrypted — that would
    produce undecryptable ciphertext.
    """
    return msgspec.json.decode(msgspec.json.encode(data.original_payload), type=target)
