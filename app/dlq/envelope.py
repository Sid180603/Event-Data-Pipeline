"""T5 dead-letter queue.

**The DLQ carries two different payload shapes, and saying otherwise is a lie an
operator acts on.** This module's docstring used to claim the DLQ always carries
the post-encryption event. It does not, and the reason is the stage that rejected
the event:

* Rejected **after** the encrypt stage (an event that was encrypted and then
  crossed the size limit): the payload is the ENCRYPTED event. A plaintext payload
  there would make the DLQ a second PII store on a second topic, and a replayed
  record would arrive already carrying ciphertext to be encrypted again.
* Rejected **during** validation: the event was never encrypted, so the only
  payload that exists is the plaintext request element. There is nothing else it
  could carry. Encrypting a record we have already judged invalid would be work
  spent on a dead event, and redacting it would leave an operator holding a DLQ
  record they cannot diagnose.

So **a validation-stage DLQ record does contain plaintext PII.** That is a
consequence of the pipeline order (plan D6, stage 4 before stage 5) and it is
accepted deliberately -- see the reasoning at `app/ingest/pipeline.py` stage 7 --
but it must be stated, because "the DLQ never holds plaintext" is exactly the kind
of claim an auditor will check and exactly the kind that turns out to be false.
`dlq_data_to_ingress` is the other half of the same fact: it round-trips each shape
to the struct it was stored as, so a validate-stage record comes back as an
`IngressEvent` and is encrypted exactly once on re-injection, while an
encrypt-stage record comes back as a `CloudEvent` and must not be re-encrypted.

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
    """Wrap a rejected event.

    `rejected_payload` is the payload **in the shape the rejection left it in** --
    an encrypted `CloudEvent` for a rejection after the encrypt stage, the
    plaintext request element for one during validation. See the module docstring
    for why both exist and what the plaintext case implies; this function does not
    encrypt, redact or transform it, because doing either would break the replay
    path `dlq_data_to_ingress` implements.
    """
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
