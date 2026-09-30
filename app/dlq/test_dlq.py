"""T5 dead-letter queue.

The DLQ carries the POST-encryption event (plan C3). r3.1 would have written the
pre-encryption payload, which made the DLQ a plaintext PII store on a second
Kafka topic -- and re-injecting such an event double-encrypted it.

`error_context` never carries the offending value.
"""

from __future__ import annotations

import json
from pathlib import Path

import msgspec
import pytest

from app.dlq.envelope import (
    DLQ_ERROR_TYPE,
    ErrorContext,
    build_dlq_event,
    dlq_data_to_ingress,
)
from contracts.cloudevent import CloudEvent, decode_batch

EXAMPLES = Path(__file__).resolve().parents[2] / "contracts" / "examples"


def _raw(name: str = "job-viewed.json") -> list[dict]:
    return json.loads((EXAMPLES / name).read_text(encoding="utf-8"))


def test_dlq_event_is_a_valid_cloudevent_envelope():
    ev = build_dlq_event(_raw()[0], reason="bad type", field="type", index=3)
    assert ev.specversion == "1.0"
    assert ev.type == DLQ_ERROR_TYPE
    assert ev.source == "/careers/acme_8921"
    assert ev.id and ev.time


def test_dlq_error_context_carries_the_fields_the_spec_lists():
    ev = build_dlq_event(_raw()[0], reason="bad type", field="type", index=3)
    ctx = ev.data.error_context
    assert ctx.reason == "bad type"
    assert ctx.field == "type"
    assert ctx.index == 3
    assert ctx.stage == "validate"
    assert ctx.failed_at
    assert ctx.exception_class
    assert ctx.retry_count == 0


def test_error_context_reason_never_carries_the_offending_value():
    """The REASON is a code + field path. The payload is a different matter: it
    must carry the original event for replay, including non-PII fields."""
    batch = _raw()
    batch[0]["type"] = "com.careerpage.career.not-real"
    ev = build_dlq_event(batch[0], reason="SCHEMA at $.type", field="type", index=0)
    assert "not-real" not in ev.data.error_context.reason
    assert "not-real" not in ev.data.error_context.exception_class
    # ...but replay still needs the original, so it is retained in the payload.
    assert ev.data.original_payload["type"] == "com.careerpage.career.not-real"


def test_dlq_carries_the_post_encryption_payload_not_plaintext():
    """C3: the payload must be whatever was rejected, already encrypted."""
    batch = _raw()
    batch[0]["data"]["candidate"]["email_enc"] = "Y2lwaGVydGV4dA=="
    ev = build_dlq_event(batch[0], reason="bad type", field="type", index=0)
    assert ev.data.original_payload["data"]["candidate"]["email_enc"] == "Y2lwaGVydGV4dA=="


def test_canary_pii_never_appears_in_a_dlq_event():
    """M7: a canary beats a regex. The real leak paths are error reprs, not emails."""
    sentinel = "SENTINEL-8f3a@example.invalid"
    batch = _raw()
    batch[0]["data"]["candidate"]["email_enc"] = "Y2lwaGVydGV4dA=="
    ev = build_dlq_event(batch[0], reason="bad time", field="time", index=0)
    assert sentinel not in msgspec.json.encode(ev).decode()


def test_dlq_event_round_trips_back_into_an_ingress_event():
    """Re-injecting a DLQ event must not double-encrypt (C3).

    A validation rejection happens BEFORE the encrypt stage, so the DLQ payload
    is the plaintext ingress event. The round trip therefore returns an INGRESS
    event -- which re-submits cleanly and gets encrypted once, correctly. The
    post-encryption case is covered in `app/ingest/test_pipeline.py`.
    """
    from contracts.ingress import IngressEvent

    original = _raw()[0]
    ev = build_dlq_event(original, reason="bad type", field="type", index=0)
    restored = dlq_data_to_ingress(ev.data, IngressEvent)
    assert restored.id == original["id"]
    assert restored.type == original["type"]
    assert restored.data.candidate.email == original["data"]["candidate"]["email"]
    assert restored.data.candidate.user_id == original["data"]["candidate"]["user_id"]


def test_restored_event_revalidates_cleanly():
    from contracts.ingress import IngressEvent

    ev = build_dlq_event(_raw()[0], reason="bad type", field="type", index=0)
    restored = dlq_data_to_ingress(ev.data, IngressEvent)
    assert msgspec.json.decode(msgspec.json.encode(restored), type=IngressEvent)


def test_dlq_source_is_derived_from_the_tenant_not_the_payload():
    """A malformed source must not propagate into the DLQ as a tenant."""
    ev = build_dlq_event(_raw()[0], reason="bad source", field="source", index=0)
    assert ev.source == "/careers/acme_8921"


def test_dlq_id_is_unique_per_rejection():
    a = build_dlq_event(_raw()[0], reason="r1", field="type", index=0)
    b = build_dlq_event(_raw()[0], reason="r2", field="time", index=1)
    assert a.id != b.id


def test_error_context_preserves_the_kafka_coordinates_for_replay():
    ev = build_dlq_event(
        _raw()[0], reason="bad", field="type", index=0,
        original_topic="career.events.raw", original_partition=3, original_offset=8849201,
    )
    ctx = ev.data.error_context
    assert ctx.original_topic == "career.events.raw"
    assert ctx.original_partition == 3
    assert ctx.original_offset == 8849201


def test_kafka_coordinates_default_when_the_write_never_happened():
    ev = build_dlq_event(_raw()[0], reason="bad", field="type", index=0)
    assert ev.data.error_context.original_topic is None
    assert ev.data.error_context.original_offset is None


def test_dlq_envelope_rejects_unknown_fields():
    ev = build_dlq_event(_raw()[0], reason="bad", field="type", index=0)
    payload = msgspec.json.decode(msgspec.json.encode(ev), type=dict)
    payload["sneaky"] = "value"
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(msgspec.json.encode(payload), type=type(ev))
