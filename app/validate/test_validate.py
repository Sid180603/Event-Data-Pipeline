"""T5 validation: per-event rejection, never per-batch."""

from __future__ import annotations

import json
from pathlib import Path

import msgspec
import pytest

from app.validate.events import BatchOutcome, validate_batch
from contracts.cloudevent import CloudEvent, decode_batch

EXAMPLES = Path(__file__).resolve().parents[2] / "contracts" / "examples"


def _raw(name: str = "job-viewed.json") -> list[dict]:
    """EGRESS-shaped events.

    `app/validate` runs on what is PUBLISHED, so it validates
    `contracts.cloudevent.CloudEvent` -- the post-encryption shape. The published
    examples are the ingress shape (what a client sends), so this builds its own
    egress fixture rather than reusing them. The ingress->egress transform is
    covered by `app/ingest/test_pipeline.py`.
    """
    import msgspec

    from contracts.cloudevent import CandidateMetadata

    raw = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    for ev in raw:
        ev["data"]["candidate"] = msgspec.json.decode(
            msgspec.json.encode(CandidateMetadata(user_id_pseudo="a1b2c3")),
            type=dict,
        )
    return raw


# --- happy path --------------------------------------------------------------


def test_a_valid_batch_accepts_every_event():
    out = validate_batch(_raw())
    assert out.accepted == 1
    assert out.rejected == []
    assert all(isinstance(e, CloudEvent) for e in out.events)


def test_a_good_event_is_preserved_intact():
    out = validate_batch(_raw())
    assert out.events[0].id == _raw()[0]["id"]
    assert out.events[0].type == _raw()[0]["type"]


# --- per-event rejection (D6) ------------------------------------------------


def test_one_bad_event_does_not_fail_the_batch():
    batch = _raw("application-step-completed.json")  # two good events
    batch[1]["type"] = "com.careerpage.career.not-real"
    out = validate_batch(batch)
    assert out.accepted == 1
    assert len(out.rejected) == 1
    assert out.rejected[0].index == 1


def test_rejection_records_an_index_and_a_reason():
    batch = _raw("application-step-completed.json")
    batch[1]["type"] = "com.careerpage.career.not-real"
    out = validate_batch(batch)
    assert out.rejected[0].reason
    assert "type" in out.rejected[0].reason.lower()


def test_every_event_can_fail_independently():
    batch = _raw("application-step-completed.json")
    batch[0]["type"] = "bad"
    batch[1]["time"] = "not-a-timestamp"
    out = validate_batch(batch)
    assert out.accepted == 0
    assert [r.index for r in out.rejected] == [0, 1]


def test_an_undecodable_batch_reports_rather_than_raises():
    out = validate_batch([{"not": "an event"}])
    assert out.accepted == 0
    assert out.rejected


# --- single-tenant batches (D2) ---------------------------------------------


def test_a_batch_mixing_two_tenants_is_rejected():
    batch = _raw()
    batch.append(dict(batch[0], id="01J8XOTHER", source="/careers/other_tenant_1"))
    out = validate_batch(batch)
    assert out.accepted == 0
    assert "tenant" in out.rejected[0].reason.lower()


# --- duplicate id within a batch (M13) --------------------------------------


def test_duplicate_id_within_a_batch_is_rejected():
    batch = _raw("application-step-completed.json")
    batch[1]["id"] = batch[0]["id"]
    out = validate_batch(batch)
    assert out.accepted == 1
    assert "duplicate" in out.rejected[0].reason.lower()


def test_the_same_id_in_two_different_batches_is_fine():
    a = _raw()
    b = _raw()
    assert validate_batch(a).accepted == 1
    assert validate_batch(b).accepted == 1


def test_same_id_different_tenant_is_not_a_duplicate():
    batch = _raw()
    batch.append(dict(batch[0], source="/careers/other_tenant_1"))
    out = validate_batch(batch)
    # rejected for mixing tenants, NOT for duplicate id
    assert "tenant" in out.rejected[0].reason.lower()


# --- semantic rules msgspec does not check ----------------------------------


def test_bad_rfc3339_time_is_rejected():
    batch = _raw()
    batch[0]["time"] = "30-09-2026 14:43:09"
    out = validate_batch(batch)
    assert out.accepted == 0
    assert "time" in out.rejected[0].reason.lower()


def test_unknown_context_attribute_is_rejected():
    batch = _raw()
    batch[0]["career_site_id"] = "acme_8921"
    out = validate_batch(batch)
    assert out.accepted == 0


def test_oversized_event_is_rejected():
    from app.config import MAX_EVENT_BYTES

    batch = _raw()
    batch[0]["data"]["event_payload"]["client_metadata"] = {"blob": "x" * (MAX_EVENT_BYTES + 1)}
    out = validate_batch(batch)
    assert out.accepted == 0
    assert "size" in out.rejected[0].reason.lower() or "64" in out.rejected[0].reason.lower()


# --- ordering input (C1) -----------------------------------------------------


def test_events_can_be_ordered_by_sequence():
    batch = _raw("application-step-completed.json")
    batch[0]["sequence"] = "0000000002"
    batch[1]["sequence"] = "0000000001"
    out = validate_batch(batch)
    ordered = sorted(out.events, key=lambda e: e.sequence)
    assert [e.sequence for e in ordered] == ["0000000001", "0000000002"]


# --- outcome shape (CONTRACT.md 202) -----------------------------------------


def test_outcome_serialises_to_the_documented_response_shape():
    batch = _raw("application-step-completed.json")
    batch[1]["type"] = "bad"
    payload = validate_batch(batch).to_response()
    assert payload["accepted"] == 1
    assert payload["rejected"] == [{"index": 1, "reason": payload["rejected"][0]["reason"]}]


def test_accepted_and_rejected_counts_always_sum_to_the_batch_size():
    for name in ("job-viewed.json", "application-step-completed.json", "user-logged-in.json"):
        out = validate_batch(_raw(name))
        assert out.accepted + len(out.rejected) == len(_raw(name))
