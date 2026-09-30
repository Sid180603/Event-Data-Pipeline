"""T2 contract tests. RED before implementation."""

from __future__ import annotations

import json
from pathlib import Path

import msgspec
import pytest

from contracts import attributes as A
from contracts.cloudevent import (
    ORG_PREFIX,
    CloudEvent,
    Data,
    EVENT_TYPES,
    decode_batch,
)
from contracts.ingress import IngressEvent, decode_ingress
from contracts.ledger import LedgerRecord, Ledger, dedup_key

HERE = Path(__file__).parent
EXAMPLES = HERE / "examples"
SCHEMA_PATH = HERE / "event.schema.json"

# Every context/extension attribute we permit. Anything else in the envelope
# fails the allowlist test regardless of its value (plan H3).
SAFE_CONTEXT_ATTRS = {
    "specversion",
    "id",
    "source",
    "type",
    "subject",
    "time",
    "dataschema",
    "datacontenttype",
    "keyversion",
    "sequence",
    "sourcechannel",
    "referrertype",
    "completionmethod",
}


def _example(name: str) -> dict:
    return json.loads((EXAMPLES / name).read_text(encoding="utf-8"))


def decode_ingress_batch(batch: list[dict]) -> list[IngressEvent]:
    """Published examples are the INGRESS shape -- what a client sends.

    A client cannot hold a tenant key, so it sends plaintext PII and the gateway
    encrypts on the way in. The egress shape (`contracts.cloudevent.CloudEvent`)
    is what lands on the topic and is validated separately, in
    `app/ingest/test_pipeline.py`, which exercises the real ingress -> egress
    transform.
    """
    return decode_ingress(msgspec.json.encode(batch))


# --- Attribute naming rules (E2) --------------------------------------------


def test_attribute_names_are_lowercase_alnum_and_under_20_chars():
    for name in SAFE_CONTEXT_ATTRS:
        assert A.is_valid_attribute_name(name), f"{name} violates naming rules"


def test_attribute_name_rejects_underscores():
    # E2: CloudEvents requires [a-z0-9] only. Underscores are illegal.
    assert not A.is_valid_attribute_name("source_channel")
    assert not A.is_valid_attribute_name("career_site_id")
    assert not A.is_valid_attribute_name("CompletionMethod")


def test_attribute_name_rejects_over_20_chars():
    assert not A.is_valid_attribute_name("a" * 21)
    assert A.is_valid_attribute_name("a" * 20)


def test_attribute_name_rejects_data_which_is_reserved():
    assert not A.is_valid_attribute_name("data")


# --- Envelope decoding -------------------------------------------------------


@pytest.mark.parametrize("path", sorted(EXAMPLES.glob("*.json")), ids=lambda p: p.name)
def test_every_example_decodes(path):
    batch = _example(path.name)
    events = decode_ingress_batch(batch)
    assert len(events) == len(batch)
    for ev in events:
        assert ev.specversion == "1.0"
        A.validate_envelope(ev)


def test_examples_cover_all_nine_event_types():
    seen = set()
    for path in EXAMPLES.glob("*.json"):
        for ev in decode_ingress_batch(_example(path.name)):
            seen.add(ev.type)
    assert seen == set(EVENT_TYPES), f"missing: {set(EVENT_TYPES) - seen}"


def test_all_event_types_use_the_org_reverse_dns_prefix():
    for t in EVENT_TYPES:
        assert t.startswith(ORG_PREFIX), t


def test_missing_required_attribute_is_rejected():
    payload = _example("job-viewed.json")[0]
    del payload["id"]
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(msgspec.json.encode(payload), type=list[CloudEvent])


def test_bad_source_prefix_is_rejected():
    payload = _example("job-viewed.json")[0]
    payload["source"] = "not-a-career-source"
    with pytest.raises(ValueError, match="source"):
        ev = decode_ingress_batch([payload])[0]
        A.validate_envelope(ev)


def test_non_rfc3339_time_is_rejected():
    payload = _example("job-viewed.json")[0]
    payload["time"] = "30-09-2026 14:43:09"
    with pytest.raises(ValueError, match="time"):
        A.validate_envelope(decode_ingress_batch([payload])[0])


def test_unknown_event_type_is_rejected():
    payload = _example("job-viewed.json")[0]
    payload["type"] = "com.careerpage.career.not-a-real-event"
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(msgspec.json.encode(payload), type=list[CloudEvent])


def test_plaintext_pii_field_in_candidate_is_rejected_not_dropped():
    """Without forbid_unknown_fields a client posting data.candidate.email has
    it silently dropped: the event is then accepted carrying no email and
    nobody is aware. Rejecting is the fix -- a loud error the client can act on."""
    payload = _example("job-viewed.json")[0]
    payload["data"]["candidate"]["email"] = "senniel@example.com"
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(msgspec.json.encode(payload), type=list[CloudEvent])


def test_unknown_field_in_event_payload_is_rejected():
    payload = _example("job-viewed.json")[0]
    payload["data"]["event_payload"]["secret_note"] = "x"
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(msgspec.json.encode(payload), type=list[CloudEvent])


def test_underscored_attribute_in_envelope_fails_the_allowlist():
    payload = _example("job-viewed.json")[0]
    payload["career_site_id"] = "acme_8921"  # illegal extension name
    assert "career_site_id" in A.extra_attributes(payload)
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(msgspec.json.encode(payload), type=list[CloudEvent])


# --- PII placement (H3) ------------------------------------------------------


def test_no_pii_values_appear_in_context_attributes():
    """Context attributes carry identifiers and routing, never PII values.

    Egress-shaped event: the sentinel stands in for whatever a caller might try
    to smuggle into a context attribute. At ingress the PII field is `email` and
    lives under `data`, which the allowlist already covers.
    """
    sentinel = "SENTINEL-8f3a@example.invalid"
    from contracts.cloudevent import Data as _Data
    from contracts.cloudevent import CandidateMetadata

    ev = decode_batch(
        [
            {
                **_example("job-viewed.json")[0],
                "data": {
                    "candidate": CandidateMetadata(user_id_pseudo="a1b2c3"),
                    "event_payload": {"job_id": "job_1", "session_id": "sess_1"},
                },
            }
        ]
    )[0]
    for attr in SAFE_CONTEXT_ATTRS:
        value = getattr(ev, attr, None)
        if isinstance(value, str):
            assert sentinel not in value, f"PII leaked into context attr {attr}"


def test_identity_events_use_pseudonymous_subject_when_published():
    """H3 applies to what we PUBLISH, not to what a client sends."""
    from contracts.cloudevent import CandidateMetadata, decode_batch

    raw = decode_batch(
        [
            {
                **_example("user-logged-in.json")[0],
                "data": {
                    "candidate": CandidateMetadata(user_id_pseudo="a1b2c3"),
                    "event_payload": {"job_id": "-", "session_id": "s1"},
                },
            }
        ]
    )[0]
    # A published identity event carrying a raw usr_ id is the H3 violation.
    raw.subject = "usr_992182741"
    assert A.envelope_problem(raw, require_pseudonymous_subject=True) == "RAW_SUBJECT"
    # ...and the same value is legitimate on the wire from a client.
    assert A.envelope_problem(raw) is None


# --- Single-tenant batches (D2) ---------------------------------------------


def test_mixed_tenant_batch_is_detected():
    a = _example("job-viewed.json")[0]
    b = _example("job-viewed.json")[0]
    b["source"] = "/careers/other_tenant_1"
    evs = decode_ingress_batch([a, b])
    assert A.distinct_sources(evs) == 2


def test_single_tenant_batch_has_one_source():
    evs = decode_ingress_batch(_example("job-viewed.json"))
    assert A.distinct_sources(evs) == 1


# --- Schema generation (single source of truth) ------------------------------


def test_generated_schema_is_committed_and_not_stale():
    from contracts.cloudevent import generate_schema_json

    committed = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert committed == json.loads(generate_schema_json()), (
        "event.schema.json is stale — regenerate with "
        "`python -m contracts.gen_schema`"
    )


def test_the_ingress_schema_is_published_and_not_stale():
    """Both shapes are contracts. The ingress one is what a client codes against,
    so it must be as published and as fresh as the egress one."""
    from contracts.ingress import generate_ingress_schema_json

    path = HERE / "ingress.schema.json"
    assert path.exists(), "ingress.schema.json missing — regenerate it"
    assert json.loads(path.read_text(encoding="utf-8")) == json.loads(
        generate_ingress_schema_json()
    ), "ingress.schema.json is stale"


def test_the_ingress_schema_is_self_contained():
    """A `$ref` into a `$defs` block that was not published is a 3-line file that
    validates nothing. The components must travel with the root."""
    doc = json.loads((HERE / "ingress.schema.json").read_text(encoding="utf-8"))
    defs = doc.get("$defs", {})
    assert defs, "ingress.schema.json has no $defs — every $ref would dangle"
    assert {"IngressEvent", "IngressCandidate", "IngressData"} <= set(defs)
    assert doc["$defs"]["IngressCandidate"]["required"] == ["user_id"]
    # And the ingress candidate is plaintext, not ciphertext.
    props = set(doc["$defs"]["IngressCandidate"]["properties"])
    assert "email" in props and "user_id" in props
    assert not any(p.endswith("_enc") or p == "user_id_pseudo" for p in props)


def test_ingress_and_egress_candidate_shapes_are_different():
    """They must not be conflated: one struct for both directions means a client
    can post nothing at all."""
    from contracts.cloudevent import CandidateMetadata
    from contracts.ingress import IngressCandidate

    ingress = set(IngressCandidate.__struct_fields__)
    egress = set(CandidateMetadata.__struct_fields__)
    assert "email" in ingress and "email" not in egress
    assert "user_id" in ingress and "user_id_pseudo" in egress
    # Neither shape accepts the other's PII fields.
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(
            msgspec.json.encode(
                {
                    **_example("job-viewed.json")[0],
                    "data": {
                        "candidate": {"user_id": "u1", "email_enc": "x"},
                        "event_payload": {"job_id": "j", "session_id": "s"},
                    },
                }
            ),
            type=list[IngressEvent],
        )


# --- Derived Kafka key (C2) --------------------------------------------------


def test_kafka_key_is_derived_not_taken_from_the_envelope():
    """C2: the client cannot choose the partition key."""
    payload = _example("job-viewed.json")[0]
    payload["partitionkey"] = "victim_tenant|victim_user"  # attacker-controlled
    ev = decode_ingress_batch([payload])[0]
    key = A.derive_kafka_key("acme_8921", "a1b2c3d4e5f6")
    assert key == "acme_8921|a1b2c3d4e5f6"
    assert "victim" not in key


def test_client_supplied_partitionkey_mismatch_is_detectable():
    payload = _example("job-viewed.json")[0]
    supplied = "acme_8921|different"
    assert A.partitionkey_conflicts(supplied, "acme_8921", "a1b2c3d4e5f6")


# --- Ledger (D8/M2) ---------------------------------------------------------


def test_ledger_round_trips(tmp_path):
    path = tmp_path / "l.jsonl"
    with Ledger(path) as led:
        led.append(LedgerRecord("01J8X", "/careers/acme_8921", "com.careerpage.career.job-viewed", "acme_8921", "a1b2", 0))
        led.append(LedgerRecord("01J8Y", "/careers/acme_8921", "com.careerpage.career.job-viewed", "acme_8921", "a1b2", 1))
    assert len(Ledger(path).read_all()) == 2


def test_ledger_detects_duplicate_source_id_pairs():
    recs = [
        LedgerRecord("01J8X", "/careers/a", "t", "a", "u", 0),
        LedgerRecord("01J8X", "/careers/a", "t", "a", "u", 1),
    ]
    assert Ledger.duplicates(recs) == 1


def test_same_id_different_source_is_not_a_duplicate():
    """Dedup key is (source, id) -- id alone would false-positive here."""
    recs = [
        LedgerRecord("01J8X", "/careers/a", "t", "a", "u", 0),
        LedgerRecord("01J8X", "/careers/b", "t", "b", "u", 0),
    ]
    assert Ledger.duplicates(recs) == 0


def test_dedup_key_is_source_and_id():
    assert dedup_key("/careers/a", "01J8X") == ("/careers/a", "01J8X")
