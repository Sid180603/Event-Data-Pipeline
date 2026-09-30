"""T8a driver: the funnel state machine.

The driver must model real user journeys, not emit random events. Random events
produce random dashboards, which is worthless for proving the DB team's funnel
work.
"""

from __future__ import annotations

import pytest

from driver.fsm import (
    ABANDONED,
    DRAFT_SAVED,
    SUBMITTED,
    SessionConfig,
    generate_session,
    session_survival,
)
from contracts.attributes import envelope_problem

TENANT = "acme_8921"
PSEUDO = "a1b2c3d4e5f6"


def _cfg(**kw) -> SessionConfig:
    base = dict(
        career_site_id=TENANT,
        user_pseudo=PSEUDO,
        source_channel="WEB_APP",
        job_id="job_88320491",
        steps=3,
        outcome=SUBMITTED,
        start_sequence=0,
    )
    base.update(kw)
    return SessionConfig(**base)


def _types(events):
    return [e["type"] for e in events]


# --- shape of a journey ------------------------------------------------------


def test_a_submitted_journey_starts_with_job_viewed():
    assert _types(generate_session(_cfg()))[0].endswith("job-viewed")


def test_a_submitted_journey_ends_with_application_submitted():
    assert _types(generate_session(_cfg()))[-1].endswith("application-submitted")


def test_each_step_completed_produces_one_event():
    events = generate_session(_cfg(steps=3))
    assert sum(1 for e in events if e["type"].endswith("step-completed")) == 3


def test_a_saved_later_journey_ends_with_draft_saved():
    events = generate_session(_cfg(outcome=DRAFT_SAVED))
    assert _types(events)[-1].endswith("application-draft-saved")


def test_an_abandoned_journey_emits_no_terminal_event():
    """G1: APPLICATION_ABANDONED belongs to the Queue team's Flink job, which
    synthesises it on the watermark timeout. The driver omits the terminal event
    and lets that happen. If we emitted it too, the drop-off metric double-counts."""
    events = generate_session(_cfg(outcome=ABANDONED, steps=3))
    assert not any(e["type"].endswith("application-submitted") for e in events)
    assert not any(e["type"].endswith("draft-saved") for e in events)


def test_the_driver_never_emits_application_abandoned():
    for outcome in (SUBMITTED, DRAFT_SAVED, ABANDONED):
        events = generate_session(_cfg(outcome=outcome))
        assert not any("application-abandoned" in e["type"] for e in events)


def test_a_step_completed_is_never_emitted_without_an_application_started():
    for outcome in (SUBMITTED, DRAFT_SAVED, ABANDONED):
        types = _types(generate_session(_cfg(outcome=outcome)))
        for i, t in enumerate(types):
            if t.endswith("step-completed"):
                assert any(x.endswith("application-started") for x in types[:i])


# --- envelope validity -------------------------------------------------------


def test_every_generated_event_is_a_valid_envelope():
    from contracts.cloudevent import decode_batch

    events = generate_session(_cfg())
    decoded = decode_batch(events)
    assert len(decoded) == len(events)
    for ev in decoded:
        assert envelope_problem(ev) is None


def test_every_generated_event_belongs_to_the_session_tenant():
    for e in generate_session(_cfg()):
        assert e["source"] == f"/careers/{TENANT}"


# --- sequence (C1) -----------------------------------------------------------


def test_sequence_is_zero_padded_and_monotonic():
    seqs = [int(e["sequence"]) for e in generate_session(_cfg(steps=4))]
    assert seqs == sorted(seqs)
    assert all(len(e["sequence"]) == 10 for e in generate_session(_cfg(steps=4)))


def test_sequence_continues_from_the_supplied_start():
    events = generate_session(_cfg(steps=2, start_sequence=500))
    assert int(events[0]["sequence"]) == 500
    assert int(events[1]["sequence"]) == 501


def test_sequence_is_the_only_way_to_order_a_session():
    """A session is a coherent unit; the Queue team sorts on this."""
    events = generate_session(_cfg(steps=3))
    assert [int(e["sequence"]) for e in events] == list(
        range(int(events[0]["sequence"]), int(events[0]["sequence"]) + len(events))
    )


# --- identifiers -------------------------------------------------------------


def test_ids_are_unique_within_a_source():
    events = generate_session(_cfg(steps=6))
    ids = [e["id"] for e in events]
    assert len(set(ids)) == len(ids)


def test_two_sessions_with_the_same_start_do_not_collide():
    a = generate_session(_cfg(steps=4, start_sequence=0))
    b = generate_session(_cfg(steps=4, start_sequence=0))
    assert [e["id"] for e in a] != [e["id"] for e in b]


# --- payload shapes ----------------------------------------------------------


def test_a_web_session_carries_user_agent_and_page_context():
    ev = generate_session(_cfg(source_channel="WEB_APP"))[0]
    meta = ev["data"]["event_payload"]["client_metadata"]
    assert "userAgent" in meta
    assert "page" in meta


def test_a_mobile_session_carries_app_version_and_device():
    ev = generate_session(_cfg(source_channel="MOBILE_APP"))[0]
    meta = ev["data"]["event_payload"]["client_metadata"]
    assert "app" in meta
    assert "device" in meta


def test_source_channel_is_recorded_on_every_event():
    for e in generate_session(_cfg(source_channel="MOBILE_APP")):
        assert e["sourcechannel"] == "MOBILE_APP"


# --- completion method (spec #10) -------------------------------------------


def test_steps_are_tagged_with_a_completion_method():
    events = generate_session(_cfg(steps=3))
    for e in events:
        if e["type"].endswith("step-completed"):
            assert e["completionmethod"] in {"MANUAL", "RESUME_AUTOFILL", "HYBRID"}


def test_every_step_event_records_time_spent():
    for e in generate_session(_cfg(steps=3)):
        if e["type"].endswith("step-completed"):
            assert e["data"]["event_payload"]["time_spent_on_step_ms"] > 0


# --- recommendation attribution (spec #4/5) ----------------------------------


def test_application_started_can_carry_recommendation_attribution():
    ev = next(e for e in generate_session(_cfg()) if e["type"].endswith("application-started"))
    assert ev["referrertype"] in {"SEARCH", "RECOMMENDATION", "DIRECT", "THIRD_PARTY_WEBHOOK"}


def test_recommendation_attribution_lists_recommended_jobs():
    ev = next(
        e for e in generate_session(_cfg(referrer="RECOMMENDATION")) if e["type"].endswith("application-started")
    )
    assert ev["data"]["event_payload"]["recommended_job_ids"]


# --- funnel maths ------------------------------------------------------------


def test_survival_curve_is_monotonically_non_increasing():
    curve = session_survival(1000, drop_off_rate=0.3, steps=4)
    assert curve[0] == 1000
    assert all(a >= b for a, b in zip(curve, curve[1:]))


def test_survival_curve_ends_at_or_below_the_start():
    assert session_survival(500, drop_off_rate=0.3, steps=4)[-1] <= 500


def test_zero_dropoff_keeps_every_session():
    assert session_survival(200, drop_off_rate=0.0, steps=4)[-1] == 200


def test_survival_curve_length_is_steps_plus_one():
    assert len(session_survival(100, drop_off_rate=0.2, steps=3)) == 4
