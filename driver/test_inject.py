"""T8c: fault injection -- a countable bad event, and a single-tenant flood.

Two demo beats cannot be performed without this module:

* **"inject 5% garbage -> the DLQ catches exactly 5%"**. The demo compares the
  DLQ depth against what the driver says it sent, so the injected fraction has to
  be an exact integer. A Bernoulli draw at 5% misses by hundreds over a 5M-event
  run, and "roughly 5%" cannot be asserted.
* **"one tenant floods -> it gets shed, the other 499 are unaffected"**. The
  Zipfian corpus in `driver/skew.py` deliberately spreads volume *out*, and
  `app.ratelimit` budgets each tenant separately, so a flood has to be its own
  mode rather than a parameter of the corpus.

Every variant here is rejected by a code the gateway already emits
(`app.validate.events`). Each one is driven through the gateway's own
`validate_batch` and ingress decoder and the code asserted -- an invented code
would make the demo assert something the DLQ never actually says.
"""

from __future__ import annotations

import json
from collections import Counter

import msgspec
import pytest

from app.validate.events import (
    CODE_BAD_TIME,
    CODE_DUPLICATE_ID,
    CODE_SCHEMA,
    CODE_UNKNOWN_ATTR,
    validate_batch,
)
from contracts.cloudevent import EVENT_TYPES
from contracts.ingress import IngressEvent, decode_ingress
from contracts.ledger import Ledger
from driver.corpus import CorpusBuilder
from driver.inject import (
    ILLEGAL_ATTRIBUTE,
    ILLEGAL_TIME,
    ILLEGAL_TYPE,
    VARIANT_CODES,
    MalformedInjector,
    TenantFlood,
    corrupt,
    count_events,
    flood_user_id,
    main,
    plan_injection,
)
from driver.skew import SkewedCorpus
from driver.tenants import TenantCatalog

TENANTS = [f"tenant_{i:04d}" for i in range(5)]
SESSIONS = 120


# --- the gateway's own verdict ------------------------------------------------
#
# `app.ingest.pipeline` runs two stages that can reject an event before any of
# auth, encryption or the size check: decode each element as `IngressEvent`
# (stage 2), then validate whatever decoded against the published contract
# structs (stage 4). Neither later stage can turn a variant that fails here into
# a success, so this is the whole verdict for a bad event.


def _contract_form(event: dict) -> dict:
    """The gateway's contract view of one ingress event, as a raw dict.

    The field swap is `_contract_form`'s, imported rather than repeated: the
    whole point is that the code under test is judged by the structs the gateway
    actually validates against, so a second copy of that mapping here would be a
    second thing to keep in step with the first.
    """
    from app.ingest.pipeline import _contract_form

    ingress = msgspec.json.decode(msgspec.json.encode(event), type=IngressEvent)
    return msgspec.json.decode(msgspec.json.encode(_contract_form(ingress)), type=dict)


def gateway_codes(batch: list[dict]) -> set[str]:
    """The reason CODES the gateway would file to the DLQ for `batch`."""
    forms: list[dict] = []
    codes: set[str] = set()
    for event in batch:
        try:
            forms.append(_contract_form(event))
        except msgspec.DecodeError:
            from contracts.attributes import extra_attributes

            codes.add(CODE_UNKNOWN_ATTR if extra_attributes(event) else CODE_SCHEMA)
    for rejection in validate_batch(forms).rejections:
        codes.add(rejection.reason.split()[0])
    return codes


# --- helpers ------------------------------------------------------------------


def _corpus(seed: int = 7) -> CorpusBuilder:
    return CorpusBuilder(career_site_ids=TENANTS, seed=seed, users_per_tenant=10)


def _corrupt(variant: str, event: dict, *, partner: dict | None = None) -> dict:
    """One event, corrupted the way the injector corrupts it."""
    return corrupt(event, variant, partner_id=None if partner is None else partner["id"])


def _injected(rate: float, *, seed: int = 7, max_events: int = 200):
    """A corpus with `rate` of its events malformed, and the report.

    Two corpora, deliberately. A `CorpusBuilder` advances its own `random.Random`
    as it generates, so a second pass over the SAME instance emits a different
    number of events -- 672 becomes 668 -- and the count an exact plan needs would
    be counting a run that is not the one being injected. The injector refuses a
    run whose size does not match the plan, so this is the shape callers need.
    """
    total = count_events(_corpus(seed=seed).batches(SESSIONS))
    injector = MalformedInjector(rate=rate, seed=seed)
    batches = list(
        injector.batches(
            _corpus(seed=seed).batches(SESSIONS, max_events=max_events), total_events=total
        )
    )
    return batches, injector.report, total


def _per_batch_injections(batches: list[list[dict]], plan) -> Counter:
    """How many of the planned injections landed in each batch."""
    counts: Counter = Counter()
    offset = 0
    for batch in batches:
        for j in range(len(batch)):
            if plan.variant_at(offset + j) is not None:
                counts[offset] += 1
        offset += len(batch)
    return counts


# --- the count is exact, and the default is clean -----------------------------


def test_a_clean_run_injects_nothing():
    _, report, total = _injected(0.0)
    assert report.injected == 0
    assert report.by_variant == {}
    assert report.emitted == total


def test_a_zero_rate_run_is_byte_identical_to_the_corpus():
    """Opt-in means opt-in: nothing is rewritten, so a default run cannot have a
    garbage event in it by accident."""
    corpus = _corpus()
    batches = list(corpus.batches(40))
    injector = MalformedInjector(rate=0.0)
    out = list(injector.batches(iter(batches), total_events=count_events(batches)))
    assert out == batches


def test_five_percent_is_exactly_five_percent():
    batches, report, total = _injected(0.05)
    assert report.injected == round(total * 0.05)
    assert report.injected / report.emitted == pytest.approx(0.05, abs=0.002)
    assert sum(len(b) for b in batches) == report.emitted


def test_the_injected_count_is_an_exact_integer_by_construction():
    """The plan is what the demo asserts against, so the rounding is pinned on
    round numbers rather than left to whatever a corpus happened to produce."""
    assert plan_injection(total_events=1_000, rate=0.05).count == 50
    assert plan_injection(total_events=10_000, rate=0.07).count == 700
    assert plan_injection(total_events=3, rate=0.05).count == 0
    assert plan_injection(total_events=1_000, rate=0.0).count == 0
    assert plan_injection(total_events=1_000, rate=1.0).count == 1_000


def test_the_injected_count_does_not_depend_on_the_batch_size():
    _, fine, _ = _injected(0.05, max_events=25)
    _, coarse, _ = _injected(0.05, max_events=500)
    assert fine.injected == coarse.injected


def test_a_different_rate_gives_a_different_count():
    assert _injected(0.02)[1].injected < _injected(0.05)[1].injected


def test_every_variant_is_exercised_at_a_demo_rate():
    _, report, _ = _injected(0.05)
    assert set(report.by_variant) == set(VARIANT_CODES)
    assert sum(report.by_variant.values()) == report.injected


def test_the_variants_are_the_gateway_own_reason_codes():
    """The variant name IS the DLQ code, so `report.by_variant` is directly
    comparable to a DLQ depth breakdown with no mapping table to get wrong."""
    assert set(VARIANT_CODES) == {CODE_SCHEMA, CODE_BAD_TIME, CODE_UNKNOWN_ATTR, CODE_DUPLICATE_ID}


# --- determinism --------------------------------------------------------------


def test_the_plan_is_deterministic_for_a_seed():
    a = plan_injection(total_events=1_000, rate=0.05, seed=3)
    b = plan_injection(total_events=1_000, rate=0.05, seed=3)
    assert a.slots == b.slots


def test_a_different_seed_injects_at_different_positions():
    a = plan_injection(total_events=1_000, rate=0.05, seed=1)
    b = plan_injection(total_events=1_000, rate=0.05, seed=2)
    assert a.slots != b.slots
    assert a.count == b.count


def test_the_same_events_and_seed_produce_the_same_malformed_events():
    corpus = _corpus()
    batches = list(corpus.batches(40))
    total = count_events(batches)
    first = list(MalformedInjector(rate=0.1, seed=9).batches(iter(batches), total_events=total))
    second = list(MalformedInjector(rate=0.1, seed=9).batches(iter(batches), total_events=total))
    assert first == second


# --- the injection is distributed, not clustered ------------------------------


def test_no_two_injections_land_on_adjacent_events():
    """Adjacency would put two bad events in one small batch and leave the next
    twenty batches clean, which is exactly the clustering the demo must not show."""
    slots = [slot.index for slot in plan_injection(total_events=2_000, rate=0.05).slots]
    assert all(b - a >= 2 for a, b in zip(slots, slots[1:], strict=False))


def test_the_injected_events_are_spread_across_the_run():
    slots = [slot.index for slot in plan_injection(total_events=4_000, rate=0.05).slots]
    assert max(slots) - min(slots) >= 3_500, "injections clustered into part of the run"


def test_injected_events_land_in_many_different_batches():
    """Per-event rejection is exercised at scale only if the bad events are not
    all in one batch -- the gateway's per-event verdict is the thing being shown.
    At 5% and a 50-event batch, essentially every batch is contaminated."""
    batches, report, total = _injected(0.05, max_events=50)
    plan = plan_injection(total_events=total, rate=0.05, seed=7)
    contaminated = _per_batch_injections(batches, plan)
    assert len(contaminated) >= 0.9 * len(batches)
    assert len(contaminated) > 1


def test_no_single_batch_collects_more_than_its_share_of_the_bad_events():
    """A batch holding 30% of the garbage is clustering wearing a hat."""
    batches, report, total = _injected(0.05, max_events=50)
    plan = plan_injection(total_events=total, rate=0.05, seed=7)
    contaminated = _per_batch_injections(batches, plan)
    mean = report.injected / len(batches)
    assert max(contaminated.values()) <= 2 * mean + 1


# --- each variant is genuinely invalid ----------------------------------------


def test_a_clean_corpus_event_is_accepted_by_the_same_checks():
    """The control: without the injector these events pass. Otherwise the tests
    below would prove nothing about the corruption."""
    event = next(iter(_corpus().batches(2)))[0]
    assert gateway_codes([event]) == set()


def test_the_schema_variant_breaks_the_published_type_enum():
    event = next(iter(_corpus().batches(2)))[0]
    corrupt = _corrupt(CODE_SCHEMA, event)
    assert corrupt["type"] not in EVENT_TYPES
    assert gateway_codes([corrupt]) == {CODE_SCHEMA}


def test_the_bad_time_variant_is_not_rfc_3339():
    event = next(iter(_corpus().batches(2)))[0]
    corrupt = _corrupt(CODE_BAD_TIME, event)
    assert corrupt["time"] == ILLEGAL_TIME
    assert gateway_codes([corrupt]) == {CODE_BAD_TIME}


def test_the_unknown_attribute_variant_is_refused_by_the_ingress_decoder():
    event = next(iter(_corpus().batches(2)))[0]
    corrupt = _corrupt(CODE_UNKNOWN_ATTR, event)
    assert ILLEGAL_ATTRIBUTE in corrupt
    with pytest.raises(msgspec.DecodeError):
        decode_ingress(msgspec.json.encode([corrupt]))
    assert gateway_codes([corrupt]) == {CODE_UNKNOWN_ATTR}


def test_an_illegal_attribute_name_cannot_come_from_the_allowlist():
    from contracts.attributes import SAFE_CONTEXT_ATTRS, is_valid_attribute_name

    assert ILLEGAL_ATTRIBUTE not in SAFE_CONTEXT_ATTRS
    assert not is_valid_attribute_name(ILLEGAL_ATTRIBUTE)


def test_the_duplicate_id_variant_shares_a_source_and_id_in_one_batch():
    batch = next(iter(_corpus().batches(2)))
    first, second = batch[0], batch[1]
    corrupt = _corrupt(CODE_DUPLICATE_ID, second, partner=first)
    assert (corrupt["source"], corrupt["id"]) == (first["source"], first["id"])
    assert gateway_codes([first, corrupt]) == {CODE_DUPLICATE_ID}


def test_two_valid_events_with_different_ids_are_not_rejected():
    batch = next(iter(_corpus().batches(2)))
    assert gateway_codes(list(batch)) == set()


def test_injection_never_emits_application_abandoned():
    """The Queue team's Flink job synthesises it on the watermark timeout
    (SPEC.txt:338-339); emitting it here too would double-count drop-off."""
    batches, _, _ = _injected(0.10)
    assert not any("application-abandoned" in ev["type"] for b in batches for ev in b)


def test_every_injected_event_still_looks_like_an_event_of_its_tenant():
    """A bad event is bad in one specific way. Corrupting the envelope must not
    also move the event to another tenant, or the demo's per-tenant math breaks."""
    batches, _, _ = _injected(0.10)
    for batch in batches:
        assert len({ev["source"] for ev in batch}) == 1


# --- the ground-truth ledger --------------------------------------------------


def test_every_emitted_event_is_written_to_the_ledger_including_the_malformed_ones(tmp_path):
    """Reconciliation has to be able to tell "rejected on purpose" from "lost",
    and the ledger schema is unchanged -- only what is recorded differs."""
    path = tmp_path / "l.jsonl"
    total = count_events(_corpus().batches(SESSIONS))
    injector = MalformedInjector(rate=0.05)
    report = injector.build(_corpus().batches(SESSIONS), path, total_events=total)
    records = Ledger(path).read_all()
    assert len(records) == report.emitted
    assert report.injected == round(total * 0.05)
    assert {r.tenant for r in records} == set(TENANTS)


def test_a_one_event_batch_still_holds_a_duplicate_pair():
    """A corpus's final flush can be a single event, and one event cannot hold a
    duplicate. The partner is appended rather than the injection dropped, so the
    malformed count stays exact and the gateway still has a pair to reject."""
    event = next(iter(_corpus().batches(2)))[0]
    injector = MalformedInjector(rate=1.0, seed=1, variants=(CODE_DUPLICATE_ID,))
    assert injector.plan(1).variant_at(0) == CODE_DUPLICATE_ID

    out = list(injector.batches([[event]], total_events=1))
    assert injector.report.injected == 1
    assert injector.report.appended == 1
    assert len(out[0]) == 2
    assert (out[0][0]["source"], out[0][0]["id"]) == (out[0][1]["source"], out[0][1]["id"])
    assert gateway_codes(out[0]) == {CODE_DUPLICATE_ID}


def test_no_injection_is_ever_dropped_for_want_of_a_partner():
    """The count is the demo's headline claim, so it is checked over a run wide
    enough for the awkward batches -- small ones, odd tails -- to show up."""
    for rate in (0.05, 0.11, 0.25, 0.5):
        batches, report, total = _injected(rate, max_events=7)
        assert report.injected == round(total * rate)
        assert sum(len(b) for b in batches) == total + report.appended


def test_the_ledger_records_the_malformed_ids_so_they_can_be_matched_to_the_dlq(tmp_path):
    path = tmp_path / "l.jsonl"
    total = count_events(_corpus(seed=4).batches(SESSIONS))
    injector = MalformedInjector(rate=0.05, seed=4)
    report = injector.build(_corpus(seed=4).batches(SESSIONS), path, total_events=total)
    records = Ledger(path).read_all()
    # Every injected DUPLICATE_ID event reuses a partner's id, so the number of
    # (source, id) collisions in the ledger is exactly the number injected --
    # which is what lets a DLQ replay be matched back to a sent event.
    assert Ledger.duplicates(records) == report.by_variant[CODE_DUPLICATE_ID]
    assert len({(r.source, r.id) for r in records}) == len(records) - report.by_variant[CODE_DUPLICATE_ID]
    assert injector.plan(total).count == report.injected


def test_a_clean_run_writes_a_ledger_with_no_duplicates(tmp_path):
    path = tmp_path / "l.jsonl"
    total = count_events(_corpus().batches(SESSIONS))
    MalformedInjector(rate=0.0).build(_corpus().batches(SESSIONS), path, total_events=total)
    assert Ledger.duplicates(Ledger(path).read_all()) == 0


def test_the_injector_refuses_a_corpus_that_is_not_the_size_it_was_planned_for():
    """The trap that makes an exact count a lie: a second pass over the SAME
    corpus instance emits a different number of events, and injections planned
    past the end of the shorter run would be dropped with nothing failing."""
    corpus = _corpus()
    total = count_events(corpus.batches(SESSIONS))
    with pytest.raises(ValueError, match="counted"):
        list(MalformedInjector(rate=0.05).batches(corpus.batches(SESSIONS), total_events=total))


# --- the single-tenant flood --------------------------------------------------


def _flood(tenant: str = TENANTS[0], **kw) -> TenantFlood:
    kw.setdefault("sessions", 200)
    return TenantFlood(tenant, **kw)


def test_the_flood_targets_exactly_one_tenant():
    flood = _flood(TENANTS[2])
    assert {ev["source"] for b in flood.batches() for ev in b} == {f"/careers/{TENANTS[2]}"}


def test_the_flood_emits_the_traffic_it_was_asked_for():
    flood = _flood(sessions=150)
    assert sum(len(b) for b in flood.batches()) > 150


def test_the_flood_is_a_high_rate_stream_for_its_tenant():
    """The point of the mode: one tenant's volume is an order of magnitude above
    what the corpus would give it, which is what the per-tenant limiter sheds."""
    catalog = TenantCatalog(5, seed=7)
    flooded = catalog.ids[0]
    baseline = SkewedCorpus(catalog=catalog, seed=7, users_per_tenant=10)
    per_tenant = Counter(
        ev["source"] for b in baseline.batches(200) for ev in b
    )
    flood = TenantFlood(flooded, sessions=2_000, seed=7)
    flooded_events = sum(len(b) for b in flood.batches())
    assert flooded_events > 5 * per_tenant[f"/careers/{flooded}"]


def test_the_flood_users_are_disjoint_from_the_baseline_users():
    """The trap: a flooding user that reuses a baseline user's id inherits that
    user's sequence counter, and the two runs emit the same `sequence` twice --
    which silently breaks the ordering the whole run is measured on."""
    corpus = _corpus()
    baseline_users = {
        ev["data"]["candidate"]["user_id"] for b in corpus.batches(80) for ev in b
    }
    flood_users = {
        ev["data"]["candidate"]["user_id"] for b in _flood(sessions=200).batches() for ev in b
    }
    assert baseline_users & flood_users == set()


def test_flood_user_ids_are_distinguishable_from_baseline_user_ids():
    """Not a coin flip: `derive_users` mints 8 hex chars, so a flood id whose
    last eight characters are not hex can never collide with one."""
    for i in range(4):
        user = flood_user_id("tenant_0000", i)
        assert user[-8:] != user[-8:].upper().lower() or not all(
            c in "0123456789abcdef" for c in user[-8:]
        )


def test_the_combined_run_keeps_every_users_sequence_numbers_unique(tmp_path):
    path = tmp_path / "combined.jsonl"
    flooded = TENANTS[0]
    total = count_events(_corpus().batches(SESSIONS))
    MalformedInjector(rate=0.0).build(_corpus().batches(SESSIONS), path, total_events=total)
    _flood(flooded, sessions=300).build(path)

    by_user: dict[tuple[str, str], list[int]] = {}
    for r in Ledger(path).read_all():
        by_user.setdefault((r.source, r.user_pseudo), []).append(r.seq)
    assert any(len(seqs) > 1 for seqs in by_user.values()), "expected repeat users"
    for seqs in by_user.values():
        assert len(set(seqs)) == len(seqs), "a user was handed the same sequence twice"


def test_only_the_flooded_tenant_gains_volume(tmp_path):
    flooded = TENANTS[1]
    baseline_path, combined_path = tmp_path / "base.jsonl", tmp_path / "combined.jsonl"
    total = count_events(_corpus().batches(SESSIONS))
    for path in (baseline_path, combined_path):
        MalformedInjector(rate=0.0).build(_corpus().batches(SESSIONS), path, total_events=total)
    _flood(flooded, sessions=300).build(combined_path)

    before = Counter(r.tenant for r in Ledger(baseline_path).read_all())
    after = Counter(r.tenant for r in Ledger(combined_path).read_all())
    for tenant in TENANTS:
        if tenant == flooded:
            assert after[tenant] > before[tenant]
        else:
            assert after[tenant] == before[tenant], f"{tenant} was disturbed by the flood"


def test_the_flood_sequence_counter_continues_across_sessions_for_a_user():
    seen: dict[tuple[str, str], set[str]] = {}
    for batch in _flood(sessions=200).batches():
        for ev in batch:
            key = (ev["source"], ev["data"]["candidate"]["user_id"])
            assert ev["sequence"] not in seen.setdefault(key, set())
            seen[key].add(ev["sequence"])
    assert len(seen) > 1, "the flood should spread over its own users"


def test_the_flood_is_deterministic_for_a_seed():
    def shape(seed: int) -> list:
        return [
            (ev["type"], ev["sequence"], ev["sourcechannel"])
            for b in TenantFlood(TENANTS[0], sessions=60, seed=seed).batches()
            for ev in b
        ]

    assert shape(5) == shape(5)


def test_every_flood_event_is_a_valid_ingress_event():
    for batch in _flood(sessions=200).batches():
        assert len(decode_ingress(msgspec.json.encode(batch))) == len(batch)


def test_the_flood_never_emits_application_abandoned():
    assert not any(
        "application-abandoned" in ev["type"] for b in _flood(sessions=200).batches() for ev in b
    )


def test_the_flood_respects_the_batch_cap():
    for batch in _flood(sessions=200).batches(max_events=25):
        assert len(batch) <= 25


def test_the_flood_default_batch_is_under_the_gateway_cap():
    from app.config import MAX_EVENTS_PER_BATCH

    assert max(len(b) for b in _flood(sessions=200).batches()) <= MAX_EVENTS_PER_BATCH


def test_the_flood_never_emits_a_duplicate_id():
    batches = [b for b in _flood(sessions=200).batches()]
    for batch in batches:
        ids = [(ev["source"], ev["id"]) for ev in batch]
        assert len(set(ids)) == len(ids)


def test_a_flood_of_zero_sessions_is_empty_rather_than_an_error():
    assert list(TenantFlood(TENANTS[0], sessions=0).batches()) == []


def test_a_flood_with_no_users_is_refused():
    with pytest.raises(ValueError):
        TenantFlood(TENANTS[0], sessions=10, users=0)


def test_a_flood_with_no_tenant_is_refused():
    with pytest.raises(ValueError):
        TenantFlood("", sessions=10)


# --- the CLI ------------------------------------------------------------------


def test_the_cli_defaults_to_no_injection(tmp_path, capsys):
    assert main(["--sessions", "20", "--tenants", "3", "--ledger", str(tmp_path / "l.jsonl")]) == 0
    assert json.loads(capsys.readouterr().out)["injected"] == 0


def test_the_cli_reports_the_injected_count_so_the_demo_can_assert_it(tmp_path, capsys):
    path = tmp_path / "l.jsonl"
    assert main(
        [
            "--sessions", "120",
            "--tenants", "3",
            "--ledger", str(path),
            "--inject-invalid-rate", "5",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["injected"] > 0
    assert summary["injected"] == round(summary["emitted"] * 0.05)
    assert set(summary["by_variant"]) == set(VARIANT_CODES)
    assert summary["ledger_rows"] == len(Ledger(path).read_all())


def test_the_cli_floods_only_the_chosen_tenant(tmp_path, capsys):
    path = tmp_path / "l.jsonl"
    assert main(
        [
            "--sessions", "40",
            "--tenants", "4",
            "--ledger", str(path),
            "--flood-tenant", TENANTS[2],
            "--flood-sessions", "300",
        ]
    ) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["flood"]["tenant"] == TENANTS[2]
    assert summary["flood"]["events"] > 0
    counts = Counter(r.tenant for r in Ledger(path).read_all())
    assert counts[TENANTS[2]] > 10 * max(counts[t] for t in TENANTS if t != TENANTS[2])


def test_the_cli_refuses_a_rate_above_a_hundred(capsys):
    with pytest.raises(SystemExit):
        main(["--inject-invalid-rate", "150"])
    assert "100" in capsys.readouterr().err


def test_the_cli_is_off_without_a_ledger(tmp_path, capsys):
    assert main(["--sessions", "20", "--tenants", "3"]) == 0
    assert json.loads(capsys.readouterr().out)["emitted"] > 0
