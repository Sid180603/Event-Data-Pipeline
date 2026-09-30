"""Corpus tests: the properties the gateway and T11 depend on."""

from __future__ import annotations

from contracts.attributes import distinct_sources
from contracts.cloudevent import decode_batch
from contracts.ledger import Ledger
from driver.corpus import CorpusBuilder

TENANTS = [f"tenant_{i:03d}" for i in range(5)]


def _builder(**kw) -> CorpusBuilder:
    return CorpusBuilder(career_site_ids=TENANTS, users_per_tenant=10, **kw)


def test_every_batch_is_single_tenant():
    for batch in _builder().batches(60):
        assert distinct_sources(decode_batch(batch)) == 1


def test_batches_respect_the_event_cap():
    for batch in _builder().batches(60, max_events=25):
        assert len(batch) <= 25


def test_every_event_in_every_batch_decodes():
    for batch in _builder().batches(40):
        assert len(decode_batch(batch)) == len(batch)


def test_the_corpus_spans_every_tenant():
    seen = set()
    for batch in _builder().batches(60):
        seen |= {ev["source"] for ev in batch}
    assert len(seen) == len(TENANTS)


def test_all_three_source_channels_appear():
    seen = set()
    for batch in _builder(seed=11).batches(120):
        seen |= {ev["sourcechannel"] for ev in batch}
    assert seen == {"WEB_APP", "MOBILE_APP", "THIRD_PARTY_SERVICE"}


def test_building_writes_a_ledger_matching_the_event_count(tmp_path):
    batches, events = _builder().build(40, tmp_path / "l.jsonl")
    records = Ledger(tmp_path / "l.jsonl").read_all()
    assert events == len(records)
    assert batches > 0
    assert all(r.type.startswith("com.careerpage.career.") for r in records)


def test_the_ledger_has_no_duplicate_source_id_pairs(tmp_path):
    _builder().build(60, tmp_path / "l.jsonl")
    assert Ledger.duplicates(Ledger(tmp_path / "l.jsonl").read_all()) == 0


def test_ids_are_unique_within_a_tenant(tmp_path):
    _builder().build(60, tmp_path / "l.jsonl")
    records = Ledger(tmp_path / "l.jsonl").read_all()
    for tenant in TENANTS:
        ids = [r.id for r in records if r.source == f"/careers/{tenant}"]
        assert len(set(ids)) == len(ids)


def test_the_corpus_is_deterministic_for_a_given_seed(tmp_path):
    a = CorpusBuilder(career_site_ids=TENANTS, seed=3, users_per_tenant=5).build(
        30, tmp_path / "a.jsonl"
    )
    b = CorpusBuilder(career_site_ids=TENANTS, seed=3, users_per_tenant=5).build(
        30, tmp_path / "b.jsonl"
    )
    assert a == b


def test_a_different_seed_produces_different_traffic(tmp_path):
    a = CorpusBuilder(career_site_ids=TENANTS, seed=1, users_per_tenant=5).build(30, tmp_path / "a.jsonl")
    b = CorpusBuilder(career_site_ids=TENANTS, seed=2, users_per_tenant=5).build(30, tmp_path / "b.jsonl")
    assert a != b


def test_sequence_continues_across_sessions_for_the_same_user(tmp_path):
    _builder(seed=5).build(80, tmp_path / "l.jsonl")
    records = Ledger(tmp_path / "l.jsonl").read_all()
    by_user: dict[tuple[str, str], list[int]] = {}
    for r in records:
        by_user.setdefault((r.source, r.user_pseudo), []).append(r.seq)
    multi = [seqs for seqs in by_user.values() if len(seqs) > 1]
    assert multi, "expected at least one user with multiple sessions"
    assert all(len(set(seqs)) == len(seqs) for seqs in by_user.values())


def test_high_dropoff_yields_fewer_terminal_events():
    low = CorpusBuilder(career_site_ids=TENANTS, seed=2, users_per_tenant=10, drop_off_rate=0.0)
    high = CorpusBuilder(career_site_ids=TENANTS, seed=2, users_per_tenant=10, drop_off_rate=0.9)

    def terminals(b):
        return sum(
            1
            for batch in b.batches(60)
            for ev in batch
            if ev["type"].endswith(("application-submitted", "application-draft-saved"))
        )

    assert terminals(high) < terminals(low)


def test_the_driver_never_generates_application_abandoned(tmp_path):
    _builder().build(80, tmp_path / "l.jsonl")
    records = Ledger(tmp_path / "l.jsonl").read_all()
    assert not any(r.type.endswith("application-abandoned") for r in records)
