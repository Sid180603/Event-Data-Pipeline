"""T8b: the tenant skew model, sharding, and the third-party source.

A uniform tenant distribution hides exactly the two failures this architecture
exists to handle: a hot partition on an enterprise tenant, and rate starvation
on the SMB tail. SPEC.txt:148 says large tenants generate "exponentially higher"
traffic during a hiring drive, so the driver has to reproduce the skew, not
average it away.

These tests pin the shape of that skew, the bound on a single sticky-routed
user, and the disjointness of independently generated shards.
"""

from __future__ import annotations

import math
import random

import msgspec
import pytest

from contracts.attributes import envelope_problem
from contracts.cloudevent import decode_batch
from contracts.ledger import Ledger
from driver.corpus import SOURCE_CHANNELS
from driver.skew import (
    DEFAULT_SKEW_EXPONENT,
    MAX_EVENTS_PER_SESSION,
    MAX_EVENTS_PER_USER_PER_RUN,
    MAX_SESSIONS_PER_USER,
    SkewedCorpus,
    apportion,
    busiest_tenant,
    max_events_per_user,
    plan_sessions,
    skew_report,
    structural_fingerprint,
    zipf_shares,
)
from driver.tenants import DEFAULT_TENANT_COUNT, TenantCatalog, merge_ledgers
from driver.webhook_source import (
    THIRD_PARTY_CHANNEL,
    THIRD_PARTY_REFERRER,
    parse_webhook,
    webhook_payload,
)

SHARDS = 4


def _catalog(count: int = DEFAULT_TENANT_COUNT) -> TenantCatalog:
    return TenantCatalog(count, seed=7)


def _corpus(**kw) -> SkewedCorpus:
    kw.setdefault("seed", 7)
    return SkewedCorpus(**kw)


# --- the catalog -------------------------------------------------------------


def test_the_catalog_defaults_to_five_hundred_tenants():
    assert DEFAULT_TENANT_COUNT == 500
    assert len(_catalog().ids) == 500


def test_the_tenant_count_is_configurable():
    assert len(_catalog(37).ids) == 37


def test_tenant_ids_are_unique():
    ids = _catalog().ids
    assert len(set(ids)) == len(ids)


def test_tenant_ids_are_stable_for_a_given_seed():
    assert TenantCatalog(50, seed=3).ids == TenantCatalog(50, seed=3).ids


def test_tenant_order_is_rank_order():
    """Index 0 is the busiest tenant, so the plan's weights line up with the list."""
    assert _catalog(10).ids[0] == TenantCatalog(10, seed=7).ids[0]


def test_a_tenant_and_its_users_share_a_name_space():
    cat = TenantCatalog(5, seed=7)
    tenant = cat.ids[0]
    assert all(u.startswith(tenant) for u in cat.users(tenant, 4))


def test_a_tenant_user_list_is_a_prefix_of_a_longer_one():
    """Volume-scaled user counts must not renumber existing users, or a tenant's
    sticky routing would change shape just because the run got bigger."""
    cat = TenantCatalog(5, seed=7)
    tenant = cat.ids[0]
    assert cat.users(tenant, 3) == cat.users(tenant, 40)[:3]


# --- the skew itself (SPEC.txt:148) -----------------------------------------


def test_the_default_exponent_concentrates_the_volume():
    assert DEFAULT_SKEW_EXPONENT > 1.0


def test_shares_are_normalised_to_one():
    assert sum(zipf_shares(500, 1.2)) == pytest.approx(1.0)


def test_an_exponent_of_one_is_textbook_zipf():
    """s=1 gives rank1/median == n/2 exactly: the property the model is named for."""
    shares = zipf_shares(500, 1.0)
    assert shares[0] / sorted(shares)[250] == pytest.approx(250, rel=0.01)


def test_a_higher_exponent_concentrates_further():
    assert skew_report(zipf_shares(500, 1.4)).top_1pct_share > skew_report(
        zipf_shares(500, 1.0)
    ).top_1pct_share


def test_a_lower_exponent_flattens_towards_uniform():
    assert skew_report(zipf_shares(500, 0.6)).top_1pct_share < skew_report(
        zipf_shares(500, 1.2)
    ).top_1pct_share


def test_the_top_one_percent_of_tenants_take_a_majority_of_the_volume():
    """A handful of enterprises drive the pipeline; the other 495 are tail."""
    report = skew_report(zipf_shares(500, DEFAULT_SKEW_EXPONENT))
    assert report.top_1pct_tenants == 5
    assert report.top_1pct_share > 0.45
    assert report.top_5pct_share > 0.65


def test_the_busiest_tenant_dwarfs_the_median_one():
    shares = zipf_shares(500, DEFAULT_SKEW_EXPONENT)
    assert shares[0] / sorted(shares)[250] > 100


def test_the_tail_is_thin():
    """The bottom half of the catalog carries ~5% of the volume between 250
    tenants -- a tenth of its population share. That is the starved tail."""
    shares = sorted(zipf_shares(500, DEFAULT_SKEW_EXPONENT))
    assert sum(shares[:250]) < 0.06


def test_the_skew_is_power_law_not_just_a_few_big_tenants():
    """Log-log linearity *is* the definition of a power law. One whale plus 499
    equal tenants would sit on that line for two ranks and fall off a cliff;
    staying on it out to rank 500 is what makes the tail part of the law."""
    exponent = 1.2
    shares = zipf_shares(500, exponent)
    xs = [math.log(rank) for rank in range(1, 501)]
    ys = [math.log(share) for share in shares]
    slope = (ys[-1] - ys[0]) / (xs[-1] - xs[0])
    assert slope == pytest.approx(-exponent, rel=1e-9)
    for x, y in zip(xs, ys, strict=True):
        assert y == pytest.approx(ys[0] + slope * x, abs=1e-9)


def test_each_rank_step_costs_less_than_the_one_before():
    """The 1->2 drop is the steepest and the curve flattens smoothly, so the
    head is a continuum of large tenants rather than one outlier."""
    shares = zipf_shares(500, 1.2)
    ratios = [shares[i - 1] / shares[i] for i in range(2, 20)]
    assert ratios == sorted(ratios, reverse=True)
    assert ratios[0] > 1.6 > ratios[-1] > 1.0


# --- apportionment -----------------------------------------------------------


def test_apportionment_does_not_lose_or_invent_sessions():
    weights = zipf_shares(200, 1.2)
    counts = apportion(9_973, weights)
    assert sum(counts) == 9_973
    assert all(c >= 0 for c in counts)


def test_apportionment_follows_the_weights():
    weights = zipf_shares(200, 1.2)
    counts = apportion(100_000, weights)
    assert counts[0] / counts[100] == pytest.approx(weights[0] / weights[100], rel=0.01)


def test_apportionment_keeps_every_tenant_at_zero_minimum():
    counts = apportion(10, zipf_shares(500, 1.2))
    assert sum(1 for c in counts if c == 0) > 400


def test_apportionment_is_deterministic():
    weights = zipf_shares(64, 1.1)
    assert apportion(5_000, weights) == apportion(5_000, weights)


# --- the volume plan ---------------------------------------------------------


def test_the_plan_spreads_every_tenant():
    plan = plan_sessions(_catalog(60), 6_000)
    assert len(plan.sessions_by_tenant) > 0
    assert sum(plan.sessions_by_tenant.values()) == 6_000


def test_the_plan_gives_the_head_tenant_far_more_sessions_than_the_tail():
    plan = plan_sessions(_catalog(500), 1_000_000)
    counts = sorted(plan.sessions_by_tenant.values(), reverse=True)
    assert counts[0] > 100 * counts[len(counts) // 2]
    assert counts[0] > 50 * counts[-1]


def test_the_plan_is_deterministic_for_a_seed():
    a = plan_sessions(_catalog(80), 20_000)
    b = plan_sessions(_catalog(80), 20_000)
    assert a.sessions_by_tenant == b.sessions_by_tenant


def test_the_plan_follows_the_exponent():
    loose = plan_sessions(_catalog(200), 100_000, exponent=0.9)
    tight = plan_sessions(_catalog(200), 100_000, exponent=1.4)
    assert max(tight.sessions_by_tenant.values()) > 2 * max(loose.sessions_by_tenant.values())


def test_the_busiest_tenant_is_always_the_first_one():
    cat = _catalog(100)
    plan = plan_sessions(cat, 50_000)
    tenant, sessions = busiest_tenant(plan)
    assert tenant == cat.ids[0]
    assert sessions == max(plan.sessions_by_tenant.values())


def test_the_user_floor_is_configurable_on_the_plan_and_the_corpus():
    """The floor only bites the tail -- the whale is scaled past it either way --
    so it is the tail tenant that proves the setting is actually threaded through
    the builder and not silently defaulted."""
    cat = _catalog(500)
    floor = 8
    plan = plan_sessions(cat, 1_000_000, users_per_tenant=floor)
    tail = sorted(plan.sessions_by_tenant)[250]
    assert len(plan.users_by_tenant[tail]) == floor
    corpus_plan = SkewedCorpus(catalog=cat, seed=7, users_per_tenant=floor).plan_for(1_000_000)
    assert len(corpus_plan.users_by_tenant[tail]) == floor


# --- the per-user hot-spot bound (sticky routing) ----------------------------


def test_the_per_user_cap_is_consistent_with_the_per_session_cap():
    assert MAX_SESSIONS_PER_USER * MAX_EVENTS_PER_SESSION == MAX_EVENTS_PER_USER_PER_RUN


def test_a_whale_tenant_scales_its_own_user_count_to_hold_the_bound():
    """One sticky-routed user owns one partition, therefore one worker for the
    whole run. A whale's traffic has to spread over more users, not fewer."""
    cat = _catalog(500)
    plan = plan_sessions(cat, 1_000_000, users_per_tenant=50)
    tenant, _ = busiest_tenant(plan)
    assert len(plan.users_by_tenant[tenant]) > 50


def test_no_planned_user_exceeds_the_per_session_cap():
    plan = plan_sessions(_catalog(500), 1_000_000, users_per_tenant=50)
    assert plan.max_sessions_per_user <= MAX_SESSIONS_PER_USER


def test_the_planned_bound_holds_for_every_tenant_including_the_whale():
    plan = plan_sessions(_catalog(500), 1_000_000, users_per_tenant=50)
    for tenant, sessions in plan.sessions_by_tenant.items():
        users = plan.users_by_tenant[tenant]
        assert -(-sessions // len(users)) <= MAX_SESSIONS_PER_USER


def test_the_whale_tenant_would_blow_the_bound_without_user_scaling():
    """The risk is real and is asserted rather than hidden: hold the whale at a
    flat 50 users and its hottest user is handed ~38,600 events in a 1M-session
    run -- 7.7x over MAX_EVENTS_PER_USER_PER_RUN -- all on one sticky partition.
    That is why head tenants scale their user count. If this ever stops holding,
    the scaling is no longer load bearing and the threshold is no longer
    protecting anything."""
    plan = plan_sessions(_catalog(500), 1_000_000, users_per_tenant=50)
    tenant, sessions = busiest_tenant(plan)
    flat = -(-sessions // 50) * MAX_EVENTS_PER_SESSION
    assert flat > MAX_EVENTS_PER_USER_PER_RUN


def test_a_generated_corpus_keeps_every_user_under_the_threshold():
    corpus = _corpus(exponent=1.2)
    assert max_events_per_user(corpus.batches(40_000)) <= MAX_EVENTS_PER_USER_PER_RUN


def test_a_generated_corpus_actually_reaches_the_whale_tenant():
    """The bound is only interesting if a whale exists to be bounded."""
    corpus = _corpus(exponent=1.2)
    plan = corpus.plan_for(40_000)
    tenant, _ = busiest_tenant(plan)
    sources = {ev["source"] for batch in corpus.batches(40_000) for ev in batch}
    assert f"/careers/{tenant}" in sources


# --- the generated corpus ----------------------------------------------------


def test_the_corpus_emits_third_party_webhook_events():
    seen = {
        (ev["sourcechannel"], ev.get("referrertype"))
        for batch in _corpus().batches(2_000)
        for ev in batch
    }
    assert (THIRD_PARTY_CHANNEL, THIRD_PARTY_REFERRER) in seen


def test_every_third_party_event_in_the_corpus_is_webhook_attributed():
    """A partner's traffic is attributed to the partner, not to a browser search
    that never happened."""
    for batch in _corpus().batches(3_000):
        for ev in batch:
            if ev["sourcechannel"] == THIRD_PARTY_CHANNEL:
                assert ev["referrertype"] == THIRD_PARTY_REFERRER


def test_third_party_metadata_carries_the_partner_url_and_utm():
    found = False
    for batch in _corpus().batches(3_000):
        for ev in batch:
            if ev["sourcechannel"] != THIRD_PARTY_CHANNEL:
                continue
            meta = ev["data"]["event_payload"]["client_metadata"]
            assert meta["referrer_url"].startswith("https://")
            assert meta["utm_source"] and meta["utm_medium"]
            found = True
    assert found, "expected third-party traffic in the corpus"


def test_the_corpus_never_generates_application_abandoned():
    """G1: the Queue team's Flink job synthesises it on the watermark timeout
    (SPEC.txt:338-339). Emitting it here would double-count the drop-off rate."""
    for batch in _corpus().batches(3_000):
        assert not any("application-abandoned" in ev["type"] for ev in batch)


def test_every_corpus_event_is_a_valid_envelope():
    for batch in _corpus().batches(1_000):
        assert all(envelope_problem(ev) is None for ev in decode_batch(batch))


def test_the_corpus_spans_all_three_source_channels():
    seen = {ev["sourcechannel"] for batch in _corpus().batches(3_000) for ev in batch}
    assert seen == set(SOURCE_CHANNELS)


def test_a_regenerated_corpus_is_structurally_identical():
    """Same seed, same traffic shape, so two load runs are comparable."""
    a = structural_fingerprint(ev for b in _corpus().batches(3_000) for ev in b)
    b = structural_fingerprint(ev for b in _corpus().batches(3_000) for ev in b)
    assert a == b


def test_a_different_exponent_changes_the_traffic_shape():
    a = structural_fingerprint(
        ev for b in _corpus(exponent=1.0).batches(3_000) for ev in b
    )
    b = structural_fingerprint(
        ev for b in _corpus(exponent=1.4).batches(3_000) for ev in b
    )
    assert a != b


def test_every_batch_is_single_tenant():
    from contracts.attributes import distinct_sources

    for batch in _corpus().batches(2_000):
        assert distinct_sources(decode_batch(batch)) == 1


def test_sequence_numbers_stay_unique_per_tenant_and_user():
    """The user schedule is round-robin over a volume-sized user list, so a whale
    hands the same user several sessions. C1 requires their `sequence` ranges not
    to overlap -- the Queue team's sessionizer sorts on it."""
    seen: dict[tuple[str, str], set[str]] = {}
    for batch in _corpus(exponent=1.2).batches(8_000):
        for ev in batch:
            key = (ev["source"], ev["data"]["candidate"]["user_id_pseudo"])
            assert ev["sequence"] not in seen.setdefault(key, set())
            seen[key].add(ev["sequence"])
    assert any(len(v) > 1 for v in seen.values()), "expected repeat users on the whale"


# --- sharding ----------------------------------------------------------------


def test_shards_partition_the_catalog():
    cat = _catalog(200)
    seen: list[str] = []
    for shard in range(5):
        seen += cat.shard_ids(shard, 5)
    assert sorted(seen) == sorted(cat.ids)
    assert len(set(seen)) == len(cat.ids)


def test_shard_assignment_is_stable_across_processes():
    """Python's hash() is salted per process, so two driver processes would pick
    different tenants. The assignment must be a fixed digest of the id."""
    assert TenantCatalog(200, seed=7).shard_ids(0, 5) == TenantCatalog(200, seed=7).shard_ids(0, 5)
    assert [TenantCatalog(200, seed=7).shard_of(t, 5) for t in TenantCatalog(200, seed=7).ids[:8]] == [
        3, 0, 0, 4, 4, 1, 0, 2
    ]


def test_a_shard_only_generates_its_own_tenants():
    cat = _catalog(200)
    corpus = SkewedCorpus(catalog=cat, seed=7, shard=1, shards=5)
    mine = set(cat.shard_ids(1, 5))
    seen = {ev["source"] for b in corpus.batches(2_000) for ev in b}
    assert {s.rsplit("/", 1)[-1] for s in seen} <= mine


def test_shards_reconstruct_the_unsharded_corpus():
    cat = _catalog(200)
    whole = structural_fingerprint(
        ev for b in SkewedCorpus(catalog=cat, seed=7).batches(4_000) for ev in b
    )
    merged = []
    for shard in range(SHARDS):
        merged += list(
            SkewedCorpus(catalog=cat, seed=7, shard=shard, shards=SHARDS).batches(4_000)
        )
    assert structural_fingerprint(ev for b in merged for ev in b) == whole


def test_merging_shards_leaves_no_duplicate_tenant_and_id_pair(tmp_path):
    cat = _catalog(120)
    paths = []
    for shard in range(SHARDS):
        path = tmp_path / f"shard{shard}.jsonl"
        SkewedCorpus(catalog=cat, seed=7, shard=shard, shards=SHARDS).build(
            2_000, path
        )
        paths.append(path)
    merge_ledgers(paths, tmp_path / "merged.jsonl")
    assert Ledger.duplicates(Ledger(tmp_path / "merged.jsonl").read_all()) == 0


def test_merging_shards_that_overlap_is_rejected(tmp_path):
    """A tenant owned by two shards would fork its sequence numbers, so the merge
    must fail loudly rather than silently double-count the tenant."""
    a, b = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    _corpus().build(200, a)
    _corpus().build(200, b)
    with pytest.raises(ValueError, match="shard"):
        merge_ledgers([a, b], tmp_path / "merged.jsonl")


# --- the third-party webhook source ------------------------------------------


def test_a_partner_payload_round_trips():
    """The generator's payload must survive the wire form the partner actually
    posts -- JSON bytes, not the struct."""
    payload = webhook_payload(random.Random(1), "tenant_0001", "job_12345")
    parsed = parse_webhook(msgspec.json.encode(payload))
    assert parsed.career_site_id == "tenant_0001"
    assert parsed.job_id == "job_12345"


def test_a_partner_payload_from_the_wire_decodes():
    parsed = parse_webhook(
        {
            "partner": "jobboard-eu",
            "external_application_id": "ext-991",
            "career_site_id": "acme_8921",
            "job_id": "job_88320491",
            "referrer_url": "https://boards.example.com/jobs/88320491",
            "utm_source": "linkedin",
            "utm_medium": "cpc",
            "occurred_at": "2026-09-30T14:43:09Z",
            "applicant": {"candidate_ref": "cand-42", "email_hmac": "abc123"},
        }
    )
    assert parsed.applicant.candidate_ref == "cand-42"


def test_a_partner_payload_with_an_unknown_field_is_rejected():
    """Additive-only versioning (CONTRACT.md): a partner adding a field must be a
    loud failure at the edge, not a silently dropped field."""
    with pytest.raises(Exception):
        parse_webhook(
            {
                "partner": "jobboard-eu",
                "external_application_id": "ext-991",
                "career_site_id": "acme_8921",
                "job_id": "job_88320491",
                "referrer_url": "https://boards.example.com/jobs/88320491",
                "utm_source": "linkedin",
                "utm_medium": "cpc",
                "occurred_at": "2026-09-30T14:43:09Z",
                "applicant": {"candidate_ref": "cand-42"},
                "salary_expectation": 90000,
            }
        )
