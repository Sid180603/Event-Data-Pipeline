"""T7 per-tenant token bucket. RED before implementation.

No test here sleeps. Time is injected (`FakeClock`) so refill math is exact
rather than approximately-right-after-a-`sleep(0.01)`.
"""

from __future__ import annotations

import math
import threading
import time
from collections import defaultdict

import pytest

from app.ratelimit import (
    RETRY_AFTER_HEADER,
    TOO_MANY_REQUESTS,
    BucketRegistry,
    RateLimitDecision,
    TenantLimits,
    TokenBucket,
)

# --- Injectable clock --------------------------------------------------------


class FakeClock:
    """Monotonic clock under test control."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# --- Criterion 1/2: configurable rate + burst, one token per event ------------


def test_acquire_consumes_exactly_one_token_and_allows():
    clock = FakeClock()
    bucket = TokenBucket(rate=10.0, burst=10, clock=clock)
    decision = bucket.acquire()
    assert isinstance(decision, RateLimitDecision)
    assert decision.allowed is True
    assert decision.remaining == pytest.approx(9.0)
    assert bucket.consumed == 1


def test_bucket_allows_exactly_its_burst_then_denies():
    bucket = TokenBucket(rate=1.0, burst=5, clock=FakeClock())
    decisions = [bucket.acquire() for _ in range(7)]
    assert [d.allowed for d in decisions] == [True] * 5 + [False] * 2
    assert bucket.consumed == 5
    assert bucket.denied == 2


def test_rate_and_burst_are_configurable_per_bucket():
    slow = TokenBucket(rate=1.0, burst=2, clock=FakeClock())
    fast = TokenBucket(rate=1000.0, burst=7, clock=FakeClock())
    assert [slow.acquire().allowed for _ in range(3)] == [True, True, False]
    assert [fast.acquire().allowed for _ in range(8)] == [True] * 7 + [False]


def test_rate_must_be_positive_and_burst_at_least_one():
    with pytest.raises(ValueError):
        TokenBucket(rate=0.0, burst=5)
    with pytest.raises(ValueError):
        TokenBucket(rate=-1.0, burst=5)
    with pytest.raises(ValueError):
        TokenBucket(rate=1.0, burst=0)


# --- Criterion 3: denial is a 429 signal carrying Retry-After -----------------


def test_denial_is_a_429_signal_with_a_retry_after_value():
    assert TOO_MANY_REQUESTS == 429
    assert RETRY_AFTER_HEADER == "Retry-After"
    bucket = TokenBucket(rate=2.0, burst=1, clock=FakeClock())
    assert bucket.acquire().allowed is True
    denied = bucket.acquire()
    assert denied.allowed is False
    # The handler (app/middleware) answers `TOO_MANY_REQUESTS` and copies
    # `retry_after` into the Retry-After header; the value is a positive int.
    assert isinstance(denied.retry_after, int)
    assert denied.retry_after >= 1


def test_retry_after_is_the_whole_seconds_until_one_token_exists():
    clock = FakeClock()
    bucket = TokenBucket(rate=2.0, burst=2, clock=clock)
    assert bucket.acquire().allowed and bucket.acquire().allowed
    # Bucket is empty: 0.5s buys one token, but Retry-After is integral seconds.
    assert bucket.acquire().retry_after == 1
    clock.advance(0.25)  # half a token: 0.25s short, still rounds up to 1s
    assert bucket.acquire().retry_after == 1
    clock.advance(0.25)  # one whole token now
    assert bucket.acquire().allowed is True
    assert bucket.acquire().retry_after == 1


def test_allowed_decision_carries_no_wait():
    decision = TokenBucket(rate=1.0, burst=1, clock=FakeClock()).acquire()
    assert decision.allowed is True
    assert decision.retry_after == 0


# --- Criterion 8: lazy refill, no background thread, no sleeping -------------


def test_refill_is_proportional_to_elapsed_time():
    clock = FakeClock()
    bucket = TokenBucket(rate=10.0, burst=100, clock=clock)
    for _ in range(100):
        bucket.acquire()
    assert bucket.acquire().allowed is False
    clock.advance(0.25)  # 0.25s * 10/s = 2.5 tokens
    assert bucket.acquire().allowed is True
    assert bucket.acquire().allowed is True
    assert bucket.acquire().allowed is False
    assert bucket.consumed == 102


def test_refill_does_not_accumulate_credit_beyond_burst():
    clock = FakeClock()
    bucket = TokenBucket(rate=1.0, burst=3, clock=clock)
    for _ in range(3):
        bucket.acquire()
    clock.advance(10_000.0)  # 10,000 tokens of credit offered
    assert [bucket.acquire().allowed for _ in range(4)] == [True, True, True, False]
    assert bucket.denied == 1


def test_refill_boundary_exactly_one_token_per_period():
    clock = FakeClock()
    bucket = TokenBucket(rate=2.0, burst=2, clock=clock)  # one token per 0.5s
    assert bucket.acquire().allowed and bucket.acquire().allowed
    assert bucket.acquire().allowed is False
    clock.advance(0.499)  # 0.998 tokens: still short
    assert bucket.acquire().allowed is False
    clock.advance(0.001)  # exactly 0.5s: exactly one token, and it is usable
    assert bucket.acquire().allowed is True
    assert bucket.acquire().allowed is False


def test_zero_elapsed_time_grants_nothing():
    clock = FakeClock()
    bucket = TokenBucket(rate=1000.0, burst=1, clock=clock)
    assert bucket.acquire().allowed is True
    for _ in range(10):  # same instant, 10 times over
        assert bucket.acquire().allowed is False


def test_denial_does_not_bank_credit_for_the_tenant():
    """A denied call must still advance the refill clock, or a client that keeps
    hammering a dry bucket gets every token that accrued since its first try."""
    clock = FakeClock()
    bucket = TokenBucket(rate=1.0, burst=1, clock=clock)
    assert bucket.acquire().allowed is True
    clock.advance(0.5)
    assert bucket.acquire().allowed is False
    clock.advance(0.5)  # 1s total since the last *successful* take
    assert bucket.acquire().allowed is True


def test_bucket_never_sleeps_and_spawns_no_thread(monkeypatch):
    def _boom(*a, **k):  # pragma: no cover - only runs if the code is wrong
        raise AssertionError("rate limiting must not sleep")

    monkeypatch.setattr(time, "sleep", _boom)
    clock = FakeClock()
    bucket = TokenBucket(rate=1000.0, burst=1, clock=clock)
    before = threading.active_count()
    for step in range(50):
        clock.advance(0.01)  # lazy refill: time only moves because we asked it to
        bucket.acquire()
    assert threading.active_count() == before


# --- Criterion 4: per-bucket locks, no global lock ---------------------------


def test_each_bucket_has_its_own_lock():
    a = TokenBucket(rate=10.0, burst=10, clock=FakeClock())
    b = TokenBucket(rate=10.0, burst=10, clock=FakeClock())
    assert a._lock is not b._lock
    assert isinstance(a._lock, type(threading.Lock()))


def test_acquire_holds_the_buckets_own_lock_across_the_whole_read_modify_write():
    """The lock must span refill + compare + decrement, not just one of them.

    Checked deterministically rather than by racing: under CPython's GIL a
    lock-free check-and-decrement survives a thread-storm test, so only
    observing the block can prove the lock is really taken.
    """
    bucket = TokenBucket(rate=1e9, burst=100, clock=FakeClock())
    done: list[RateLimitDecision] = []
    bucket._lock.acquire()  # stand in for a slow section in another request
    t = threading.Thread(target=lambda: done.append(bucket.acquire()))
    t.start()
    t.join(timeout=0.5)
    assert t.is_alive(), "acquire proceeded while another caller held the bucket"
    assert done == [] and bucket.consumed == 0
    bucket._lock.release()
    t.join(timeout=5)
    assert done == [RateLimitDecision(True, 0, 99.0)]


def test_one_tenants_held_lock_never_blocks_another_tenant():
    """Criterion 4: contention is per-bucket. One mutex in front of the registry
    (or a lock shared by all buckets) would make this block, and 500 tenants
    would serialise behind whichever one is slowest."""
    slow = TokenBucket(rate=1e9, burst=2000, clock=FakeClock())
    fast = TokenBucket(rate=1e9, burst=2000, clock=FakeClock())
    served: list[RateLimitDecision] = []
    slow._lock.acquire()
    try:
        t = threading.Thread(target=lambda: served.extend(fast.acquire() for _ in range(1000)))
        t.start()
        t.join(timeout=10)  # a shared lock would never get here
        assert not t.is_alive(), "fast waited on slow's lock: contention is not per-bucket"
    finally:
        slow._lock.release()
    assert len(served) == 1000 and all(d.allowed for d in served)


def test_no_lost_updates_under_concurrency():
    """Frozen clock, so refill is impossible: any allowance above the burst, or
    a request missing from the tally, is a lost update. This pins the
    accounting; the two tests above pin the lock that makes it true."""
    tenants, threads, iterations, burst = 12, 8, 10, 30
    clock = FakeClock()
    limits = {f"t{i}": TenantLimits(rate=1.0, burst=burst) for i in range(tenants)}
    registry = BucketRegistry(limits, default=TenantLimits(rate=1.0, burst=burst), clock=clock)

    allowed: dict[str, list[bool]] = defaultdict(list)
    gates = {f"t{i}": threading.Barrier(threads) for i in range(tenants)}

    def worker(tenant: str) -> None:
        for _ in range(iterations):
            gates[tenant].wait()  # maximise the collision window
            allowed[tenant].append(registry.acquire(tenant).allowed)

    pool = [
        threading.Thread(target=worker, args=(f"t{i}",))
        for i in range(tenants)
        for _ in range(threads)
    ]
    for t in pool:
        t.start()
    for t in pool:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in pool)

    for i in range(tenants):
        tenant = f"t{i}"
        got = allowed[tenant]
        assert len(got) == threads * iterations
        assert sum(got) == burst, f"{tenant} lost updates: {sum(got)} > {burst}"
        bucket = registry.bucket_for(tenant)
        assert bucket.consumed == sum(got)
        assert bucket.denied == len(got) - sum(got)


# --- Registry: criteria 5, 7 --------------------------------------------------


def test_new_tenant_gets_a_bucket_lazily():
    clock = FakeClock()
    limits = {"acme": TenantLimits(rate=10.0, burst=10)}
    registry = BucketRegistry(limits, default=TenantLimits(rate=2.0, burst=2), clock=clock)
    assert registry.summary().tenant_count == 1
    assert registry.acquire("newcomer").allowed is True  # default limits apply
    assert registry.summary().tenant_count == 2


def test_unconfigured_tenant_uses_the_default_limits():
    registry = BucketRegistry(
        {}, default=TenantLimits(rate=1.0, burst=1), clock=FakeClock()
    )
    assert [registry.acquire("x").allowed for _ in range(3)] == [True, False, False]


def test_bucket_is_created_once_and_reused_per_tenant():
    registry = BucketRegistry({}, default=TenantLimits(rate=1.0, burst=1), clock=FakeClock())
    first = registry.bucket_for("acme")
    for _ in range(100):
        assert registry.bucket_for("acme") is first
    assert registry.bucket_for("other") is not first


def test_registry_holds_no_global_lock():
    """A single registry-wide mutex would serialise 500 tenants behind each
    other; lazy creation relies on `dict.setdefault` being one atomic call."""
    registry = BucketRegistry({}, default=TenantLimits(rate=1.0, burst=1))
    locks = [v for k, v in vars(registry).items() if isinstance(v, type(threading.Lock()))]
    assert locks == []


def test_concurrent_first_touch_creates_exactly_one_bucket():
    clock = FakeClock()
    registry = BucketRegistry({}, default=TenantLimits(rate=1.0, burst=1), clock=clock)
    seen: list[TokenBucket] = []
    gate = threading.Barrier(32)

    def worker() -> None:
        gate.wait()
        seen.append(registry.bucket_for("acme"))

    pool = [threading.Thread(target=worker) for _ in range(32)]
    for t in pool:
        t.start()
    for t in pool:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in pool)
    assert len(seen) == 32
    assert len({id(b) for b in seen}) == 1


def test_registry_summary_reports_tenant_count_and_configured_rate():
    registry = BucketRegistry(
        {"acme": TenantLimits(rate=120.0, burst=240), "globex": TenantLimits(rate=7.5, burst=15)},
        default=TenantLimits(rate=1.0, burst=1),
        clock=FakeClock(),
    )
    registry.acquire("acme")
    registry.acquire("acme")
    summary = registry.summary()
    assert summary.tenant_count == 2
    assert summary.configured_tenant_count == 2
    assert summary.rates == {"acme": 120.0, "globex": 7.5}
    # The rate we publish is the rate we enforce, which is the sticky-routing
    # precondition made checkable.
    bucket = registry.bucket_for("acme")
    assert summary.rates["acme"] == bucket.rate


def test_summary_counts_only_tenants_this_process_has_seen():
    clock = FakeClock()
    registry = BucketRegistry(
        {f"t{i}": TenantLimits(rate=10.0, burst=10) for i in range(500)},
        default=TenantLimits(rate=1.0, burst=1),
        clock=clock,
    )
    assert registry.summary().tenant_count == 500  # configured, not yet materialised
    for i in range(0, 500, 100):
        registry.acquire(f"t{i}")
    assert registry.summary().tenant_count == 500
    assert registry.bucket_for("t0").consumed == 1
    assert registry.bucket_for("t1").consumed == 0


# --- Criterion 6: a whale is a whale, and nobody else notices ----------------


def test_whale_gets_its_full_burst_while_other_499_are_unaffected():
    clock = FakeClock()
    whale = TenantLimits(rate=1000.0, burst=1000)
    minnow = TenantLimits(rate=10.0, burst=10)
    limits = {"whale": whale, **{f"t{i}": minnow for i in range(499)}}
    registry = BucketRegistry(limits, default=minnow, clock=clock)

    for _ in range(1000):
        assert registry.acquire("whale").allowed is True
    assert registry.acquire("whale").allowed is False  # the whale is over its own limit

    for i in range(499):
        tenant = f"t{i}"
        assert [registry.acquire(tenant).allowed for _ in range(11)] == [True] * 10 + [False]
        assert registry.bucket_for(tenant).consumed == 10

    assert registry.summary().rates["whale"] == 1000.0
    assert registry.summary().rates["t0"] == 10.0


# --- Tenant isolation --------------------------------------------------------


def test_tenant_x_over_its_limit_cannot_deny_tenant_y():
    clock = FakeClock()
    registry = BucketRegistry(
        {"x": TenantLimits(rate=1.0, burst=5), "y": TenantLimits(rate=1.0, burst=5)},
        default=TenantLimits(rate=1.0, burst=1),
        clock=clock,
    )
    for _ in range(5):
        assert registry.acquire("x").allowed is True
    for _ in range(10_000):
        assert registry.acquire("x").allowed is False
    assert (registry.bucket_for("x").consumed, registry.bucket_for("x").denied) == (5, 10_000)
    assert [registry.acquire("y").allowed for _ in range(6)] == [True] * 5 + [False]
    assert (registry.bucket_for("y").consumed, registry.bucket_for("y").denied) == (5, 1)


def test_interleaved_denials_never_spend_another_tenants_budget():
    clock = FakeClock()
    registry = BucketRegistry(
        {"a": TenantLimits(rate=1.0, burst=3), "b": TenantLimits(rate=1.0, burst=3)},
        default=TenantLimits(rate=1.0, burst=1),
        clock=clock,
    )
    for _ in range(100):
        clock.advance(3.0)  # both tenants refill to full
        for _ in range(3):
            assert registry.acquire("a").allowed is True
        assert registry.acquire("a").allowed is False  # a is dry
        # b still holds its own full budget: a's denial cost it nothing.
        assert [registry.acquire("b").allowed for _ in range(4)] == [True, True, True, False]
    assert registry.bucket_for("a").consumed == 300
    assert registry.bucket_for("a").denied == 100
    assert registry.bucket_for("b").consumed == 300
    assert registry.bucket_for("b").denied == 100


# --- The sticky-routing invariant, at the level this module can observe -------


def _drive(registry: BucketRegistry, tenant: str, *, steps: int, per_step: int, dt: float, clock: FakeClock) -> int:
    allowed = 0
    for _ in range(steps):
        allowed += sum(registry.acquire(tenant).allowed for _ in range(per_step))
        clock.advance(dt)
    return allowed


def test_one_user_has_one_bucket_so_one_tenant_has_one_budget_per_process():
    """D12 in miniature: sticky routing sends every event of a `(tenant, user)`
    pair to one worker, so the pair's traffic hits one bucket and one budget."""
    registry = BucketRegistry(
        {"acme": TenantLimits(rate=1.0, burst=4)}, default=TenantLimits(rate=1.0, burst=1), clock=FakeClock()
    )
    acme_users = [f"usr_{i}" for i in range(1000)]
    allowed = sum(registry.acquire("acme").allowed for _ in acme_users)
    assert allowed == 4  # 1000 users' events, one 4-token budget
    assert registry.bucket_for("acme").consumed == 4
    assert registry.summary().tenant_count == 1


def test_sticky_routing_keeps_the_enforced_rate_within_1_2x_of_configured():
    """THE H1 regression test, at this module's level.

    Six independent worker processes (N >= 6 per D12) each enforcing the
    configured rate would give a tenant 6x the rate it was configured with.
    Sticky routing means a tenant's traffic has exactly one owner, so the
    aggregate is the configured rate.
    """
    rate, burst = 10.0, 10
    steps, per_step, dt = 100, 100, 0.1  # 10s of offered load at 1000 events/s
    window = steps * dt

    clock = FakeClock()
    sticky = BucketRegistry(
        {"acme": TenantLimits(rate=rate, burst=burst)},
        default=TenantLimits(rate=rate, burst=burst),
        clock=clock,
    )
    allowed = _drive(sticky, "acme", steps=steps, per_step=per_step, dt=dt, clock=clock)
    measured = allowed / window
    # Burst is slack on top of the sustained rate, hence 1.0x <= measured.
    assert rate <= measured <= 1.2 * rate
    assert sticky.summary().rates["acme"] == rate


def test_without_sticky_routing_the_aggregate_rate_is_n_times_configured():
    """The failure mode H1 predicted, pinned so the fix cannot silently lapse."""
    n_workers, rate, burst = 6, 10.0, 10
    steps, per_step, dt = 100, 100, 0.1
    window = steps * dt
    clock = FakeClock()

    workers = [
        BucketRegistry(
            {"acme": TenantLimits(rate=rate, burst=burst)},
            default=TenantLimits(rate=rate, burst=burst),
            clock=clock,
        )
        for _ in range(n_workers)
    ]
    total = 0
    for _ in range(steps):
        for w in workers:  # round-robin: a user's events spread over all workers
            total += sum(w.acquire("acme").allowed for _ in range(per_step // n_workers))
        clock.advance(dt)

    assert total / window >= n_workers * rate  # N x the configured rate
    for w in workers:  # each worker is individually correct ...
        assert w.summary().rates["acme"] == rate
        assert w.bucket_for("acme").consumed <= 1.2 * rate * window
    # ... which is exactly why only the *routing* decides whether the tenant's
    # real rate is the configured one.


def test_retry_after_never_asks_a_client_to_wait_longer_than_a_token_takes():
    clock = FakeClock()
    bucket = TokenBucket(rate=4.0, burst=4, clock=clock)  # one token per 0.25s
    for _ in range(4):
        bucket.acquire()
    for _ in range(20):
        bucket.acquire()  # spends the token accrued since the previous step
        denied = bucket.acquire()
        assert denied.allowed is False
        # One token costs 0.25s, so the honest wait is 1s; never more than it
        # would take to refill the whole bucket.
        assert 1 <= denied.retry_after <= math.ceil(bucket.burst / bucket.rate)
        clock.advance(0.25)
