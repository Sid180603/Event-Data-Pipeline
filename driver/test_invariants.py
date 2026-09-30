"""A subclass must not silently break the base class's internals.

T8c found that `CorpusBuilder._outcome` took no RNG while `SkewedCorpus._outcome`
took one. That is an LSP violation across the subclass boundary: it happened to
work because `sessions()` was overridden in lockstep, and it would have broken
the next subclass that overrode one and not the other — with no error, just
wrong traffic. These pin the contract so that failure is a test failure.
"""

from __future__ import annotations

import inspect
import random

from driver.corpus import CorpusBuilder
from driver.inject import plan_injection
from driver.skew import SkewedCorpus


def test_the_subclass_does_not_reshape_the_base_signature():
    """`_outcome` must take the same parameters in the subclass as in the base."""
    base = inspect.signature(CorpusBuilder._outcome)
    sub = inspect.signature(SkewedCorpus._outcome)
    assert list(base.parameters) == list(sub.parameters), (
        f"SkewedCorpus._outcome{sub} does not match CorpusBuilder._outcome{base}; "
        "an override that changes the signature is a silent LSP violation"
    )


def test_the_base_takes_the_rng_rather_than_reading_instance_state():
    """A generator-per-call RNG is what makes a two-pass count-then-generate safe."""
    params = list(inspect.signature(CorpusBuilder._outcome).parameters)
    assert "rng" in params, (
        "_outcome must accept an rng parameter; reading self.rng makes a second "
        "pass over the same instance emit a different number of events"
    )


def test_two_passes_over_one_instance_still_differ_and_that_is_known():
    """Documenting the footgun rather than pretending it away.

    `CorpusBuilder` advances a single RNG on the instance, so asking the same
    object for events twice is not a re-read -- it is a continuation. T8c's
    `batches()` compensates by refusing a run whose size does not match its
    plan. This test records the underlying behaviour so that compensation stays
    deliberate rather than looking like a bug.
    """
    builder = CorpusBuilder(career_site_ids=["t1", "t2"], users_per_tenant=5)
    first = sum(len(events) for _t, events in builder.sessions(20))
    second = sum(len(events) for _t, events in builder.sessions(20))
    assert first != second, (
        "CorpusBuilder is now deterministic across calls, so the injection "
        "planner's size check can be revisited"
    )


def test_batches_refuses_a_run_whose_size_does_not_match_the_plan():
    """T8c's compensation for the two-pass trap: a size mismatch is an error,
    not a silently shortened injection run."""
    plan = plan_injection(total_events=2000, rate=0.05, seed=1)
    assert len(plan.slots) == 100, f"expected exactly 100 injections, got {len(plan.slots)}"
    # A plain run with no registered plan must be unaffected.
    builder = CorpusBuilder(career_site_ids=["t1"], users_per_tenant=5)
    assert list(builder.batches(10))


def test_outcome_is_reproducible_for_a_given_rng_seed():
    builder = CorpusBuilder(career_site_ids=["t1"], drop_off_rate=0.3)
    a = builder._outcome(random.Random(7))
    b = builder._outcome(random.Random(7))
    assert a == b


def test_the_subclass_still_produces_its_own_distribution():
    """A shared signature must not mean shared behaviour."""
    skew = SkewedCorpus(seed=3, tenant_count=2, users_per_tenant=5, drop_off_rate=0.5)
    outcomes = {skew._outcome(random.Random(i)) for i in range(40)}
    assert len(outcomes) > 1, "the override is being ignored entirely"
