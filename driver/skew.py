"""T8b: the Zipfian tenant skew.

SPEC.txt:148 -- "Large enterprise tenants generate exponentially higher traffic
during hiring drives than SMBs." A driver that spreads sessions evenly across
tenants cannot exercise the two failures the architecture is built around: a hot
partition on the whale, and rate starvation on the tail. So tenant volume is
apportioned from a Zipf law with a configurable exponent, and the whale's
traffic is deliberately left ugly enough to find the hot-partition bugs.

Two deliberate properties:

* **Apportionment, not a random draw.** Sessions are handed out by
  largest-remainder, so the realised distribution IS the requested power law
  rather than a noisy sample of one. A run is reproducible and a regression in
  the model shows up as a test failure instead of as variance.
* **User counts scale with tenant volume.** Kafka keys on
  `career_site_id|user_id_pseudo` and the gateway is sticky, so one user is one
  partition and one worker for the entire run. A whale with a fixed user count
  concentrates a quarter of the corpus onto a few partitions. The cap is
  enforced here, in the model, and `MAX_EVENTS_PER_USER_PER_RUN` is the number it
  is enforced against -- see the test that asserts the whale really would blow
  through it without the scaling.
"""

from __future__ import annotations

import hashlib
import math
import random
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from driver.corpus import SOURCE_CHANNELS, CorpusBuilder
from driver.fsm import ABANDONED, DRAFT_SAVED, SUBMITTED, SessionConfig, generate_session
from driver.tenants import DEFAULT_TENANT_COUNT, DEFAULT_USERS_PER_TENANT, TenantCatalog
from driver.webhook_source import THIRD_PARTY_CHANNEL, webhook_events, webhook_payload

#: s=1.0 is textbook Zipf (rank 1 holds 14.7% of a 500-tenant catalog). Real
#: career traffic is steeper than that -- a handful of enterprises run hiring
#: drives -- so the default is 1.2: the top 1% of tenants take ~49% of the
#: volume, the top 5% take ~72%, and rank 1 out-sources the median tenant ~750:1.
DEFAULT_SKEW_EXPONENT = 1.2

#: A session is viewed + wishlisted + started + up to 4 steps + terminal. The
#: upper bound is what the per-user cap is divided by.
MAX_EVENTS_PER_SESSION = 8

#: A reference run is 1M sessions (~5M events) replayed at 50k events/sec across
#: 8 workers, so one worker retires ~6.25k events/sec for ~100s. A single
#: sticky-routed user owning more than this many events is asking one worker to
#: absorb a visible slice of the whole run on its own -- the point where the
#: whale stops being a partition-skew curiosity and becomes a latency spike.
#: Exceeding it is a mis-parameterised model, not a bug in the driver.
MAX_EVENTS_PER_USER_PER_RUN = 5_000

#: Sessions per user, from the two bounds above.
MAX_SESSIONS_PER_USER = MAX_EVENTS_PER_USER_PER_RUN // MAX_EVENTS_PER_SESSION


# --- the distribution --------------------------------------------------------


def zipf_shares(n: int, exponent: float = DEFAULT_SKEW_EXPONENT) -> list[float]:
    """Normalised Zipf shares for ranks 1..n: weight(rank) = 1 / rank**exponent."""
    if n < 1:
        raise ValueError(f"n must be positive, got {n}")
    if exponent <= 0:
        raise ValueError(f"exponent must be positive, got {exponent}")
    weights = [1.0 / (rank**exponent) for rank in range(1, n + 1)]
    total = math.fsum(weights)
    return [w / total for w in weights]


def apportion(total: int, weights: list[float]) -> list[int]:
    """Split `total` units across `weights` by largest remainder.

    Every unit is placed and none are invented, so the tail receives a true zero
    rather than a rounded-up one: with a steep exponent most of a 500-tenant
    catalog gets no sessions at all in a short run, which is the point.
    """
    if total < 0:
        raise ValueError(f"total must be non-negative, got {total}")
    weight_sum = math.fsum(weights)
    if weight_sum <= 0:
        return [0] * len(weights)
    exact = [total * w / weight_sum for w in weights]
    counts = [int(x) for x in exact]
    remainder = total - sum(counts)
    for i in sorted(range(len(weights)), key=lambda i: (-(exact[i] - counts[i]), i))[:remainder]:
        counts[i] += 1
    return counts


@dataclass(frozen=True, slots=True)
class SkewReport:
    """How concentrated a volume distribution is, in the terms a reviewer asks for."""

    top_1pct_tenants: int
    top_1pct_share: float
    top_5pct_share: float
    busiest: float
    median: float
    thinnest: float

    @property
    def head_over_median(self) -> float:
        return self.busiest / self.median

    @property
    def head_over_tail(self) -> float:
        return self.busiest / self.thinnest


def skew_report(shares: list[float]) -> SkewReport:
    n = len(shares)
    ordered = sorted(shares, reverse=True)
    median = sorted(shares)[n // 2]
    one_pct = max(1, round(n * 0.01))
    five_pct = max(one_pct, round(n * 0.05))
    return SkewReport(
        top_1pct_tenants=one_pct,
        top_1pct_share=sum(ordered[:one_pct]),
        top_5pct_share=sum(ordered[:five_pct]),
        busiest=ordered[0],
        median=median,
        thinnest=ordered[-1],
    )


# --- the volume plan ---------------------------------------------------------


@dataclass(frozen=True, slots=True)
class VolumePlan:
    """Which tenant gets how many sessions, and who carries them.

    `users_by_tenant` is volume-scaled: a tenant needing more than
    `MAX_SESSIONS_PER_USER` sessions per user is given more users, because under
    sticky routing the alternative is a partition hot enough to distort the run.
    """

    sessions_by_tenant: dict[str, int]
    users_by_tenant: dict[str, list[str]]

    @property
    def max_sessions_per_user(self) -> int:
        return max(
            (math.ceil(s / len(self.users_by_tenant[t])) for t, s in self.sessions_by_tenant.items() if s),
            default=0,
        )

    @property
    def max_events_per_user(self) -> int:
        """Upper bound on what one sticky-routed user can be handed in a run."""
        return self.max_sessions_per_user * MAX_EVENTS_PER_SESSION


def plan_sessions(
    catalog: TenantCatalog,
    total_sessions: int,
    *,
    exponent: float = DEFAULT_SKEW_EXPONENT,
    users_per_tenant: int = DEFAULT_USERS_PER_TENANT,
) -> VolumePlan:
    """The whole catalog's session budget, apportioned by Zipf rank."""
    counts = apportion(total_sessions, zipf_shares(len(catalog.ids), exponent))
    sessions_by_tenant: dict[str, int] = {}
    users_by_tenant: dict[str, list[str]] = {}
    for tenant, sessions in zip(catalog.ids, counts, strict=True):
        if sessions == 0:
            continue
        needed = max(users_per_tenant, math.ceil(sessions / MAX_SESSIONS_PER_USER))
        sessions_by_tenant[tenant] = sessions
        users_by_tenant[tenant] = catalog.users(tenant, needed)
    return VolumePlan(sessions_by_tenant, users_by_tenant)


def busiest_tenant(plan: VolumePlan) -> tuple[str, int]:
    tenant = max(plan.sessions_by_tenant, key=plan.sessions_by_tenant.__getitem__)
    return tenant, plan.sessions_by_tenant[tenant]


# --- measurement -------------------------------------------------------------


def max_events_per_user(batches: Iterable[list[dict]]) -> int:
    """The largest share of a run handed to one (tenant, user) pair.

    The pair, not the user alone: `user_id_pseudo` is an HMAC scoped to a tenant,
    so the same human applying to two career sites is two keys and two routes.
    """
    totals: dict[tuple[str, str], int] = {}
    for batch in batches:
        for ev in batch:
            key = (ev["source"], ev["data"]["candidate"]["user_id"])
            totals[key] = totals.get(key, 0) + 1
    return max(totals.values(), default=0)


def structural_fingerprint(events: Iterable[dict]) -> str:
    """A content hash of the traffic's SHAPE, ignoring the generated ids.

    `id`, `session_id` and `email_hmac` are uuid4, so a regenerated corpus can
    never be byte-identical -- real unique ids are the point. Everything a load
    run's behaviour depends on is here: tenant, type, ordering, channel and
    attribution. Sorted per tenant so that two independently generated shards
    fingerprint the same as the single run they reconstruct.
    """
    per_source: dict[str, Counter[tuple[str, str, str, str | None]]] = {}
    for ev in events:
        per_source.setdefault(ev["source"], Counter())[
            (ev["type"], ev["sequence"], ev["sourcechannel"], ev.get("referrertype"))
        ] += 1
    digest = hashlib.blake2b(digest_size=16)
    for source in sorted(per_source):
        digest.update(f"{source}|{sorted(per_source[source].items())}".encode())
    return digest.hexdigest()


# --- the corpus --------------------------------------------------------------


class SkewedCorpus(CorpusBuilder):
    """`CorpusBuilder` with a Zipfian tenant mix, shardable and volume-scaled.

    Subclasses rather than reimplements, so `batches()` and `build()` -- the
    single-tenant batching and the ledger receipt the gateway's acceptance tests
    depend on -- are the same code T8a shipped. Only the session mix changes.

    Every shard must be given the same `total_sessions`, `seed` and `exponent`:
    each one apportions the *global* plan and keeps only its own tenants, so the
    shards partition the run exactly instead of each running a whole one.
    """

    def __init__(
        self,
        *,
        catalog: TenantCatalog | None = None,
        seed: int = 7,
        exponent: float = DEFAULT_SKEW_EXPONENT,
        tenant_count: int = DEFAULT_TENANT_COUNT,
        users_per_tenant: int = DEFAULT_USERS_PER_TENANT,
        drop_off_rate: float = 0.25,
        shard: int | None = None,
        shards: int = 1,
    ) -> None:
        self.catalog = catalog if catalog is not None else TenantCatalog(tenant_count, seed=seed)
        self.seed = seed
        self.exponent = exponent
        self.drop_off_rate = drop_off_rate
        self.shard = shard
        self.shards = shards
        self._plan: VolumePlan | None = None
        self._plan_total: int | None = None
        own = (
            self.catalog.ids
            if shard is None
            else self.catalog.shard_ids(shard, shards)
        )
        super().__init__(
            career_site_ids=own,
            seed=seed,
            users_per_tenant=users_per_tenant,
            drop_off_rate=drop_off_rate,
        )

    def plan_for(self, total_sessions: int) -> VolumePlan:
        """The global plan, narrowed to this shard's tenants."""
        if self._plan_total != total_sessions or self._plan is None:
            self._plan = plan_sessions(
                self.catalog,
                total_sessions,
                exponent=self.exponent,
                users_per_tenant=self.users_per_tenant,
            )
            self._plan_total = total_sessions
        mine = set(self.career_site_ids)
        return VolumePlan(
            {t: n for t, n in self._plan.sessions_by_tenant.items() if t in mine},
            {t: u for t, u in self._plan.users_by_tenant.items() if t in mine},
        )

    def _outcome(self, rng: random.Random) -> str:
        r = rng.random()
        if r < self.drop_off_rate:
            return ABANDONED
        if r < self.drop_off_rate + 0.15:
            return DRAFT_SAVED
        return SUBMITTED

    def sessions(self, total_sessions: int) -> Iterator[tuple[str, list[dict]]]:
        """Sessions in tenant-rank order, sized by the skew.

        Every per-session draw is seeded from (seed, tenant, index) and never
        from a shared stream, so a shard's content depends only on its own
        tenants. Interleaving the tenants differently would otherwise change
        what each shard emits.
        """
        plan = self.plan_for(total_sessions)
        for tenant in self.catalog.ids:
            count = plan.sessions_by_tenant.get(tenant, 0)
            if count == 0:
                continue
            users = plan.users_by_tenant[tenant]
            order = list(users)
            random.Random(f"{self.seed}:{tenant}:order").shuffle(order)
            rng = random.Random(f"{self.seed}:{tenant}:session")
            for i in range(count):
                user = order[i % len(order)]
                job_id = f"job_{rng.randrange(10_000, 99_999)}"
                channel = rng.choice(SOURCE_CHANNELS)
                start = self._next_sequence(tenant, user)
                session_rng = random.Random(f"{self.seed}:{tenant}:{i}")
                if channel == THIRD_PARTY_CHANNEL:
                    events = webhook_events(
                        webhook_payload(session_rng, tenant, job_id),
                        user_id_pseudo=user,
                        start_sequence=start,
                        rng=session_rng,
                    )
                else:
                    events = generate_session(
                        SessionConfig(
                            career_site_id=tenant,
                            user_pseudo=user,
                            source_channel=channel,
                            job_id=job_id,
                            steps=rng.randint(1, 4),
                            outcome=self._outcome(rng),
                            start_sequence=start,
                            referrer=rng.choice(["SEARCH", "RECOMMENDATION", "DIRECT"]),
                            rng=session_rng,
                        )
                    )
                # Reserve the range the session consumed, not just its start, or
                # the next session for this user reissues sequence numbers.
                self._seq[(tenant, user)] = int(events[-1]["sequence"]) + 1
                yield tenant, events


__all__ = [
    "DEFAULT_SKEW_EXPONENT",
    "MAX_EVENTS_PER_SESSION",
    "MAX_EVENTS_PER_USER_PER_RUN",
    "MAX_SESSIONS_PER_USER",
    "SkewReport",
    "SkewedCorpus",
    "VolumePlan",
    "apportion",
    "busiest_tenant",
    "max_events_per_user",
    "plan_sessions",
    "skew_report",
    "structural_fingerprint",
    "zipf_shares",
]
