"""T8c: fault injection.

Two demo beats cannot be performed without this module, and neither can be faked
by hand at the console:

* **"inject 5% garbage -> the DLQ catches exactly 5%"**. The demo compares the
  DLQ depth against what the driver says it sent, so the injected fraction has to
  be an exact integer. A Bernoulli draw at 5% misses by hundreds over a 5M-event
  run, and "roughly 5%" is not an assertion. So the bad events are *placed*, not
  drawn: `plan_injection` computes the exact count and a stratified set of
  positions, and every run with the same seed injects at the same places.
* **"one tenant floods -> it gets shed, the other 499 are unaffected"**.
  `driver/skew.py` deliberately spreads volume out across a Zipf law, and
  `app.ratelimit` budgets each tenant on its own bucket, so a flood has to be a
  separate mode over the same corpus rather than a parameter of it.

The variants are the gateway's own reason CODES, imported from
`app.validate.events` rather than retyped, so a rename there breaks this module
loudly instead of producing a variant the DLQ never reports. `test_inject.py`
drives each one through the gateway's real decoder and `validate_batch` and
asserts the code comes back.

Neither mode is the default. A clean run emits exactly what the corpus emitted:
the injector is a pass-through at rate 0, and the flood is a distinct object the
caller has to ask for.
"""

from __future__ import annotations

import argparse
import bisect
import copy
import json
import math
import random
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from app.validate.events import (
    CODE_BAD_TIME,
    CODE_DUPLICATE_ID,
    CODE_SCHEMA,
    CODE_UNKNOWN_ATTR,
)
from contracts.ledger import Ledger, LedgerRecord
from driver.corpus import SOURCE_CHANNELS
from driver.fsm import SUBMITTED, SessionConfig, generate_session
from driver.skew import SkewedCorpus
from driver.tenants import TenantCatalog

#: The injection variants, named for the gateway CODES they provoke. The name IS
#: the code, so `report.by_variant` is directly comparable to a DLQ depth
#: breakdown with no mapping table in between to get wrong.
VARIANT_CODES: tuple[str, ...] = (CODE_SCHEMA, CODE_BAD_TIME, CODE_UNKNOWN_ATTR, CODE_DUPLICATE_ID)

#: A `type` outside the published enum, so `contracts.cloudevent.EventType`
#: refuses it and the gateway reports SCHEMA at `$.type`.
ILLEGAL_TYPE = "com.careerpage.career.job-clicked"
#: Not RFC 3339 (`contracts.attributes._RFC3339_RE`), so `envelope_problem`
#: reports BAD_TIME.
ILLEGAL_TIME = "30/09/2026 14:43:09"
#: An underscore, which CloudEvents context-attribute names may not carry
#: (`contracts.attributes._ATTR_RE`), and a plausible one to get wrong: a client
#: re-sending the tenant it was already bound to.
ILLEGAL_ATTRIBUTE = "career_site_id"

#: A flood spreads over a few dozen of its own users rather than pinning one
#: partition. A flood is meant to be SHED by the per-tenant limiter, and it is
#: also meant to be honest traffic, not 5,000 events on one sticky route.
DEFAULT_FLOOD_USERS = 64
DEFAULT_FLOOD_SESSIONS = 5_000


# --- the plan -----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Slot:
    """One malformed event: a global event index and the CODE it will draw."""

    index: int
    variant: str


@dataclass(frozen=True, slots=True)
class InjectionPlan:
    """Where the malformed events go. Pure arithmetic on (total, rate, seed)."""

    total_events: int
    rate: float
    seed: int
    slots: tuple[Slot, ...]

    @property
    def count(self) -> int:
        return len(self.slots)

    def variant_at(self, index: int) -> str | None:
        """The variant to inject at global event `index`, or None for a clean one."""
        position = bisect.bisect_left(self.slots, index, key=lambda slot: slot.index)
        if position < len(self.slots) and self.slots[position].index == index:
            return self.slots[position].variant
        return None


def plan_injection(
    *,
    total_events: int,
    rate: float,
    seed: int = 7,
    variants: Sequence[str] = VARIANT_CODES,
) -> InjectionPlan:
    """Exactly `round(total_events * rate)` malformed events, evenly spread.

    The count is arithmetic rather than a sample: the demo's headline is "the DLQ
    caught exactly the number we sent", and that assertion only survives if the
    number the driver reports is the number the driver injected.

    The positions are stratified, not drawn: the run is cut into `count` equal
    buckets and one event is injected somewhere inside each. A random draw clumps
    -- and a clump is the failure this mode exists to rule out, because the bad
    events landing in one batch would exercise the gateway's per-event verdict
    exactly once. Stratifying also puts a floor under the gap between two
    injections (two buckets wide, so two bad events are never adjacent), which a
    random draw cannot promise. The offset within a bucket is seeded, so the same
    seed injects at the same indices twice and two seeds do not coincide.

    Variants are dealt round-robin, so every variant in `variants` is exercised
    whenever the count allows and no variant is left with a count to explain.
    """
    if total_events < 0:
        raise ValueError(f"total_events must be non-negative, got {total_events}")
    if not 0.0 <= rate <= 1.0:
        raise ValueError(f"rate must be between 0 and 1, got {rate}")
    variants = tuple(variants)
    if not variants:
        raise ValueError("at least one variant is required")

    count = min(math.floor(total_events * rate + 0.5), total_events)
    if count == 0:
        return InjectionPlan(total_events, rate, seed, ())

    bucket = total_events / count
    rng = random.Random(f"inject:{seed}")
    slots = []
    for k in range(count):
        # `bucket - 2` keeps the next injection at least two events away, so a
        # small batch can never collect two of them back to back. A bucket
        # narrower than that has no room to jitter in, and at such a rate the
        # injections are adjacent anyway.
        slack = int(bucket) - 2
        index = k * bucket + (rng.randrange(slack + 1) if slack >= 0 else 0)
        slots.append(Slot(index=int(index), variant=variants[k % len(variants)]))
    return InjectionPlan(total_events, rate, seed, tuple(slots))


# --- the corruption -----------------------------------------------------------


def corrupt(event: dict, variant: str, *, partner_id: str | None = None) -> dict:
    """A copy of `event` that the gateway rejects with `variant`'s code.

    A copy, and one field at a time: the event keeps its tenant, its sequence and
    the rest of its envelope, so a demo can point at a malformed event and see
    that the *only* thing wrong with it is the thing that was injected.

    `partner_id` is required for DUPLICATE_ID, because a duplicate is a relation
    to another event rather than a property of this one.
    """
    broken = copy.deepcopy(event)
    if variant == CODE_SCHEMA:
        broken["type"] = ILLEGAL_TYPE
    elif variant == CODE_BAD_TIME:
        broken["time"] = ILLEGAL_TIME
    elif variant == CODE_UNKNOWN_ATTR:
        broken[ILLEGAL_ATTRIBUTE] = broken["source"].rsplit("/", 1)[-1]
    elif variant == CODE_DUPLICATE_ID:
        if not partner_id:
            raise ValueError("a DUPLICATE_ID event needs a partner id in its batch")
        broken["id"] = partner_id
    else:
        raise ValueError(f"unknown variant {variant!r}")
    return broken


# --- the report ---------------------------------------------------------------


@dataclass(slots=True)
class InjectionReport:
    """What the run emitted, and how much of it was malformed on purpose.

    `by_variant` is keyed by the gateway CODE, so the demo asserts
    `dlq_depth[code] == report.by_variant[code]` with nothing to translate.

    `emitted` counts what went out, which can exceed `total_events` by `appended`
    -- see `MalformedInjector._batch`. The malformed count is unaffected by that,
    so the assertion the demo makes still holds exactly.
    """

    batches: int = 0
    emitted: int = 0
    injected: int = 0
    appended: int = 0
    by_variant: dict[str, int] = field(default_factory=dict)

    @property
    def rate(self) -> float:
        return self.injected / self.emitted if self.emitted else 0.0

    def to_dict(self) -> dict:
        return {
            "batches": self.batches,
            "emitted": self.emitted,
            "injected": self.injected,
            "appended": self.appended,
            "rate": round(self.rate, 6),
            "by_variant": dict(sorted(self.by_variant.items())),
        }


# --- the injector -------------------------------------------------------------


class MalformedInjector:
    """A pass-through over a corpus that corrupts an exact fraction of it.

    Usage is count-then-inject, because "exactly" needs the total before the first
    event is written:

        total = count_events(fresh_corpus().batches(n))
        report = MalformedInjector(rate=0.05).build(
            fresh_corpus().batches(n), ledger_path, total_events=total
        )

    Two corpora, not one, and that is load-bearing: a corpus generator advances a
    `random.Random` held on the instance, so asking the SAME instance for its
    events twice emits a different number the second time. The count has to come
    from a corpus built identically from the same seed, and `batches` refuses to
    hand back a run that turns out not to be the size the plan was built for.
    """

    def __init__(
        self,
        *,
        rate: float = 0.0,
        seed: int = 7,
        variants: Sequence[str] = VARIANT_CODES,
    ) -> None:
        self.rate = rate
        self.seed = seed
        self.variants = tuple(variants)
        self.report = InjectionReport()

    @property
    def enabled(self) -> bool:
        return self.rate > 0

    def plan(self, total_events: int) -> InjectionPlan:
        if not self.enabled:
            return InjectionPlan(total_events, 0.0, self.seed, ())
        return plan_injection(
            total_events=total_events, rate=self.rate, seed=self.seed, variants=self.variants
        )

    def batches(
        self, batches: Iterable[list[dict]], *, total_events: int | None = None
    ) -> Iterator[list[dict]]:
        """The same batches, with the plan's events corrupted. One pass, no buffering."""
        if self.enabled and total_events is None:
            raise ValueError(
                "an exact injection count needs the corpus size: pass total_events=count_events(batches)"
            )
        plan = self.plan(total_events or 0)
        index = 0
        for batch in batches:
            self.report.batches += 1
            emitted = self._batch(batch, plan, index)
            self.report.emitted += len(emitted)
            index += len(batch)
            yield emitted
        # The two invariants the demo rests on, checked rather than assumed. A
        # corpus that is not the size the plan was built for loses injections off
        # the end of the stream silently, which is a "roughly 5%" that reads
        # exactly like an exact one in the DLQ.
        if total_events is not None and (
            index != total_events or self.report.injected != plan.count
        ):
            raise ValueError(
                f"planned {plan.count} malformed events in {total_events} and injected "
                f"{self.report.injected} in {index}: the corpus handed over is "
                "not the one that was counted (see count_events)"
            )

    def build(
        self,
        batches: Iterable[list[dict]],
        ledger_path: Path,
        *,
        total_events: int | None = None,
    ) -> InjectionReport:
        """Write the ledger, malformed events included, and return the report.

        Every emitted event is recorded, so the ground truth distinguishes "the
        gateway rejected this on purpose" from "this never arrived". The ledger
        schema does not change; only what lands in it does.
        """
        ledger_path = Path(ledger_path)
        ledger_path.unlink(missing_ok=True)  # Ledger appends; a stale run would accumulate
        with Ledger(ledger_path) as ledger:
            for batch in self.batches(batches, total_events=total_events):
                for ev in batch:
                    ledger.append(
                        LedgerRecord(
                            id=ev["id"],
                            source=ev["source"],
                            type=ev["type"],
                            tenant=ev["source"].rsplit("/", 1)[-1],
                            user_pseudo=ev["data"]["candidate"]["user_id"],
                            seq=int(ev["sequence"]),
                        )
                    )
        return self.report

    def _batch(self, batch: list[dict], plan: InjectionPlan, offset: int) -> list[dict]:
        """One batch, with the plan's events in it corrupted.

        The whole batch is in view because DUPLICATE_ID is a relation to another
        event and not a property of this one: the partner has to be a clean event
        in the SAME batch, or the gateway's per-batch pass would never see the
        collision and the demo would count a rejection that does not happen.

        A one-event batch leaves no room for that, and a corpus's final flush can
        be exactly one event. Rather than drop the injection -- which would make
        the malformed count a guess again -- the clean partner is appended to the
        batch. The run then emits one extra CLEAN event and still exactly the
        planned number of malformed ones, so `accepted + dlq == emitted` and
        `dlq == injected` both keep holding. `report.appended` counts it.
        """
        corrupted = list(batch)
        for i, event in enumerate(batch):
            variant = plan.variant_at(offset + i)
            if variant is None:
                continue
            self.report.injected += 1
            self.report.by_variant[variant] = self.report.by_variant.get(variant, 0) + 1
            partner = _partner(batch, plan, offset, i)
            if partner is None:
                corrupted.append(event)
                self.report.appended += 1
            corrupted[i] = corrupt(
                event, variant, partner_id=event["id"] if partner is None else partner["id"]
            )
        return corrupted


def _partner(batch: list[dict], plan: InjectionPlan, offset: int, index: int) -> dict | None:
    """A clean event in the same batch for a DUPLICATE_ID injection to collide with.

    Backwards first, then forwards. Backwards because the gateway keeps the
    FIRST of a repeated (source, id) and rejects the later one, so pairing with
    an earlier event means the corrupted one is the thing that gets rejected --
    which is the event the ledger recorded as injected. None means every event in
    this batch is itself an injection, which takes a rate near 100%; `_batch`
    handles that by appending the partner rather than inventing an id.
    """
    for j in [*range(index - 1, -1, -1), *range(index + 1, len(batch))]:
        if plan.variant_at(offset + j) is None:
            return batch[j]
    return None


# --- counting -----------------------------------------------------------------


def count_events(batches: Iterable[list[dict]]) -> int:
    """How many events a batch stream will emit. One pass, nothing buffered.

    Pass a stream from a FRESH corpus, not a second call to the same generator.
    `CorpusBuilder` and `SkewedCorpus` draw from a `random.Random` held on the
    instance, so a second call continues that stream and emits a different number
    of events -- a 672-event corpus becomes 668 on its second pass, and a plan
    built against the first number quietly loses its last injections. Two
    instances built from the same seed agree, which is what makes counting one
    run and generating another sound.
    """
    return sum(len(batch) for batch in batches)


# --- the single-tenant flood --------------------------------------------------


def flood_user_id(career_site_id: str, index: int) -> str:
    """A user id for the flood, in a namespace the baseline corpus never mints.

    The flood has to have its own sequence space. `sequence` is a per-user
    counter, and the Queue team's sessionizer orders on it: if a flooding user
    shared an id with a baseline user, the two would interleave two half-journeys
    under one counter and the demo's ordering claim would be false without
    anything failing. `driver.corpus.derive_users` mints eight hex characters, so
    a namespace ending in a non-hex character cannot collide with one by
    construction rather than by luck.
    """
    return f"{career_site_id}:flood{index:04d}"


class TenantFlood:
    """A high-rate stream for ONE tenant. Every other tenant stays at baseline.

    Separate from `SkewedCorpus` on purpose. The corpus models a normal run, where
    volume follows a Zipf law across 500 tenants; a flood models a hiring drive
    concentrating on one. Overlaying the two -- raising a tenant's share inside
    the Zipf model -- would perturb the apportionment every other tenant's volume
    was derived from, so the "the other 499 are unaffected" half of the claim
    would no longer be true. Here the flood is a separate stream added alongside
    an untouched corpus, which is what makes that half checkable: the other
    tenants' event counts are identical with and without it.
    """

    def __init__(
        self,
        career_site_id: str,
        *,
        sessions: int = DEFAULT_FLOOD_SESSIONS,
        users: int = DEFAULT_FLOOD_USERS,
        seed: int = 7,
    ) -> None:
        if not career_site_id:
            raise ValueError("career_site_id is required")
        if sessions < 0:
            raise ValueError(f"sessions must be non-negative, got {sessions}")
        if users < 1:
            raise ValueError(f"users must be positive, got {users}")
        self.career_site_id = career_site_id
        self.sessions = sessions
        self.seed = seed
        self.users = [flood_user_id(career_site_id, i) for i in range(users)]

    def events(self) -> Iterator[list[dict]]:
        """Sessions for this tenant only, each contiguous in `sequence`.

        One counter per flooding user, advanced by the whole range a session
        consumed so the next session cannot reissue a number.
        """
        rng = random.Random(f"{self.seed}:{self.career_site_id}:flood")
        seq: dict[str, int] = {user: 0 for user in self.users}
        for i in range(self.sessions):
            user = self.users[i % len(self.users)]
            events = generate_session(
                SessionConfig(
                    career_site_id=self.career_site_id,
                    user_pseudo=user,
                    source_channel=rng.choice(SOURCE_CHANNELS),
                    job_id=f"job_{rng.randrange(10_000, 99_999)}",
                    # A hiring drive is short applications, and short means a
                    # high event rate per session -- which is the point.
                    steps=1,
                    outcome=SUBMITTED,
                    start_sequence=seq[user],
                    referrer=rng.choice(["SEARCH", "RECOMMENDATION", "DIRECT"]),
                    rng=random.Random(f"{self.seed}:{self.career_site_id}:flood:{i}"),
                )
            )
            seq[user] = int(events[-1]["sequence"]) + 1
            yield events

    def batches(self, *, max_events: int = 200) -> Iterator[list[dict]]:
        """Single-tenant batches, same shape `CorpusBuilder.batches` produces."""
        buf: list[dict] = []
        for events in self.events():
            buf.extend(events)
            while len(buf) >= max_events:
                yield buf[:max_events]
                buf = buf[max_events:]
        while buf:
            yield buf[:max_events]
            buf = buf[max_events:]

    def build(self, ledger_path: Path, *, max_events: int = 200) -> int:
        """Append the flood to a ledger. Returns the event count."""
        events = 0
        with Ledger(Path(ledger_path)) as ledger:
            for batch in self.batches(max_events=max_events):
                for ev in batch:
                    events += 1
                    ledger.append(
                        LedgerRecord(
                            id=ev["id"],
                            source=ev["source"],
                            type=ev["type"],
                            tenant=self.career_site_id,
                            user_pseudo=ev["data"]["candidate"]["user_id"],
                            seq=int(ev["sequence"]),
                        )
                    )
        return events


# --- the CLI ------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m driver.inject",
        description="Generate a corpus with an exact fraction of malformed events, "
        "and/or a single-tenant flood. Both are off by default.",
    )
    parser.add_argument("--sessions", type=int, default=1_000, help="baseline sessions to generate")
    parser.add_argument("--tenants", type=int, default=5, help="tenants in the catalog")
    parser.add_argument("--seed", type=int, default=7, help="corpus and injection seed")
    parser.add_argument("--users-per-tenant", type=int, default=50)
    parser.add_argument("--max-events", type=int, default=200, help="events per batch")
    parser.add_argument("--ledger", type=Path, help="write the ground-truth ledger here")
    parser.add_argument(
        "--inject-invalid-rate",
        type=float,
        default=0.0,
        metavar="PCT",
        help="exactly this percentage of events is malformed (0-100). Default 0: a clean run.",
    )
    parser.add_argument("--flood-tenant", help="flood this tenant and leave the rest at baseline")
    parser.add_argument("--flood-sessions", type=int, default=DEFAULT_FLOOD_SESSIONS)
    parser.add_argument("--flood-users", type=int, default=DEFAULT_FLOOD_USERS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not 0.0 <= args.inject_invalid_rate <= 100.0:
        parser.error("--inject-invalid-rate is a percentage and must be between 0 and 100")
    if args.flood_tenant and args.sessions < 0:
        parser.error("--sessions must be non-negative")

    catalog = TenantCatalog(args.tenants, seed=args.seed)

    def corpus() -> SkewedCorpus:
        # A fresh corpus per pass, not one instance asked twice: the generator
        # advances its own RNG, so the second pass over the same instance is a
        # different-sized run and the exact count would be a lie. See
        # `count_events`.
        return SkewedCorpus(
            catalog=catalog, seed=args.seed, users_per_tenant=args.users_per_tenant
        )

    injector = MalformedInjector(rate=args.inject_invalid_rate / 100.0, seed=args.seed)
    total = (
        count_events(corpus().batches(args.sessions, max_events=args.max_events))
        if injector.enabled
        else None
    )
    batches = corpus().batches(args.sessions, max_events=args.max_events)
    if args.ledger is not None:
        report = injector.build(batches, args.ledger, total_events=total)
    else:
        for _ in injector.batches(batches, total_events=total):
            pass
        report = injector.report

    summary: dict = report.to_dict()
    if args.ledger is not None:
        summary["ledger"] = str(args.ledger)
        summary["ledger_rows"] = report.emitted
    if args.flood_tenant:
        if args.flood_tenant not in catalog.ids:
            parser.error(f"--flood-tenant {args.flood_tenant!r} is not in the {args.tenants}-tenant catalog")
        flood = TenantFlood(
            args.flood_tenant,
            sessions=args.flood_sessions,
            users=args.flood_users,
            seed=args.seed,
        )
        if args.ledger is not None:
            flood_events = flood.build(args.ledger, max_events=args.max_events)
            summary["ledger_rows"] += flood_events
        else:
            flood_events = count_events(flood.batches(max_events=args.max_events))
        summary["flood"] = {
            "tenant": args.flood_tenant,
            "events": flood_events,
            "users": len(flood.users),
        }
    print(json.dumps(summary, indent=2))
    return 0


__all__ = [
    "DEFAULT_FLOOD_SESSIONS",
    "DEFAULT_FLOOD_USERS",
    "ILLEGAL_ATTRIBUTE",
    "ILLEGAL_TIME",
    "ILLEGAL_TYPE",
    "VARIANT_CODES",
    "InjectionPlan",
    "InjectionReport",
    "MalformedInjector",
    "Slot",
    "TenantFlood",
    "build_parser",
    "corrupt",
    "count_events",
    "flood_user_id",
    "main",
    "plan_injection",
]


if __name__ == "__main__":
    raise SystemExit(main())
