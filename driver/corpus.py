"""T8a driver: replay corpus.

Generated once, offline, then replayed. This is what makes 50k events/sec
reachable from Python at all: event construction is a heavy CPU cost, and it is
moved out of the load path into a one-off build step (plan D9).

Every batch is single-tenant, because the gateway rejects mixed-tenant batches
with 403 (CONTRACT.md section 7).
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from pathlib import Path

from contracts.attributes import distinct_sources
from contracts.cloudevent import decode_batch
from contracts.ledger import Ledger, LedgerRecord
from driver.fsm import ABANDONED, DRAFT_SAVED, SUBMITTED, SessionConfig, generate_session

SOURCE_CHANNELS = ["WEB_APP", "MOBILE_APP", "THIRD_PARTY_SERVICE"]


class CorpusBuilder:
    def __init__(
        self,
        *,
        career_site_ids: list[str],
        seed: int = 7,
        users_per_tenant: int = 50,
        drop_off_rate: float = 0.25,
    ) -> None:
        self.career_site_ids = career_site_ids
        self.rng = random.Random(seed)
        self.users_per_tenant = users_per_tenant
        self.drop_off_rate = drop_off_rate
        self._seq: dict[tuple[str, str], int] = {}
        self._user_cache: dict[str, list[str]] = {}

    def _next_sequence(self, career_site_id: str, user_pseudo: str) -> int:
        key = (career_site_id, user_pseudo)
        n = self._seq.get(key, 0)
        self._seq[key] = n + 1
        return n

    def _users(self, career_site_id: str) -> list[str]:
        # Deterministic per tenant, so a regenerated corpus is comparable, and
        # cached because it is consulted once per session.
        if career_site_id not in self._user_cache:
            rng = random.Random(f"{career_site_id}:users")
            self._user_cache[career_site_id] = [
                f"{career_site_id}u{rng.randrange(1 << 30):08x}" for _ in range(self.users_per_tenant)
            ]
        return self._user_cache[career_site_id]

    def _outcome(self) -> str:
        r = self.rng.random()
        if r < self.drop_off_rate:
            return ABANDONED
        if r < self.drop_off_rate + 0.15:
            return DRAFT_SAVED
        return SUBMITTED

    def sessions(self, total_sessions: int) -> Iterator[tuple[str, list[dict]]]:
        """Yield (career_site_id, events) per session, round-robin across tenants."""
        for i in range(total_sessions):
            tenant = self.career_site_ids[i % len(self.career_site_ids)]
            user = self.rng.choice(self._users(tenant))
            cfg = SessionConfig(
                career_site_id=tenant,
                user_pseudo=user,
                source_channel=self.rng.choice(SOURCE_CHANNELS),
                job_id=f"job_{self.rng.randrange(10_000, 99_999)}",
                steps=self.rng.randint(1, 4),
                outcome=self._outcome(),
                start_sequence=self._next_sequence(tenant, user),
                referrer=self.rng.choice(["SEARCH", "RECOMMENDATION", "DIRECT"]),
                rng=random.Random(f"{tenant}:{user}:{i}"),
            )
            events = generate_session(cfg)
            # Reserve the whole range the session consumed, not just its start,
            # or the next session for this user reissues sequence numbers.
            self._seq[(tenant, user)] = int(events[-1]["sequence"]) + 1
            yield tenant, events

    def batches(
        self, total_sessions: int, *, max_events: int = 200
    ) -> Iterator[list[dict]]:
        """Group sessions into single-tenant batches of at most `max_events`.

        Sessions are visited round-robin across tenants, so a single shared
        buffer would fill with mixed tenants -- and the gateway rejects those
        with 403. Each tenant therefore buffers independently.
        """
        buffers: dict[str, list[dict]] = {}
        for tenant, events in self.sessions(total_sessions):
            buf = buffers.setdefault(tenant, [])
            buf.extend(events)
            while len(buf) >= max_events:
                yield buf[:max_events]
                # Reassign the local too: `buf` and the dict entry alias the
                # same list, so trimming only the entry would loop forever.
                buf = buf[max_events:]
            buffers[tenant] = buf
        for buf in buffers.values():
            while buf:
                yield buf[:max_events]
                buf = buf[max_events:]

    def build(
        self, total_sessions: int, ledger_path: Path, *, max_events: int = 200
    ) -> tuple[int, int]:
        """Write the ledger. Returns (batches, events)."""
        batches = events = 0
        with Ledger(ledger_path) as ledger:
            for batch in self.batches(total_sessions, max_events=max_events):
                batches += 1
                for ev in batch:
                    events += 1
                    ledger.append(
                        LedgerRecord(
                            id=ev["id"],
                            source=ev["source"],
                            type=ev["type"],
                            tenant=ev["source"].rsplit("/", 1)[-1],
                            user_pseudo=ev["data"]["candidate"]["user_id_pseudo"],
                            seq=int(ev["sequence"]),
                        )
                    )
        return batches, events
