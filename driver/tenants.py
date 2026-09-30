"""T8b: the tenant catalog and shard routing.

Identity only. Which tenant is busy, how busy, and in what order is `skew.py`'s
job; this module answers "which tenants exist", "who are their users", and
"which shard owns this tenant", so the answers can be identical in several
driver processes.

The shard split is a fixed BLAKE2b digest of the tenant id rather than
`hash()`. Python salts `hash()` per process, so two driver processes given the
same catalog would claim the same tenants twice and no tenant at all.
"""

from __future__ import annotations

import hashlib
import random
from collections.abc import Iterable
from pathlib import Path

from contracts.ledger import Ledger

#: 500 tenants, per SPEC.txt:148 ("large enterprise tenants generate
#: exponentially higher traffic during hiring drives than SMBs" -- which only
#: matters if the catalog is big enough for the SMB tail to exist at all).
DEFAULT_TENANT_COUNT = 500

#: SMB floor. Head tenants get more (see `skew.VolumePlan`); nobody gets fewer,
#: so the tail is a tail of real deployments rather than of generated tokens.
DEFAULT_USERS_PER_TENANT = 50


class TenantCatalog:
    """A deterministic, ordered set of tenants. Index 0 is the busiest rank."""

    def __init__(
        self,
        count: int = DEFAULT_TENANT_COUNT,
        *,
        seed: int = 7,
        users_per_tenant: int = DEFAULT_USERS_PER_TENANT,
    ) -> None:
        if count < 1:
            raise ValueError(f"count must be positive, got {count}")
        self.count = count
        self.seed = seed
        self.users_per_tenant = users_per_tenant
        # The id encodes the rank so a tenant's weight is readable off its name
        # in a ledger, and a tokenised id (`tenant_0001`) still validates against
        # the `source` pattern in contracts/attributes.py.
        self.ids: list[str] = [f"tenant_{rank:04d}" for rank in range(count)]

    def users(self, career_site_id: str, count: int | None = None) -> list[str]:
        """The tenant's user ids, deterministic for the tenant.

        Prefix-stable: asking for 3 and then for 40 gives the same first three, so
        growing a whale's user count cannot renumber a user that already exists
        and move its sticky route.
        """
        n = self.users_per_tenant if count is None else count
        rng = random.Random(f"{career_site_id}:users")
        return [f"{career_site_id}u{rng.randrange(1 << 30):08x}" for _ in range(n)]

    def shard_of(self, career_site_id: str, shards: int) -> int:
        if shards < 1:
            raise ValueError(f"shards must be positive, got {shards}")
        digest = hashlib.blake2b(career_site_id.encode(), digest_size=8).digest()
        return int.from_bytes(digest, "big") % shards

    def shard_ids(self, shard: int, shards: int) -> list[str]:
        """The tenants this shard owns, in rank order."""
        if not 0 <= shard < shards:
            raise ValueError(f"shard {shard} out of range for {shards} shards")
        return [t for t in self.ids if self.shard_of(t, shards) == shard]


def merge_ledgers(paths: Iterable[Path], out_path: Path) -> int:
    """Concatenate shard ledgers. Returns the row count.

    A tenant appearing in two shards would have its per-user `sequence` numbers
    forked by two independent counters, and the Queue team's sessionizer would
    interleave two half-journeys forever. Disjointness is therefore checked, not
    assumed -- `id`s are uuid4 so they will not collide, but tenants can.
    """
    owner: dict[str, int] = {}
    for i, path in enumerate(paths):
        for rec in Ledger(path).read_all():
            if rec.tenant in owner and owner[rec.tenant] != i:
                raise ValueError(
                    f"tenant {rec.tenant!r} appears in shards {owner[rec.tenant]} and {i}; "
                    "shards must be tenant-disjoint"
                )
            owner[rec.tenant] = i

    rows = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.unlink(missing_ok=True)  # Ledger opens append; a stale merge would silently accumulate
    with Ledger(out_path) as out:
        for path in paths:
            for rec in Ledger(path).read_all():
                out.append(rec)
                rows += 1
    return rows


__all__ = [
    "DEFAULT_TENANT_COUNT",
    "DEFAULT_USERS_PER_TENANT",
    "TenantCatalog",
    "merge_ledgers",
]
