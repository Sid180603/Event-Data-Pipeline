"""Per-tenant token bucket: the load shedder (plan T7).

PROCESS-LOCAL BY DESIGN, not by oversight. The gateway runs N uvicorn worker
processes, and per-process state does not combine into a system-wide property.
This bucket is meant to be meaningful only because the load balancer routes on
`hash(career_site_id | user_id_pseudo)` (plan D12, sticky routing), so every event
for one `(tenant, user)` pair lands on exactly ONE worker and therefore in exactly
ONE bucket. Without that guarantee a tenant's real limit is N times the configured
rate, spread nondeterministically across workers, and nothing here can detect it.

**That balancer does not exist yet.** This module reads as though it does, and
that reading is wrong: `Settings.sticky_routing` reads `STICKY_ROUTING` from the
environment and **nothing in the gateway reads that setting**. There is no
affinity layer in front of uvicorn's `--workers`, so a `(tenant, user)` pair
reaches all N workers and a tenant's effective budget is up to N times the
configured rate. Partitioning is still correct and stable -- the Kafka key is
derived, so the partition is deterministic -- and per-process enforcement is still
a real shedder, which is what the demo's flood beat exercises. What is NOT true is
the single-bucket-per-tenant property this docstring previously asserted.

So: shed behaviour is honest, the configured rate is an N-fold floor rather than
the aggregate limit, and any claim about the *exact* enforced rate is not yet
supportable. Closing this needs an L7 balancer that hashes the gateway-derived key,
which is outside this slice.

`BucketRegistry.summary()` remains the observable seam that would make the
precondition checkable: a test can assert the rate this process enforces is the
rate it was configured with, which holds only while a tenant's traffic has a
single owner. See `test_without_sticky_routing_the_aggregate_rate_is_n_times_configured`
for the failure mode this exists to prevent.
"""

from __future__ import annotations

from .bucket import RETRY_AFTER_HEADER, TOO_MANY_REQUESTS, RateLimitDecision, TokenBucket
from .registry import BucketRegistry, RegistrySummary, TenantLimits

__all__ = [
    "BucketRegistry",
    "RETRY_AFTER_HEADER",
    "RateLimitDecision",
    "RegistrySummary",
    "TOO_MANY_REQUESTS",
    "TenantLimits",
    "TokenBucket",
]
