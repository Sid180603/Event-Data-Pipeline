"""Per-tenant token bucket: the load shedder (plan T7).

PROCESS-LOCAL BY DESIGN, not by oversight. The gateway runs N uvicorn worker
processes (N >= 6), and per-process state does not combine into a system-wide
property. This bucket is meaningful only because the load balancer routes on
`hash(career_site_id | user_id_pseudo)` (plan D12, sticky routing), so every
event for one `(tenant, user)` pair lands on exactly ONE worker and therefore in
exactly ONE bucket. Without that guarantee a tenant's real limit would be N
times the configured rate, spread nondeterministically across workers, and
nothing here could detect it.

`BucketRegistry.summary()` is the observable seam that makes the precondition
checkable: a test can assert the rate this process enforces is the rate it was
configured with, which holds only while a tenant's traffic has a single owner.
See `test_without_sticky_routing_the_aggregate_rate_is_n_times_configured` for
the failure mode this exists to prevent.
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
