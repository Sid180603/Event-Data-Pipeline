"""Per-tenant bucket registry: the mapping from tenant to budget (plan T7/T5).

The rate configuration is a fixed, bounded tenant table (plan D5 -- a JWT-
controlled cache key is an unbounded-growth DoS), read-only after construction.
The *buckets* are created lazily per tenant, because a worker's traffic is not
a uniform sample of the tenant list and materialising 500 buckets up front buys
nothing.

**No lock on the registry itself.** 500 tenants creating their first bucket
concurrently would serialise behind one mutex, which is exactly the contention
this component exists to avoid. `dict.setdefault` is a single C-level call and
is atomic under the GIL, so first-touch is lock-free. The cost is that the
losing thread of a race builds a bucket that is immediately discarded, which is
harmless: a bucket's only state is its own token count.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass

from .bucket import Clock, RateLimitDecision, TokenBucket


@dataclass(frozen=True, slots=True)
class TenantLimits:
    """A tenant's configured budget: `rate` events/second sustained, `burst`
    events of slack on top."""

    rate: float
    burst: float


@dataclass(frozen=True, slots=True)
class RegistrySummary:
    """What this process is enforcing, for tests and for the T10a metric.

    `rates` is the configured rate per tenant, which is how the sticky-routing
    precondition (plan D12) is checked: the number published here must be the
    number actually enforced, and that is only true while a tenant's traffic
    reaches a single worker.
    """

    tenant_count: int
    configured_tenant_count: int
    rates: Mapping[str, float]


class BucketRegistry:
    def __init__(
        self,
        limits: Mapping[str, TenantLimits],
        *,
        default: TenantLimits,
        clock: Clock = time.monotonic,
    ) -> None:
        """`limits` is the configured tenant table; `default` applies to a tenant
        absent from it (the JWT-binding task rejects unknown tenants before they
        reach the limiter, so this is a floor rather than an open door)."""
        self._limits: Mapping[str, TenantLimits] = dict(limits)
        self._default = default
        self._clock = clock
        self._buckets: dict[str, TokenBucket] = {}

    def _limits_for(self, tenant: str) -> TenantLimits:
        found = self._limits.get(tenant)
        return self._default if found is None else found

    def bucket_for(self, tenant: str) -> TokenBucket:
        """This tenant's bucket, created on first sight."""
        bucket = self._buckets.get(tenant)
        if bucket is not None:
            return bucket
        return self._buckets.setdefault(tenant, self._new_bucket(tenant))

    def _new_bucket(self, tenant: str) -> TokenBucket:
        limits = self._limits_for(tenant)
        return TokenBucket(rate=limits.rate, burst=limits.burst, clock=self._clock)

    def acquire(self, tenant: str) -> RateLimitDecision:
        """Charge one event to `tenant` and report allow/deny. The caller turns
        a denial into `429` + `Retry-After` (see `TOO_MANY_REQUESTS`)."""
        return self.bucket_for(tenant).acquire()

    def summary(self) -> RegistrySummary:
        rates = {tenant: limits.rate for tenant, limits in self._limits.items()}
        for tenant, bucket in self._buckets.items():
            rates.setdefault(tenant, bucket.rate)
        return RegistrySummary(
            tenant_count=len(rates),
            configured_tenant_count=len(self._limits),
            rates=rates,
        )
