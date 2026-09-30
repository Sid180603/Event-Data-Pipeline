"""A single tenant's token bucket (plan T7, defence #2 in plan section 1.3).

Rate limiting is how the gateway sheds load before it degrades: Python does not
fail fast under overload, it accepts everything and gets slower, so a tenant
that outruns its share has to be told 429 while the other tenants are untouched.

**Concurrency.** One `threading.Lock` per bucket, never a shared/global one, so
500 tenants never serialise behind each other. The lock is not decoration: the
GIL makes a single attribute store atomic but not a read-modify-write, and
`acquire` is a read-modify-write (refill from elapsed time, compare, decrement,
advance the clock). Interleaving two of those loses tokens. Under free-threading
the same code needs the lock for a second reason.

**Time.** Injected, so tests exercise refill math exactly instead of sleeping.
Refill is lazy: a bucket holds no timer and no thread, it just asks "how long has
it been" when it is called (criterion 8).
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

#: Status the caller MUST return for a denied event (plan D4). The HTTP layer
#: owns the response; this module only decides allow/deny.
TOO_MANY_REQUESTS = 429

#: Header carrying the wait. RFC 9110 integer-seconds form.
RETRY_AFTER_HEADER = "Retry-After"

#: Monotonic clock returning seconds. Injectable so tests never sleep.
Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    """Outcome of charging one event to a tenant's budget.

    `retry_after` is 0 when allowed (nothing to wait for) and always a positive
    integer when denied, so it can go straight into `Retry-After`.
    """

    allowed: bool
    retry_after: int
    remaining: float


class TokenBucket:
    """Classic token bucket: `rate` tokens/second refilled continuously, capped
    at `burst`. A new bucket starts full, so a tenant may burst immediately."""

    __slots__ = ("rate", "burst", "_clock", "_lock", "_tokens", "_updated_at", "consumed", "denied")

    def __init__(self, rate: float, burst: float, *, clock: Clock = time.monotonic) -> None:
        if rate <= 0:
            raise ValueError(f"rate must be > 0, got {rate!r}")
        if burst < 1:
            raise ValueError(f"burst must be >= 1 token, got {burst!r}")
        self.rate = float(rate)
        self.burst = float(burst)
        self._clock = clock
        self._lock = threading.Lock()
        self._tokens = self.burst
        self._updated_at = clock()
        #: Allowed / denied tallies, for the T10a metric and for the
        #: no-lost-updates test, which needs the number of tokens actually spent.
        self.consumed = 0
        self.denied = 0

    def acquire(self) -> RateLimitDecision:
        """Charge one token (one event) and report allow/deny."""
        with self._lock:
            now = self._clock()
            elapsed = now - self._updated_at
            if elapsed > 0:
                # Advance the clock on every call, allowed or not: a denial that
                # left `_updated_at` behind would bank the elapsed tokens and
                # hand the same credit out again on the next call.
                self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
                self._updated_at = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                self.consumed += 1
                return RateLimitDecision(True, 0, self._tokens)
            self.denied += 1
            return RateLimitDecision(False, self._retry_after(), self._tokens)

    def _retry_after(self) -> int:
        """Whole seconds until this bucket holds one token again. Caller holds
        the lock. At least 1, because a fractional `Retry-After` is not a valid
        HTTP-date and rounding down would invite an instant retry."""
        return max(1, math.ceil((1.0 - self._tokens) / self.rate))

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"TokenBucket(rate={self.rate!r}, burst={self.burst!r}, tokens={self._tokens!r})"
