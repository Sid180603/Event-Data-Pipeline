"""Auth failures, as HTTP status codes.

The split is not cosmetic -- the ingest handler maps `status_code` straight onto
the response, and the difference is load-bearing:

* `401` -- we cannot say who you are. Missing, malformed, badly signed, expired,
  wrong audience, or no tenant claim. Nothing is looked up per tenant, so this
  path must stay cheap: it is the one an unauthenticated attacker controls.
* `403` -- we know exactly who you are, and no. Body tenant disagrees with the
  token, mixed-tenant batch, tenant not in the registry, forged `X-Source-Type`.

Both carry a machine-readable `reason` for logs and metrics. The reason is never
echoed to the client on a 401: telling a prober *which* check failed turns the
endpoint into a signature oracle.
"""

from __future__ import annotations


class AuthError(Exception):
    status_code: int = 401

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class Unauthorized(AuthError):
    """No verified tenant identity. HTTP 401."""

    status_code = 401


class Forbidden(AuthError):
    """Verified identity, refused. HTTP 403."""

    status_code = 403
