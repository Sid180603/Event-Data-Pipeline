"""Asymmetric JWT verification (plan D4 / H2).

r3.1 specified HS256 and justified it by arithmetic that D2 had already refuted:
batching means ~1,000 verifications/sec, not 50,000. The real cost of HS256 was
never CPU -- it is that a *shared* secret lets anyone holding it mint a token for
every one of the 500 tenants, so a gateway compromise is a total compromise.

So: EdDSA (preferred) or RS256 only, verified against a cached **public** key
loaded once at construction. `algorithms` is pinned to that single value, which
is what rejects both `alg: none` and the classic key-confusion attack (an
attacker signing HS256 with the public key we already hold).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed448 import Ed448PublicKey
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from jwt import PyJWTError

from app.auth.errors import Unauthorized
from contracts.attributes import career_site_id_from_source

#: The tenant claim. This is the ONLY place a tenant identity may come from.
TENANT_CLAIM = "career_site_id"
#: The registered credential the token was issued to; selects `sourcechannel`.
CREDENTIAL_CLAIM = "credential_id"

#: `none` is not in this list and never will be.
ALLOWED_ALGORITHMS = ("EdDSA", "RS256")

_EXPECTED_KEY_TYPES: dict[str, tuple[type, ...]] = {
    "EdDSA": (Ed25519PublicKey, Ed448PublicKey),
    "RS256": (RSAPublicKey,),
}


@dataclass(frozen=True, slots=True)
class VerifiedToken:
    """What a valid signature means. Nothing here came from the request body."""

    career_site_id: str
    credential_id: str


def _load_public_key(pem: str, algorithm: str):
    if not pem.strip():
        raise ValueError("public_key_pem is empty: the gateway needs a public key")
    try:
        key = serialization.load_pem_public_key(pem.encode())
    except (ValueError, TypeError) as exc:
        # A private key lands here too -- `load_pem_public_key` refuses it. The
        # gateway must never be *able* to hold one.
        raise ValueError(f"public_key_pem is not a usable public key: {exc}") from exc
    if not isinstance(key, _EXPECTED_KEY_TYPES[algorithm]):
        raise ValueError(
            f"public_key_pem is a {type(key).__name__}, which cannot verify {algorithm}"
        )
    return key


class TokenVerifier:
    """Verifies bearer tokens against one cached public key.

    One instance per process, shared by every request: `load_pem_public_key` and
    the algorithm/key-type check happen once, not per event.
    """

    __slots__ = ("_algorithm", "_audience", "_key", "verify_count")

    def __init__(self, public_key_pem: str, algorithm: str, audience: str) -> None:
        if algorithm not in ALLOWED_ALGORITHMS:
            raise ValueError(
                f"algorithm must be one of {ALLOWED_ALGORITHMS} (asymmetric only: a "
                f"shared secret would let the gateway mint tokens for every tenant), "
                f"got {algorithm!r}"
            )
        self._key = _load_public_key(public_key_pem, algorithm)
        self._algorithm = algorithm
        self._audience = audience
        #: Every decode attempt, successful or not. This is the number the "one
        #: verify per batch, not per event" claim is measured with (D2), so it
        #: counts attempts rather than successes. A header rejected before
        #: decode costs no crypto and is not counted.
        self.verify_count = 0

    @property
    def public_key(self):
        """The cached public key. Read-only -- there is no signing path here."""
        return self._key

    def verify(self, authorization: str | None) -> VerifiedToken:
        token = _bearer_token(authorization)
        try:
            claims = jwt.decode(
                token,
                self._key,
                algorithms=[self._algorithm],  # pinned: no `none`, no HS256
                audience=self._audience,
                options={"require": ["exp", "aud"]},
            )
        except PyJWTError as exc:
            # Class name only. Whether the signature, the audience or the expiry
            # was wrong is not something a 401 body should tell a prober.
            raise Unauthorized(type(exc).__name__) from None
        finally:
            self.verify_count += 1

        return VerifiedToken(
            career_site_id=_tenant_claim(claims),
            credential_id=_credential_claim(claims),
        )


def _bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise Unauthorized("missing Authorization header")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise Unauthorized("malformed Authorization header")
    return parts[1]


def _tenant_claim(claims: dict) -> str:
    value = claims.get(TENANT_CLAIM)
    if not isinstance(value, str) or not value:
        raise Unauthorized(f"missing {TENANT_CLAIM} claim")
    # Reuse the contract's own parser rather than a second regex: the claim is
    # about to be concatenated into a `source`, so it has to survive that shape.
    # `a/b` or a 65-character id are refused rather than reaching a path or key.
    try:
        return career_site_id_from_source(f"/careers/{value}")
    except ValueError:
        raise Unauthorized(f"unusable {TENANT_CLAIM} claim") from None


def _credential_claim(claims: dict) -> str:
    value = claims.get(CREDENTIAL_CLAIM)
    if not isinstance(value, str) or not value:
        raise Unauthorized(f"missing {CREDENTIAL_CLAIM} claim")
    return value


class TokenParseBudget:
    """Rate limit for the *unauthenticated* path.

    Why this is not the per-tenant bucket: the request has no tenant yet, so it
    cannot be charged to one. Every garbage token is otherwise free for the
    attacker (a base64 split and a signature check) and would otherwise consume
    the same request budget as a legitimate 500-event batch. T7 wires the real
    limiter in; this is the hook it replaces.

    A refusal is `401`, not `429`: there is no tenant to attribute a per-tenant
    limit to, and `429` is reserved for the T7 per-tenant semantics.
    """

    __slots__ = ("_limit", "malformed", "refused")

    def __init__(self, limit: int) -> None:
        if limit < 0:
            raise ValueError("limit must be >= 0")
        self._limit = limit
        #: Every token that failed to verify or parse.
        self.malformed = 0
        #: How many of those we refused before even trying.
        self.refused = 0

    def charge(self) -> None:
        self.malformed += 1
        if self.malformed > self._limit:
            self.refused += 1
            raise Unauthorized("malformed-token budget exhausted")
