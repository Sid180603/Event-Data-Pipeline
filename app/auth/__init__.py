"""T4 auth: asymmetric JWT verification and JWT-only tenant binding (plan D4).

The whole module exists to make one invariant cheap to check: **the tenant comes
from a signed claim and from nowhere else.** Not the `source` in the body, not a
header, not a client-supplied `partitionkey`. Everything a caller sends is
treated as a hint to be validated, never as an input to be trusted.

`authorize_batch` is the single entry point. One call authenticates one HTTP
request: it verifies the token once (D2), then binds the whole batch to the one
tenant that token is for.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from app.auth.errors import AuthError, Forbidden, Unauthorized
from app.auth.jwt import (
    CREDENTIAL_CLAIM,
    TENANT_CLAIM,
    TokenParseBudget,
    TokenVerifier,
    VerifiedToken,
)
from app.auth.registry import CHANNELS, Credential, TenantRegistry
from contracts.attributes import distinct_sources
from contracts.cloudevent import CloudEvent

__all__ = [
    "AuthError",
    "AuthenticatedBatch",
    "CREDENTIAL_CLAIM",
    "CHANNELS",
    "Credential",
    "Forbidden",
    "TENANT_CLAIM",
    "TenantRegistry",
    "TokenParseBudget",
    "TokenVerifier",
    "Unauthorized",
    "VerifiedToken",
    "authorize_batch",
]


@dataclass(frozen=True, slots=True)
class AuthenticatedBatch:
    """The gateway's own view of an authorized request.

    Every field is derived here or covered by the signature. `source` is the
    one authoritative tenant string; `source_channel` comes from the credential
    registration, never from a request header.
    """

    career_site_id: str
    source: str
    source_channel: str
    credential_id: str
    event_count: int


def authorize_batch(
    events: Sequence[CloudEvent],
    *,
    authorization: str | None,
    verifier: TokenVerifier,
    registry: TenantRegistry,
    x_source_type: str | None = None,
    parse_budget: TokenParseBudget | None = None,
) -> AuthenticatedBatch:
    """Authenticate one request and bind its whole batch to one tenant.

    Raises `Unauthorized` (401) for anything that leaves us without a verified
    tenant, and `Forbidden` (403) for a verified tenant asking for something it
    does not own. The order matters: authenticate before touching tenant state,
    so an unauthenticated caller never reaches the registry.
    """
    try:
        token = verifier.verify(authorization)
    except Unauthorized:
        # Charge the unauthenticated path only. A token we cannot read has no
        # tenant, so it cannot spend a tenant's budget -- but it still costs us
        # a signature check, so it gets its own.
        if parse_budget is not None:
            parse_budget.charge()
        raise

    if not registry.contains(token.career_site_id):
        # H4: the registry is fixed. A valid signature from a tenant we have
        # never provisioned is refused rather than quietly given keys.
        raise Forbidden(f"unknown tenant {token.career_site_id!r}")

    credential = registry.credential(token.credential_id)
    if credential is None or credential.career_site_id != token.career_site_id:
        # The token pairs tenant A with tenant B's credential. Refusing the pair
        # is what stops a token being valid for two tenants at once.
        raise Forbidden(f"credential {token.credential_id!r} is not registered for this tenant")

    channel = credential.source_channel
    _check_source_type_hint(x_source_type, channel)

    source = f"/careers/{token.career_site_id}"
    _bind_batch_to_source(events, source)

    return AuthenticatedBatch(
        career_site_id=token.career_site_id,
        source=source,
        source_channel=channel,
        credential_id=credential.credential_id,
        event_count=len(events),
    )


def _check_source_type_hint(x_source_type: str | None, registered: str) -> None:
    """`X-Source-Type` is validated, never honoured.

    `sourcechannel` is a credential-registration fact. If the client sends a
    hint that disagrees, the request is refused -- silently accepting it would
    mean the channel in the Kafka record is attacker-influenced.
    """
    if x_source_type is None:
        return
    if x_source_type not in CHANNELS:
        raise Forbidden(f"X-Source-Type {x_source_type!r} is not a known source channel")
    if x_source_type != registered:
        raise Forbidden(
            f"X-Source-Type {x_source_type!r} disagrees with the registered channel "
            f"{registered!r}"
        )


def _bind_batch_to_source(events: Sequence[CloudEvent], source: str) -> None:
    """One batch, one tenant (D2).

    A multi-tenant batch would mean trusting `source` from the payload, which is
    exactly the hole D4 closes -- so it is refused whole, never split and never
    partially accepted. The cardinality check comes first so the reason is the
    real one, then every event is compared against the token's tenant.
    """
    if distinct_sources(events) > 1:
        raise Forbidden("batch mixes tenants: a batch must carry exactly one source")
    for event in events:
        if event.source != source:
            raise Forbidden(
                f"body source {event.source!r} disagrees with the token's tenant {source!r}"
            )
