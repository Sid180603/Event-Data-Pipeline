"""The fixed tenant + credential registry (plan H4).

This is a *registry*, not a cache. r3.1 cached per-tenant keys on the JWT's
`career_site_id`, which meant an attacker could mint unlimited cache keys and
that an unknown tenant silently got a key. Here the set of tenants is decided
once, at startup, and a `career_site_id` that is not in it is refused.

It is also the only source of `sourcechannel`. A client may send `X-Source-Type`
as a hint; the recorded channel always comes from the credential registration,
and a hint that disagrees is a 403 rather than something we honour (D4).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import get_args

from contracts.cloudevent import SourceChannel

CHANNELS: frozenset[str] = frozenset(get_args(SourceChannel))


@dataclass(frozen=True, slots=True)
class Credential:
    """One registered client credential.

    `credential_id` is what the token asserts, so the pairing of tenant to
    channel is a server-side fact. A token that names a credential belonging to
    another tenant is refused -- otherwise the tenant binding is only as strong
    as the token's own claims.
    """

    credential_id: str
    career_site_id: str
    source_channel: SourceChannel

    def __post_init__(self) -> None:
        if not self.credential_id:
            raise ValueError("credential_id must be non-empty")
        if not self.career_site_id:
            raise ValueError("career_site_id must be non-empty")
        if self.source_channel not in CHANNELS:
            raise ValueError(
                f"source_channel must be one of {sorted(CHANNELS)}, "
                f"got {self.source_channel!r}"
            )


class TenantRegistry:
    """Tenant membership and credential -> channel, built once at startup."""

    __slots__ = ("_credentials", "_tenants")

    def __init__(self, credentials: Iterable[Credential]) -> None:
        by_id: dict[str, Credential] = {}
        tenants: set[str] = set()
        for cred in credentials:
            # A duplicate id would silently shadow a registration, and the
            # shadowed tenant's channel is exactly what an attacker would want.
            if cred.credential_id in by_id:
                raise ValueError(f"duplicate credential_id {cred.credential_id!r}")
            by_id[cred.credential_id] = cred
            tenants.add(cred.career_site_id)
        self._credentials = by_id
        self._tenants = frozenset(tenants)

    def contains(self, career_site_id: str) -> bool:
        return career_site_id in self._tenants

    def credential(self, credential_id: str) -> Credential | None:
        return self._credentials.get(credential_id)

    @property
    def tenants(self) -> frozenset[str]:
        return self._tenants

    def __len__(self) -> int:
        return len(self._tenants)
