"""The tenant key registry: a fixed, bounded set derived once at startup.

Plan D5, review finding H4. The rejected design cached derived keys under the
`career_site_id` from the JWT. That is an unbounded-growth DoS -- the cache key
is attacker-chosen and nothing ever evicts -- and r3.1 justified the derivation
cost with "50,000 HKDF calls/sec" when there are 500 tenants in total.

This is a registry, not a cache: keys exist for the tenants that were
configured, an unknown `career_site_id` is rejected rather than derived, and a
lookup is a dict hit. That is better on all three axes -- the growth is bounded
by configuration, the derivation cost is paid once, and rejecting unknown
tenants is a tenant-existence check we wanted anyway.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from app.config import Settings
from app.crypto import aesgcm
from app.crypto.aesgcm import ciphertext_key_version
from app.crypto.keys import TenantKeys, derive_tenant_keys


class UnknownTenantError(LookupError):
    """No key for this `career_site_id`. The caller rejects the request."""


class UnknownKeyVersionError(LookupError):
    """A ciphertext names a key version this registry does not hold.

    Rotation, not tampering: the ciphertext is fine, this process simply cannot
    read it yet. Deliberately distinct from `InvalidTag` so a decrypt failure can
    be triaged without guessing.
    """


class TenantKeyRegistry:
    """Every configured tenant's keys, derived at construction and never after."""

    __slots__ = ("_keys", "key_version")

    def __init__(self, settings: Settings, career_site_ids: Iterable[str]) -> None:
        self.key_version = settings.key_version
        self._keys: dict[str, TenantKeys] = {
            career_site_id: derive_tenant_keys(
                settings.master_secret,
                settings.tenant_key_salt(career_site_id),
                career_site_id=career_site_id,
                key_version=settings.key_version,
            )
            for career_site_id in career_site_ids
        }

    def keys_for(self, career_site_id: str) -> TenantKeys:
        try:
            return self._keys[career_site_id]
        except KeyError:
            # The tenant id is echoed back because it came from a token we are
            # rejecting, and a rate limit keyed on it is the caller's problem.
            raise UnknownTenantError(
                f"career_site_id {career_site_id!r} is not in the tenant registry"
            ) from None

    def encrypt_field(
        self, career_site_id: str, plaintext: str, *, source: str, event_id: str, event_type: str, field_name: str
    ) -> str:
        """Encrypt one field with the tenant's own key, stamped with its version."""
        keys = self.keys_for(career_site_id)
        return aesgcm.encrypt_field(
            keys.enc_key,
            plaintext,
            source=source,
            event_id=event_id,
            event_type=event_type,
            field_name=field_name,
            key_version=keys.key_version,
        )

    def decrypt_field(
        self, career_site_id: str, token: str, *, source: str, event_id: str, event_type: str, field_name: str
    ) -> str:
        """Decrypt one field, selecting the key from the ciphertext's own prefix.

        The version is read from the token, not taken from the registry, so a
        consumer needs no out-of-band knowledge to pick a key. Callers MUST NOT
        surface the `InvalidTag` this can raise to a client -- it distinguishes
        "wrong key" from "wrong data" for whoever is probing.
        """
        keys = self.keys_for(career_site_id)
        key_version = ciphertext_key_version(token)
        if key_version != keys.key_version:
            raise UnknownKeyVersionError(
                f"ciphertext is keyversion {key_version}, this registry holds {keys.key_version}"
            )
        return aesgcm.decrypt_field(
            keys.enc_key,
            token,
            source=source,
            event_id=event_id,
            event_type=event_type,
            field_name=field_name,
            key_version=key_version,
        )

    @property
    def career_site_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._keys))

    def __contains__(self, career_site_id: object) -> bool:
        return career_site_id in self._keys

    def __iter__(self) -> Iterator[TenantKeys]:
        return iter(self._keys.values())

    def __len__(self) -> int:
        return len(self._keys)

    def __repr__(self) -> str:  # key material must never reach a log or a traceback
        return f"TenantKeyRegistry(tenants={len(self._keys)}, key_version={self.key_version})"
