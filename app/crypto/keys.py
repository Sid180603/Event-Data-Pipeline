"""Purpose-separated per-tenant key derivation (plan D5, review finding H4).

The published formulas are::

    enc_key = HKDF-SHA256(ikm=master, salt=career_site_id, info=b"enc")
    mac_key = HKDF-SHA256(ikm=master, salt=career_site_id, info=b"mac")

which is exactly **one HKDF extract** and **two expands that differ only in
`info`** -- the code below calls it that way, and `test_crypto.py` pins the
result against the two-formula version so the handoff to the DB team stays
byte-exact.

Why the separation is not optional: one key shared between AES-GCM and HMAC
means whoever recovers it under either primitive holds both, and a single
accidental reuse (a MAC compared with `==` under a timing oracle, an AES key
used as an HMAC key) is a full compromise of the other use.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF, HKDFExpand

#: HKDF `info` labels. These are part of the published derivation contract.
ENC_INFO = b"enc"
MAC_INFO = b"mac"

#: 32 bytes: AES-256, and the untruncated HMAC-SHA256 output.
KEY_LENGTH = 32

#: Anything shorter is not a master secret, it is a passphrase guess, and HKDF
#: will not tell us -- it will just as happily expand it into 500 tenant keys.
MIN_MASTER_SECRET_BYTES = 32


@dataclass(frozen=True, slots=True)
class TenantKeys:
    """One tenant's two purpose-separated keys, derived once at startup."""

    career_site_id: str
    #: Version of `enc_key`, prefixed to every ciphertext so rotation needs no
    #: rewrite of data already on the topic.
    key_version: int
    # `repr=False` because a traceback or a logged exception interpolates this
    # object, and key material in a log is key material in the world.
    enc_key: bytes = field(repr=False)
    mac_key: bytes = field(repr=False)


def derive_tenant_keys(
    master_secret: bytes, salt: bytes, *, career_site_id: str, key_version: int
) -> TenantKeys:
    """Derive `(enc_key, mac_key)` for one tenant.

    `salt` is the tenant id, supplied by `Settings.tenant_key_salt` so the salt
    policy lives in one place. A distinct salt per tenant is what stops one
    tenant's keys from being computable from another's.
    """
    if len(master_secret) < MIN_MASTER_SECRET_BYTES:
        raise ValueError(
            f"master secret must be at least {MIN_MASTER_SECRET_BYTES} bytes, got {len(master_secret)}"
        )

    # Extract once, expand twice. The extract is the only step that touches the
    # master secret.
    prk = HKDF.extract(hashes.SHA256(), salt, master_secret)
    return TenantKeys(
        career_site_id=career_site_id,
        key_version=key_version,
        enc_key=HKDFExpand(hashes.SHA256(), KEY_LENGTH, ENC_INFO).derive(prk),
        mac_key=HKDFExpand(hashes.SHA256(), KEY_LENGTH, MAC_INFO).derive(prk),
    )
