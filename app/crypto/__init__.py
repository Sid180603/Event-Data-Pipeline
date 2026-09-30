"""T6: PII encryption, purpose-separated key derivation, tenant registry.

Plan D5. Nothing in this package may ever return, log or raise with a
plaintext PII value; the canary tests in `test_crypto.py` are what hold that.
"""

from __future__ import annotations

from app.crypto.aesgcm import (
    NONCE_BYTES,
    build_aad,
    ciphertext_key_version,
    decrypt_field,
    encrypt_field,
    split_ciphertext,
)
from app.crypto.facade import PII_FIELDS, protect_candidate
from app.crypto.keys import ENC_INFO, KEY_LENGTH, MAC_INFO, TenantKeys, derive_tenant_keys
from app.crypto.registry import (
    TenantKeyRegistry,
    UnknownKeyVersionError,
    UnknownTenantError,
)

__all__ = [
    "ENC_INFO",
    "KEY_LENGTH",
    "MAC_INFO",
    "NONCE_BYTES",
    "PII_FIELDS",
    "TenantKeyRegistry",
    "TenantKeys",
    "UnknownKeyVersionError",
    "UnknownTenantError",
    "build_aad",
    "ciphertext_key_version",
    "decrypt_field",
    "derive_tenant_keys",
    "encrypt_field",
    "protect_candidate",
    "split_ciphertext",
]
